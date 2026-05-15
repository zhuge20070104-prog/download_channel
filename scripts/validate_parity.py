"""
Cross-warehouse parity check: Glue Silver (Athena) vs Snowflake Silver
Table: dc_wide

Three layers:
  Layer 1: COUNT(*) + row checksum (MD5-based, exact)
  Layer 2: SUM / SUMSQ / MIN / MAX / NULL_CNT for tolerance cols
  Layer 3: Sampled row-level pandas merge (triggered on Layer 1/2 failure)

Usage:
  export SF_USER=... SF_PASSWORD=... SF_ACCOUNT=... SF_WAREHOUSE=IODP_DC_WH_DEV
  python scripts/validate_parity.py 2026-05-13
"""

from __future__ import annotations

import os
import sys
import time
from typing import Any

import boto3
import pandas as pd
import snowflake.connector


# ============ Schema classification ============
SCHEMA: dict[str, list[str]] = {
    # Composite PK (dt fixed via WHERE)
    "pk_cols": ["product_id", "app_store", "country", "device"],
    # Exact-compare columns (go into MD5 row checksum)
    "exact_cols": [
        "product_id",
        "app_store",
        "country",
        "device",
        "downloads_total",
        "downloads_featured",
        "downloads_organic",
        "downloads_paid_featured",
        "downloads_paid_organic",
        "downloads_unpaid_featured",
        "downloads_unpaid_organic",
        "is_estimate_final",
        "ingest_ts",
    ],
    # Tolerance-compare columns (SUM/SUMSQ/MIN/MAX/NULL_CNT)
    "tolerance_cols": ["paid_share", "featured_share"],
    # Excluded (Snowflake-only or partition-only)
    "exclude": ["_loaded_at", "store"],
}

TOLERANCE = 1e-6      # Relative-error threshold for SUM/SUMSQ
EXACT_FLOAT_EPS = 1e-9  # Tiny absolute eps for MIN/MAX (kills Decimal-vs-float false positives)
SAMPLE_SIZE = 1000

ATHENA_DB = os.environ.get("ATHENA_DB", "iodp_dc_silver_dev")
ATHENA_TABLE = os.environ.get("ATHENA_TABLE", "dc_wide")
ATHENA_OUTPUT = os.environ.get(
    "ATHENA_OUTPUT", "s3://iodp-dc-athena-results-dev/parity/"
)
ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "primary")

SF_TABLE = os.environ.get("SF_TABLE", "IODP_DC_DEV.SILVER.DC_WIDE")


# ============ Type normalization ============
def _to_float(v: Any) -> float | None:
    if v is None:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    return float(v)


def _to_int(v: Any) -> int:
    if v is None:
        return 0
    if isinstance(v, float) and pd.isna(v):
        return 0
    return int(v)


def _normalize_agg_row(row: dict[str, Any]) -> dict[str, Any]:
    """Snowflake returns upper-case columns and Decimal types; Athena returns
    lower-case strings/floats. Unify both into lower-case keys with predictable
    Python types so downstream comparison is type-safe."""
    out: dict[str, Any] = {}
    for k, v in row.items():
        k_lower = k.lower()
        if k_lower == "row_cnt" or k_lower.endswith("_null_cnt"):
            out[k_lower] = _to_int(v)
        elif k_lower == "row_checksum":
            # row_checksum can be a very large integer; preserve precision as int
            out[k_lower] = _to_int(v)
        else:
            out[k_lower] = _to_float(v)
    return out


# ============ Query helpers ============
def athena_query(sql: str) -> pd.DataFrame:
    client = boto3.client("athena")
    qid = client.start_query_execution(
        QueryString=sql,
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
        WorkGroup=ATHENA_WORKGROUP,
    )["QueryExecutionId"]

    while True:
        state = client.get_query_execution(QueryExecutionId=qid)[
            "QueryExecution"
        ]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)

    if state != "SUCCEEDED":
        reason = client.get_query_execution(QueryExecutionId=qid)[
            "QueryExecution"
        ]["Status"].get("StateChangeReason", "")
        raise RuntimeError(f"Athena query {state}: {reason}\nSQL: {sql}")

    bucket_key = ATHENA_OUTPUT.replace("s3://", "").rstrip("/")
    return pd.read_csv(f"s3://{bucket_key}/{qid}.csv")


def sf_query(sql: str) -> pd.DataFrame:
    conn = snowflake.connector.connect(
        user=os.environ["SF_USER"],
        password=os.environ["SF_PASSWORD"],
        account=os.environ["SF_ACCOUNT"],
        warehouse=os.environ["SF_WAREHOUSE"],
        role=os.environ.get("SF_ROLE", "IODP_DC_READER_DEV"),
    )
    try:
        cur = conn.cursor()
        cur.execute(sql)
        cols = [c[0].lower() for c in cur.description]
        rows = cur.fetchall()
        return pd.DataFrame(rows, columns=cols)
    finally:
        conn.close()


# ============ Layer 1 + 2: aggregate SQL builders ============
def _athena_hash_expr() -> str:
    parts: list[str] = []
    for c in SCHEMA["exact_cols"]:
        if c == "country":
            # CHAR(2) on Snowflake right-pads with spaces — RTRIM both sides
            parts.append("COALESCE(RTRIM(country), '__NULL__')")
        elif c == "ingest_ts":
            # Athena TIMESTAMP default ms, Snowflake TIMESTAMP_NTZ default ns;
            # truncate to seconds for cross-engine equality
            parts.append(
                "COALESCE(CAST(date_trunc('second', ingest_ts) AS VARCHAR), '__NULL__')"
            )
        elif c == "is_estimate_final":
            parts.append("COALESCE(CAST(is_estimate_final AS VARCHAR), '__NULL__')")
        else:
            parts.append(f"COALESCE(CAST({c} AS VARCHAR), '__NULL__')")
    return " || '|' || ".join(parts)


def _sf_hash_expr() -> str:
    parts: list[str] = []
    for c in SCHEMA["exact_cols"]:
        if c == "country":
            parts.append("COALESCE(RTRIM(country), '__NULL__')")
        elif c == "ingest_ts":
            parts.append(
                "COALESCE(TO_VARCHAR(DATE_TRUNC('SECOND', ingest_ts)), '__NULL__')"
            )
        elif c == "is_estimate_final":
            parts.append("COALESCE(TO_VARCHAR(is_estimate_final), '__NULL__')")
        else:
            parts.append(f"COALESCE(TO_VARCHAR({c}), '__NULL__')")
    return " || '|' || ".join(parts)


def athena_aggregate_sql(dt: str) -> str:
    hash_expr = _athena_hash_expr()
    tol = ",\n        ".join(
        f"""SUM(CAST({c} AS DOUBLE)) AS {c}_sum,
        SUM(CAST({c} AS DOUBLE) * CAST({c} AS DOUBLE)) AS {c}_sumsq,
        MIN({c}) AS {c}_min,
        MAX({c}) AS {c}_max,
        SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END) AS {c}_null_cnt"""
        for c in SCHEMA["tolerance_cols"]
    )
    # row_checksum: MD5 → take first 15 hex chars (60 bits) → BIGINT → SUM.
    # Output as VARCHAR to preserve int precision through CSV round-trip.
    return f"""
    SELECT
        COUNT(*) AS row_cnt,
        CAST(
            SUM(CAST(from_base(SUBSTR(to_hex(md5(to_utf8({hash_expr}))), 1, 15), 16) AS DECIMAL(38,0)))
            AS VARCHAR
        ) AS row_checksum,
        {tol}
    FROM {ATHENA_DB}.{ATHENA_TABLE}
    WHERE dt = '{dt}'
    """


def sf_aggregate_sql(dt: str) -> str:
    hash_expr = _sf_hash_expr()
    tol = ",\n        ".join(
        f"""SUM({c}::DOUBLE) AS {c}_sum,
        SUM({c}::DOUBLE * {c}::DOUBLE) AS {c}_sumsq,
        MIN({c}) AS {c}_min,
        MAX({c}) AS {c}_max,
        SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END) AS {c}_null_cnt"""
        for c in SCHEMA["tolerance_cols"]
    )
    return f"""
    SELECT
        COUNT(*) AS row_cnt,
        TO_VARCHAR(
            SUM(TO_NUMBER(SUBSTR(MD5_HEX({hash_expr}), 1, 15), 'XXXXXXXXXXXXXXX'))
        ) AS row_checksum,
        {tol}
    FROM {SF_TABLE}
    WHERE dt = '{dt}'
    """


# ============ Compare aggregates ============
def compare_aggregates(g: dict[str, Any], s: dict[str, Any]) -> list[str]:
    errors: list[str] = []

    # Layer 1: strict equality
    if g["row_cnt"] != s["row_cnt"]:
        errors.append(f"[L1] row_cnt: glue={g['row_cnt']} sf={s['row_cnt']}")
    if g["row_checksum"] != s["row_checksum"]:
        errors.append(
            f"[L1] row_checksum mismatch (some exact-col value differs); "
            f"glue={g['row_checksum']} sf={s['row_checksum']}"
        )

    # Layer 2: tolerance cols
    for c in SCHEMA["tolerance_cols"]:
        # NULL count: strict
        if g[f"{c}_null_cnt"] != s[f"{c}_null_cnt"]:
            errors.append(
                f"[L2] {c} null_cnt: glue={g[f'{c}_null_cnt']} sf={s[f'{c}_null_cnt']}"
            )

        # MIN / MAX: should be exact, give tiny eps to absorb Decimal-vs-float marshaling
        for stat in ("min", "max"):
            gv, sv = g[f"{c}_{stat}"], s[f"{c}_{stat}"]
            if gv is None and sv is None:
                continue
            if gv is None or sv is None:
                errors.append(f"[L2] {c} {stat} one-side NULL: glue={gv} sf={sv}")
                continue
            if abs(gv - sv) > EXACT_FLOAT_EPS:
                errors.append(f"[L2] {c} {stat}: glue={gv} sf={sv}")

        # SUM / SUMSQ: relative tolerance (parallel accumulation order is non-deterministic)
        for stat in ("sum", "sumsq"):
            gv = g[f"{c}_{stat}"] or 0.0
            sv = s[f"{c}_{stat}"] or 0.0
            denom = max(abs(gv), abs(sv), 1.0)
            if abs(gv - sv) / denom > TOLERANCE:
                errors.append(
                    f"[L2] {c}_{stat} rel_diff>{TOLERANCE}: glue={gv} sf={sv}"
                )

    return errors


# ============ Layer 3: sampled row-level diff ============
def _sql_literal(v: Any) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def layer3_sample_diff(dt: str) -> pd.DataFrame:
    all_cols = SCHEMA["exact_cols"] + SCHEMA["tolerance_cols"]
    col_list = ", ".join(all_cols)
    pk_cols = SCHEMA["pk_cols"]

    g_df = athena_query(
        f"""
        SELECT {col_list}
        FROM {ATHENA_DB}.{ATHENA_TABLE} TABLESAMPLE BERNOULLI(1)
        WHERE dt = '{dt}'
        LIMIT {SAMPLE_SIZE}
        """
    )
    if g_df.empty:
        return pd.DataFrame()

    # Build a PK filter to pull the same rows from Snowflake (exact match on PK)
    pk_tuples = [
        tuple(r) for r in g_df[pk_cols].itertuples(index=False, name=None)
    ]
    pk_filter = " OR ".join(
        "("
        + " AND ".join(f"{c}={_sql_literal(v)}" for c, v in zip(pk_cols, t))
        + ")"
        for t in pk_tuples
    )
    s_df = sf_query(
        f"SELECT {col_list} FROM {SF_TABLE} WHERE dt='{dt}' AND ({pk_filter})"
    )

    merged = g_df.merge(
        s_df,
        on=pk_cols,
        how="outer",
        suffixes=("_glue", "_sf"),
        indicator=True,
    )

    diffs: list[pd.DataFrame] = []

    only_glue = merged[merged["_merge"] == "left_only"]
    if not only_glue.empty:
        diffs.append(only_glue.assign(issue="only_in_glue"))

    only_sf = merged[merged["_merge"] == "right_only"]
    if not only_sf.empty:
        diffs.append(only_sf.assign(issue="only_in_snowflake"))

    both = merged[merged["_merge"] == "both"]

    for c in SCHEMA["exact_cols"]:
        if c in pk_cols:
            continue
        bad = both[both[f"{c}_glue"] != both[f"{c}_sf"]]
        if not bad.empty:
            diffs.append(bad.assign(issue=f"value_diff:{c}"))

    for c in SCHEMA["tolerance_cols"]:
        g_v = both[f"{c}_glue"].astype(float)
        s_v = both[f"{c}_sf"].astype(float)
        denom = pd.concat(
            [g_v.abs(), s_v.abs(), pd.Series(1.0, index=g_v.index)], axis=1
        ).max(axis=1)
        bad_mask = (g_v - s_v).abs() / denom > TOLERANCE
        bad = both[bad_mask]
        if not bad.empty:
            diffs.append(bad.assign(issue=f"tolerance_diff:{c}"))

    return pd.concat(diffs, ignore_index=True) if diffs else pd.DataFrame()


# ============ Entry point ============
def validate(dt: str) -> None:
    print(f"[parity] dt={dt}")

    g_raw = athena_query(athena_aggregate_sql(dt)).iloc[0].to_dict()
    s_raw = sf_query(sf_aggregate_sql(dt)).iloc[0].to_dict()

    g = _normalize_agg_row(g_raw)
    s = _normalize_agg_row(s_raw)

    errors = compare_aggregates(g, s)
    if not errors:
        print(f"[parity] PASS  rows={g['row_cnt']}")
        return

    print("[parity] FAIL  Layer 1/2:")
    for e in errors:
        print(f"  {e}")

    print("[parity] running Layer 3 sample diff...")
    diff_df = layer3_sample_diff(dt)
    if diff_df.empty:
        print(
            "[parity] sample showed no row-level diff "
            "(issue may be in rows outside the 1000-row sample)"
        )
    else:
        print(f"[parity] {len(diff_df)} mismatched sample rows:")
        print(diff_df.head(20).to_string())

    raise AssertionError(f"Parity check failed for dt={dt}")


if __name__ == "__main__":
    dt_arg = sys.argv[1] if len(sys.argv) > 1 else "2026-05-13"
    validate(dt_arg)
