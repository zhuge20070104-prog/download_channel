# glue/seed_dropzone.py
"""
Synthetic Data Seeder: Dropzone

Generates narrow-schema Parquet files conforming to NARROW_V1_SCHEMA and writes
them to the dropzone bucket under the partition layout Bronze ETL expects:

  s3://<dropzone>/download_channel/narrow/dt=<DT>/store=<STORE>/seed-<uuid>.parquet

Designed for demo / DLQ-path testing. Runs on its own IAM role (not the Glue
ETL role) — keeps producer/consumer separation in IAM.

Args (Glue default_arguments or local CLI):
  --DROPZONE_BUCKET   target dropzone bucket
  --TARGET_DT         partition date, YYYY-MM-DD
  --TARGET_STORE      ios | google-play
  --ROW_COUNT         number of (product_id, country, device) groups; each
                      group expands to 4 rows (one per channel). Default 1000.
  --SCENARIO          clean | schema_break (default: clean)
                      schema_break drops `is_estimate_final` to exercise the
                      Bronze schema-mismatch DLQ path.

Local usage:
  python glue/seed_dropzone.py --DROPZONE_BUCKET=my-dropzone \
    --TARGET_DT=2026-05-05 --TARGET_STORE=ios --ROW_COUNT=500
"""

import io
import random
import sys
import uuid
from datetime import date as _date
from decimal import Decimal

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

CHANNELS = ["paid_featured", "paid_organic", "unpaid_featured", "unpaid_organic"]
COUNTRIES = ["US", "GB", "DE", "FR", "JP", "BR", "IN", "MX", "KR", "ID"]
DEVICES_BY_STORE = {
    "ios": ["iphone", "ipad"],
    "google-play": ["android-phone", "android-tablet"],
}


def _parse_args() -> dict:
    spec = ["DROPZONE_BUCKET", "TARGET_DT", "TARGET_STORE", "ROW_COUNT", "SCENARIO"]
    try:
        from awsglue.utils import getResolvedOptions  # type: ignore
        present = [a for a in spec if f"--{a}" in sys.argv]
        return getResolvedOptions(sys.argv, present)
    except ImportError:
        import argparse
        p = argparse.ArgumentParser()
        p.add_argument("--DROPZONE_BUCKET", required=True)
        p.add_argument("--TARGET_DT", required=True)
        p.add_argument("--TARGET_STORE", required=True, choices=["ios", "google-play"])
        p.add_argument("--ROW_COUNT", default="1000")
        p.add_argument("--SCENARIO", default="clean")
        return vars(p.parse_args())


def _generate_rows(dt: _date, store: str, group_count: int, rng: random.Random) -> list:
    devices = DEVICES_BY_STORE[store]
    rows = []
    for _ in range(group_count):
        product_id = rng.randint(1_000_000, 9_999_999)
        country = rng.choice(COUNTRIES)
        device = rng.choice(devices)

        # 4-channel split that sums to a realistic daily total.
        total = rng.randint(50, 50_000)
        weights = [rng.random() for _ in CHANNELS]
        wsum = sum(weights)
        downloads = [int(total * w / wsum) for w in weights]
        downloads[0] += total - sum(downloads)  # absorb rounding drift

        is_final = rng.random() < 0.7  # ~70% finalized

        for channel, dl in zip(CHANNELS, downloads):
            share = Decimal(dl) / Decimal(total) if total else Decimal(0)
            rows.append({
                "dt": dt,
                "product_id": product_id,
                "app_store": store,
                "country": country,
                "device": device,
                "channel": channel,
                "downloads": dl,
                "share_pct": share.quantize(Decimal("0.0001")),
                "is_estimate_final": is_final,
            })
    return rows


def _build_arrow_table(rows: list, scenario: str) -> pa.Table:
    df = pd.DataFrame(rows)

    if scenario == "schema_break":
        # Drop a non-PK column to trigger Bronze's missing-cols DLQ path.
        df = df.drop(columns=["is_estimate_final"])

    # Match NARROW_V1_SCHEMA so Bronze's type check passes — name-only checks
    # would catch missing columns, but Bronze also compares Spark dtypes.
    schema_fields = [
        ("dt",                pa.date32()),
        ("product_id",        pa.int64()),
        ("app_store",         pa.string()),
        ("country",           pa.string()),
        ("device",            pa.string()),
        ("channel",           pa.string()),
        ("downloads",         pa.int64()),
        ("share_pct",         pa.decimal128(6, 4)),
        ("is_estimate_final", pa.bool_()),
    ]
    fields = [pa.field(n, t) for n, t in schema_fields if n in df.columns]
    return pa.Table.from_pandas(df, schema=pa.schema(fields), preserve_index=False)


def main():
    args = _parse_args()
    bucket = args["DROPZONE_BUCKET"]
    dt_str = args["TARGET_DT"]
    store = args["TARGET_STORE"]
    row_count = int(args.get("ROW_COUNT") or "1000")
    scenario = args.get("SCENARIO") or "clean"

    if store not in DEVICES_BY_STORE:
        raise ValueError(f"TARGET_STORE must be one of {list(DEVICES_BY_STORE)}, got {store!r}")
    if scenario not in {"clean", "schema_break"}:
        raise ValueError(f"SCENARIO must be clean|schema_break, got {scenario!r}")

    dt = _date.fromisoformat(dt_str)

    # Deterministic seed per (dt, store): reruns produce identical bytes,
    # which makes Bronze's MD5 idempotency check meaningful in tests.
    rng = random.Random(hash((dt_str, store)) & 0xFFFFFFFF)

    print(
        f"[SEED] bucket={bucket} dt={dt_str} store={store} "
        f"groups={row_count} scenario={scenario}"
    )

    rows = _generate_rows(dt, store, row_count, rng)
    table = _build_arrow_table(rows, scenario)

    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    payload = buf.getvalue()

    key = (
        f"download_channel/narrow/dt={dt_str}/store={store}/"
        f"seed-{uuid.uuid4().hex[:8]}.parquet"
    )
    boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=payload)

    print(f"[DONE] s3://{bucket}/{key} — {len(rows)} rows, {len(payload):,} bytes")


if __name__ == "__main__":
    main()
