# glue/silver_etl.py
"""
Glue Batch Job: Bronze S3 → Silver S3

由 Glue Workflow 在 Bronze Job 成功后自动触发，也可手动运行。
职责:
  1. 读 Bronze Parquet（v1 窄表 + v2 宽表）
  2. 窄表 → 宽表 pivot（统一 schema）
  3. DQ 卡点（§12）：不通过 → DLQ + 告警，不写 Silver
  4. 写 Silver S3（统一宽表 Parquet）
  5. 更新 checkpoint
"""

import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col, current_timestamp, lit, sum as spark_sum,
)

from lib.checkpoint import CheckpointManager
from lib.dlq import write_dlq_dataframe, write_dlq_error_json
from lib.dq_checks import DownloadChannelDQ
from lib.schema_v2_wide import SILVER_OUTPUT_COLUMNS, WIDE_V2_PK

# ─── Glue Job 参数 ───
# getResolvedOptions 只解析声明的参数，可选参数必须先用 sys.argv 探测再加进列表。
REQUIRED_ARGS = [
    "JOB_NAME",
    "BRONZE_BUCKET",
    "SILVER_BUCKET",
    "CHECKPOINT_TABLE",
    "SNS_TOPIC_ARN",
    "ENVIRONMENT",
]
OPTIONAL_ARGS = ["BACKFILL_MODE", "TARGET_DT", "TARGET_STORE", "LOOKBACK_DAYS"]
present_optional = [a for a in OPTIONAL_ARGS if f"--{a}" in sys.argv]
args = getResolvedOptions(sys.argv, REQUIRED_ARGS + present_optional)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

ENVIRONMENT = args["ENVIRONMENT"]
BRONZE_BUCKET = args["BRONZE_BUCKET"]
SILVER_BUCKET = args["SILVER_BUCKET"]
SNS_TOPIC_ARN = args["SNS_TOPIC_ARN"]
JOB_RUN_ID = args.get("JOB_RUN_ID", str(uuid.uuid4()))
BACKFILL_MODE = args.get("BACKFILL_MODE", "false").lower() == "true"
TARGET_DT = args.get("TARGET_DT", None)
TARGET_STORE = args.get("TARGET_STORE", None)
LOOKBACK_DAYS = int(args.get("LOOKBACK_DAYS", "14"))

LOOKBACK_CUTOFF_DT = (
    datetime.now(timezone.utc).date() - timedelta(days=LOOKBACK_DAYS)
).isoformat()

s3_client = boto3.client("s3")
sns_client = boto3.client("sns")
checkpoint = CheckpointManager(
    table_name=args["CHECKPOINT_TABLE"],
    aws_region=args.get("AWS_REGION", "us-east-1"),
)


def send_alert(subject: str, message: str):
    """发送 SNS 告警。"""
    try:
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject[:100],
            Message=message,
        )
    except Exception as e:
        print(f"[WARN] Failed to send SNS alert: {e}")


def list_bronze_partitions():
    """扫描 Bronze 桶，返回需要处理的 (version, dt, store) 列表。"""
    partitions = []

    for version in ["v1", "v2"]:
        prefix = f"download_channel/{version}/"
        paginator = s3_client.get_paginator("list_objects_v2")

        seen = set()
        for page in paginator.paginate(Bucket=BRONZE_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                parts = key.replace(prefix, "").split("/")
                dt_part = next((p for p in parts if p.startswith("dt=")), None)
                store_part = next((p for p in parts if p.startswith("store=")), None)
                if not dt_part or not store_part:
                    continue
                dt = dt_part.split("=")[1]
                store = store_part.split("=")[1]

                if TARGET_DT and dt != TARGET_DT:
                    continue
                if TARGET_STORE and store != TARGET_STORE:
                    continue
                if (
                    not BACKFILL_MODE
                    and not TARGET_DT
                    and dt < LOOKBACK_CUTOFF_DT
                ):
                    continue

                seen.add((version, dt, store))

        partitions.extend(sorted(seen))

    return partitions


def pivot_narrow_to_wide(narrow_df):
    """将窄表 (v1) pivot 成宽表 (v2) schema。"""
    from pyspark.sql.functions import when as spark_when

    pivoted = narrow_df.groupBy("dt", "product_id", "app_store", "country", "device") \
        .agg(
            spark_sum("downloads").alias("downloads_total"),

            spark_sum(spark_when(
                col("channel").isin("paid_featured", "unpaid_featured"), col("downloads")
            ).otherwise(0)).alias("downloads_featured"),

            spark_sum(spark_when(
                col("channel").isin("paid_organic", "unpaid_organic"), col("downloads")
            ).otherwise(0)).alias("downloads_organic"),

            spark_sum(spark_when(
                col("channel") == "paid_featured", col("downloads")
            ).otherwise(0)).alias("downloads_paid_featured"),

            spark_sum(spark_when(
                col("channel") == "paid_organic", col("downloads")
            ).otherwise(0)).alias("downloads_paid_organic"),

            spark_sum(spark_when(
                col("channel") == "unpaid_featured", col("downloads")
            ).otherwise(0)).alias("downloads_unpaid_featured"),

            spark_sum(spark_when(
                col("channel") == "unpaid_organic", col("downloads")
            ).otherwise(0)).alias("downloads_unpaid_organic"),
        )

    # 计算 share 列
    pivoted = pivoted.withColumn(
        "paid_share",
        ((col("downloads_paid_featured") + col("downloads_paid_organic"))
         / col("downloads_total")).cast("decimal(6,4)")
    ).withColumn(
        "featured_share",
        (col("downloads_featured") / col("downloads_total")).cast("decimal(6,4)")
    )

    # is_estimate_final: 取窄表中该 group 的 ANY TRUE（如果有一个 channel finalized，
    # 认为整个 (dt, app, country, device) 已 finalized）
    # 简化处理：窄表 pivot 后暂设为 NULL，由下游 restate 机制覆盖
    pivoted = pivoted.withColumn("is_estimate_final", lit(None).cast("boolean"))
    pivoted = pivoted.withColumn("ingest_ts", current_timestamp())

    return pivoted


def process_partition(version: str, dt: str, store: str):
    """处理一个 Bronze 分区 → Silver。"""
    layer = "silver"
    start_time = time.time()

    # ─── 1. 检查 Bronze checkpoint 确认这个分区已处理成功 ───
    bronze_ckpt = checkpoint.get_checkpoint("bronze", dt, store)
    if not bronze_ckpt or bronze_ckpt.get("status") != "succeeded":
        print(f"[SKIP] silver/{version}/{dt}/{store} — Bronze not succeeded")
        return

    # ─── 2. 获取 Silver 锁 ───
    if not checkpoint.acquire_lock(layer, dt, store, JOB_RUN_ID):
        print(f"[LOCKED] silver/{dt}/{store} — skipping")
        return

    print(f"[PROCESS] silver/{version}/{dt}/{store}")

    try:
        # ─── 3. 读 Bronze ───
        bronze_path = (
            f"s3://{BRONZE_BUCKET}/download_channel/{version}/"
            f"dt={dt}/store={store}/"
        )
        bronze_df = spark.read.parquet(bronze_path)
        in_count = bronze_df.count()

        if in_count == 0:
            print(f"[EMPTY] silver/{version}/{dt}/{store} — 0 rows")
            checkpoint.release_lock(
                layer, dt, store, status="succeeded",
                in_count=0, out_count=0, job_run_id=JOB_RUN_ID,
            )
            return

        # ─── 4. Pivot (v1) 或 透传 (v2) ───
        if version == "v1":
            wide_df = pivot_narrow_to_wide(bronze_df)
        else:
            wide_df = bronze_df

        # ─── 5. DQ 卡点 ───
        expected_count = bronze_ckpt.get("out_count")
        dq = DownloadChannelDQ(
            partition_dt=dt,
            expected_count=int(expected_count) if expected_count else None,
        )
        dq_results = dq.run_all(wide_df)

        if dq.has_blocking_failure(dq_results):
            failures = dq.get_failures(dq_results)
            failure_detail = "; ".join(f"{r.check_name}: {r.detail}" for r in failures)

            # 写 DLQ
            write_dlq_dataframe(
                wide_df, BRONZE_BUCKET, dt,
                error_type="DQ_BLOCK",
                job_run_id=JOB_RUN_ID,
            )

            # 发告警
            send_alert(
                subject=f"[DC-ETL] DQ BLOCK: {dt}/{store}",
                message=(
                    f"Silver DQ check failed (blocking) for dt={dt}, store={store}.\n"
                    f"Failures:\n{failure_detail}\n\n"
                    f"Data written to DLQ. Manual review required."
                ),
            )

            checkpoint.release_lock(
                layer, dt, store, status="failed",
                in_count=in_count, out_count=0, dlq_count=in_count,
                job_run_id=JOB_RUN_ID,
            )
            print(f"[DQ-BLOCK] silver/{dt}/{store} — {failure_detail}")
            return

        # 非阻断告警也发通知
        warn_results = [r for r in dq_results if not r.passed]
        if warn_results:
            warn_detail = "; ".join(f"{r.check_name}: {r.detail}" for r in warn_results)
            send_alert(
                subject=f"[DC-ETL] DQ WARN: {dt}/{store}",
                message=(
                    f"Silver DQ warnings for dt={dt}, store={store}.\n"
                    f"Warnings:\n{warn_detail}\n\n"
                    f"Data was written to Silver despite warnings."
                ),
            )

        # ─── 6. 写 Silver ───
        silver_path = (
            f"s3://{SILVER_BUCKET}/download_channel/"
            f"dt={dt}/store={store}/"
        )

        # 确保列顺序一致
        output_cols = [c for c in SILVER_OUTPUT_COLUMNS if c in wide_df.columns]
        silver_df = wide_df.select(*output_cols)

        silver_df.write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(silver_path)

        out_count = silver_df.count()
        duration = int(time.time() - start_time)

        print(
            f"[DONE] silver/{dt}/{store} — "
            f"in={in_count:,} out={out_count:,} duration={duration}s"
        )

        # ─── 7. 更新 checkpoint ───
        checkpoint.release_lock(
            layer, dt, store, status="succeeded",
            in_count=in_count, out_count=out_count,
            job_run_id=JOB_RUN_ID,
        )

    except Exception as e:
        print(f"[ERROR] silver/{dt}/{store} — {e}")
        import traceback
        traceback.print_exc()

        write_dlq_error_json(
            s3_client, BRONZE_BUCKET,
            original_key=f"download_channel/{version}/dt={dt}/store={store}/",
            error_type="SILVER_PROCESSING_ERROR",
            error_message=str(e),
            job_run_id=JOB_RUN_ID,
            source_file=f"s3://{BRONZE_BUCKET}/download_channel/{version}/dt={dt}/store={store}/",
        )
        send_alert(
            subject=f"[DC-ETL] Silver Job ERROR: {dt}/{store}",
            message=f"Silver ETL failed for dt={dt}, store={store}.\nError: {e}",
        )
        checkpoint.release_lock(
            layer, dt, store, status="failed",
            in_count=0, out_count=0, dlq_count=1,
            job_run_id=JOB_RUN_ID,
        )


# ─── Main ───
print(f"Silver ETL starting: env={ENVIRONMENT}")
partitions = list_bronze_partitions()
print(f"Found {len(partitions)} Bronze partitions to process")

for version, dt, store in partitions:
    process_partition(version, dt, store)

print("Silver ETL complete")
job.commit()
