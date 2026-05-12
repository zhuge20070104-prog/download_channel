# glue/bronze_etl.py
"""
Glue Batch Job: Dropzone → Bronze S3

每日定时（EventBridge UTC 10:00）或手动触发。
读取 dropzone 桶中的原始 Parquet 文件（窄表），做:
  1. 查 DynamoDB checkpoint，确定增量 + restate 分区
  2. 获取并发锁
  3. Schema 校验 + 类型规范化 + 去重
  4. 写 Bronze S3 Parquet（覆盖写语义）
  5. 更新 checkpoint，释放锁
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
from pyspark.sql.functions import col, current_timestamp, row_number, trim
from pyspark.sql.window import Window

from lib.checkpoint import CheckpointManager, compute_file_md5s
from lib.dlq import copy_to_dlq, write_dlq_error_json
from lib.schema_v1_narrow import NARROW_V1_PK, NARROW_V1_SCHEMA

# ─── Glue Job 参数 ───
# getResolvedOptions 只解析声明的参数，可选参数必须先用 sys.argv 探测再加进列表。
REQUIRED_ARGS = [
    "JOB_NAME",
    "DROPZONE_BUCKET",
    "BRONZE_BUCKET",
    "CHECKPOINT_TABLE",
    "SNS_TOPIC_ARN",
    "ENVIRONMENT",
    "AWS_REGION",
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
DROPZONE_BUCKET = args["DROPZONE_BUCKET"]
BRONZE_BUCKET = args["BRONZE_BUCKET"]
SNS_TOPIC_ARN = args["SNS_TOPIC_ARN"]
JOB_RUN_ID = args.get("JOB_RUN_ID", str(uuid.uuid4()))
BACKFILL_MODE = args.get("BACKFILL_MODE", "false").lower() == "true"
TARGET_DT = args.get("TARGET_DT", None)        # 手动指定日期
TARGET_STORE = args.get("TARGET_STORE", None)   # 手动指定 store
LOOKBACK_DAYS = int(args.get("LOOKBACK_DAYS", "14"))  # 日常 ETL 只扫近 N 天，避开历史 partition 全扫

# 日常路径用 cutoff 限制扫描范围；BACKFILL_MODE 或 TARGET_DT 显式指定时绕过
LOOKBACK_CUTOFF_DT = (
    datetime.now(timezone.utc).date() - timedelta(days=LOOKBACK_DAYS)
).isoformat()

s3_client = boto3.client("s3")
sns_client = boto3.client("sns")
glue_client = boto3.client("glue")
checkpoint = CheckpointManager(
    table_name=args["CHECKPOINT_TABLE"],
    aws_region=args["AWS_REGION"],
)

DROPZONE_PREFIX = "download_channel/narrow/"


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


def register_bronze_partition(dt: str, store: str):
    # Best-effort：Athena 是侧路 ad-hoc 通道，注册失败不阻塞 ETL。
    # AlreadyExistsException 是预期情况（restate 重写、MSCK 已登记过），静默吞。
    db_name = f"iodp_dc_bronze_{ENVIRONMENT}"
    table_name = "dc_narrow"
    location = f"s3://{BRONZE_BUCKET}/download_channel/narrow/dt={dt}/store={store}/"
    try:
        glue_client.create_partition(
            DatabaseName=db_name,
            TableName=table_name,
            PartitionInput={
                "Values": [dt, store],
                "StorageDescriptor": {
                    "Location": location,
                    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                    },
                },
            },
        )
        print(f"[GLUE] Registered Athena partition {dt}/{store}")
    except glue_client.exceptions.AlreadyExistsException:
        pass
    except Exception as e:
        print(f"[WARN] Failed to register Athena partition {dt}/{store}: {e}")


def list_dropzone_partitions():
    """扫描 dropzone 桶，返回需要处理的 (dt, store, [keys]) 列表。"""
    paginator = s3_client.get_paginator("list_objects_v2")
    seen = {}  # (dt, store) -> [keys]

    for page in paginator.paginate(Bucket=DROPZONE_BUCKET, Prefix=DROPZONE_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            # 解析 dt=YYYY-MM-DD/store=<store>/
            parts = key.replace(DROPZONE_PREFIX, "").split("/")
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
            # 日常路径：只扫近 LOOKBACK_DAYS 天的分区。
            # backfill 或显式指定日期时绕过此限制。
            if (
                not BACKFILL_MODE
                and not TARGET_DT
                and dt < LOOKBACK_CUTOFF_DT
            ):
                continue

            seen.setdefault((dt, store), []).append(key)

    return [(dt, store, keys) for (dt, store), keys in seen.items()]


def process_partition(dt: str, store: str, keys: list):
    """处理一个分区。"""
    layer = "bronze"
    start_time = time.time()

    # ─── 1. MD5 比对，检查是否需要处理 ───
    current_md5s = compute_file_md5s(s3_client, DROPZONE_BUCKET, keys)

    if not BACKFILL_MODE and not checkpoint.needs_reprocess(layer, dt, store, current_md5s):
        print(f"[SKIP] {dt}/{store} — MD5 unchanged")
        return

    # ─── 2. 获取并发锁 ───
    if not checkpoint.acquire_lock(layer, dt, store, JOB_RUN_ID):
        print(f"[LOCKED] {dt}/{store} — skipping")
        send_alert(
            subject=f"[DC-ETL] Bronze LOCK SKIP: {dt}/{store}",
            message=(
                f"Bronze partition {dt}/{store} is locked by another job. "
                f"Skipped. If this persists, check for stale locks in DynamoDB."
            ),
        )
        return

    print(f"[PROCESS] {dt}/{store} — {len(keys)} files")

    try:
        # ─── 3. 读取数据 ───
        input_path = f"s3://{DROPZONE_BUCKET}/{DROPZONE_PREFIX}dt={dt}/store={store}/"
        raw_df = spark.read.parquet(input_path)

        in_count = raw_df.count()
        if in_count == 0:
            print(f"[EMPTY] {dt}/{store} — 0 rows, skipping")
            checkpoint.release_lock(
                layer, dt, store, status="succeeded",
                in_count=0, out_count=0, input_files=keys,
                file_md5s=current_md5s, job_run_id=JOB_RUN_ID,
            )
            return

        # ─── 4. Schema 校验 + 类型转换 ───
        # 同时校验列名和列类型：上游若把 boolean 列偷偷改成 string，name-only 检查
        # 会过，但 .cast() 会把 "" 静默转成 NULL，问题被推迟到下游 Athena 才爆。
        expected_fields = {
            f.name: f.dataType for f in NARROW_V1_SCHEMA.fields if f.name != "ingest_ts"
        }
        actual_fields = {f.name: f.dataType for f in raw_df.schema.fields}
        missing_cols = sorted(set(expected_fields) - set(actual_fields))
        type_mismatches = sorted(
            f"{name}: got {actual_fields[name]}, want {expected_fields[name]}"
            for name in expected_fields
            if name in actual_fields and actual_fields[name] != expected_fields[name]
        )
        if missing_cols or type_mismatches:
            error_parts = []
            if missing_cols:
                error_parts.append(f"missing={missing_cols}")
            if type_mismatches:
                error_parts.append(f"type_mismatch=[{'; '.join(type_mismatches)}]")
            error_msg = "; ".join(error_parts)
            print(f"[DLQ] {dt}/{store} — {error_msg}")
            for key in keys:
                copy_to_dlq(s3_client, DROPZONE_BUCKET, key, BRONZE_BUCKET)
                write_dlq_error_json(
                    s3_client, BRONZE_BUCKET, key,
                    error_type="SCHEMA_MISMATCH",
                    error_message=error_msg,
                    job_run_id=JOB_RUN_ID,
                    source_file=f"s3://{DROPZONE_BUCKET}/{key}",
                )
            checkpoint.release_lock(
                layer, dt, store, status="failed",
                in_count=in_count, out_count=0, dlq_count=in_count,
                input_files=keys, file_md5s=current_md5s, job_run_id=JOB_RUN_ID,
            )
            send_alert(
                subject=f"[DC-ETL] Bronze DLQ: {dt}/{store}",
                message=(
                    f"Schema mismatch in {dt}/{store}.\n"
                    f"{error_msg}\n"
                    f"{in_count} rows sent to DLQ."
                ),
            )
            return

        # 类型转换
        typed_df = raw_df
        for field in NARROW_V1_SCHEMA.fields:
            if field.name == "ingest_ts":
                continue
            if field.name in typed_df.columns:
                typed_df = typed_df.withColumn(field.name, col(field.name).cast(field.dataType))

        # 字符串列 trim
        for field in typed_df.schema.fields:
            if str(field.dataType) == "StringType":
                typed_df = typed_df.withColumn(field.name, trim(col(field.name)))

        # 添加 ingest_ts
        typed_df = typed_df.withColumn("ingest_ts", current_timestamp())

        # ─── 5. 去重 ───
        window = Window.partitionBy(*NARROW_V1_PK).orderBy(col("ingest_ts").desc())
        deduped_df = typed_df \
            .withColumn("_rn", row_number().over(window)) \
            .filter(col("_rn") == 1) \
            .drop("_rn")

        out_count = deduped_df.count()
        dedup_removed = in_count - out_count

        # ─── 6. 覆盖写 Bronze ───
        bronze_path = (
            f"s3://{BRONZE_BUCKET}/download_channel/narrow/"
            f"dt={dt}/store={store}/"
        )
        # 覆盖写：Spark overwrite 模式会先删旧文件再写新文件
        deduped_df.write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(bronze_path)

        duration = int(time.time() - start_time)
        print(
            f"[DONE] {dt}/{store} — "
            f"in={in_count:,} out={out_count:,} dedup_removed={dedup_removed:,} "
            f"duration={duration}s"
        )

        # ─── 7. 登记 Athena 分区（best-effort）───
        register_bronze_partition(dt, store)

        # ─── 8. 更新 checkpoint ───
        checkpoint.release_lock(
            layer, dt, store, status="succeeded",
            in_count=in_count, out_count=out_count,
            input_files=keys, file_md5s=current_md5s, job_run_id=JOB_RUN_ID,
        )

    except Exception as e:
        print(f"[ERROR] {dt}/{store} — {e}")
        # 拷原文件到 DLQ + 写 error.json，保留数据便于排查/重消费
        for key in keys:
            copy_to_dlq(s3_client, DROPZONE_BUCKET, key, BRONZE_BUCKET)
            write_dlq_error_json(
                s3_client, BRONZE_BUCKET, key,
                error_type="PROCESSING_ERROR",
                error_message=str(e),
                job_run_id=JOB_RUN_ID,
                source_file=f"s3://{DROPZONE_BUCKET}/{key}",
            )
        send_alert(
            subject=f"[DC-ETL] Bronze Job ERROR: {dt}/{store}",
            message=f"Bronze ETL failed for {dt}/{store}.\nError: {e}",
        )
        checkpoint.release_lock(
            layer, dt, store, status="failed",
            in_count=0, out_count=0, dlq_count=len(keys),
            input_files=keys, file_md5s=current_md5s, job_run_id=JOB_RUN_ID,
        )


# ─── Main ───
print(f"Bronze ETL starting: env={ENVIRONMENT}, backfill={BACKFILL_MODE}")
partitions = list_dropzone_partitions()
print(f"Found {len(partitions)} partitions to process")

for dt, store, keys in partitions:
    process_partition(dt, store, keys)

print("Bronze ETL complete")
job.commit()
