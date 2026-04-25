# glue/bronze_etl.py
"""
Glue Batch Job: Dropzone → Bronze S3

每日定时（EventBridge UTC 10:00）或手动触发。
读取 dropzone 桶中的原始文件（gzip CSV 或 Parquet），做:
  1. 查 DynamoDB checkpoint，确定增量 + restate 分区
  2. 获取并发锁
  3. Schema 校验 + 类型规范化 + 去重
  4. 写 Bronze S3 Parquet（覆盖写语义）
  5. 更新 checkpoint，释放锁

支持窄表 (v1, narrow/) 和宽表 (v2, wide/) 两种输入。
"""

import sys
import time
import uuid
from datetime import datetime, timezone

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col, current_timestamp, input_file_name, lit, row_number, trim,
)
from pyspark.sql.window import Window

from lib.checkpoint import CheckpointManager, compute_file_md5s
from lib.dlq import copy_to_dlq, write_dlq_error_json
from lib.schema_v1_narrow import NARROW_V1_PK, NARROW_V1_SCHEMA, VALID_CHANNELS
from lib.schema_v2_wide import WIDE_V2_PK, WIDE_V2_SCHEMA

# ─── Glue Job 参数 ───
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "DROPZONE_BUCKET",
    "BRONZE_BUCKET",
    "CHECKPOINT_TABLE",
    "ENVIRONMENT",
    # 可选: --BACKFILL_MODE, --TARGET_DT, --TARGET_STORE
])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

ENVIRONMENT = args["ENVIRONMENT"]
DROPZONE_BUCKET = args["DROPZONE_BUCKET"]
BRONZE_BUCKET = args["BRONZE_BUCKET"]
JOB_RUN_ID = args.get("JOB_RUN_ID", str(uuid.uuid4()))
BACKFILL_MODE = args.get("BACKFILL_MODE", "false").lower() == "true"
TARGET_DT = args.get("TARGET_DT", None)        # 手动指定日期
TARGET_STORE = args.get("TARGET_STORE", None)   # 手动指定 store

s3_client = boto3.client("s3")
checkpoint = CheckpointManager(
    table_name=args["CHECKPOINT_TABLE"],
    aws_region=args.get("AWS_REGION", "us-east-1"),
)


def list_dropzone_partitions():
    """扫描 dropzone 桶，返回需要处理的 (schema_version, dt, store, [keys]) 列表。"""
    partitions = []

    for version_prefix, version_label in [("narrow/", "v1"), ("wide/", "v2")]:
        prefix = f"download_channel/{version_prefix}"
        paginator = s3_client.get_paginator("list_objects_v2")

        seen = {}  # (dt, store) -> [keys]
        for page in paginator.paginate(Bucket=DROPZONE_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                # 解析 dt=YYYY-MM-DD/store=<store>/
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

                seen.setdefault((dt, store), []).append(key)

        for (dt, store), keys in seen.items():
            partitions.append((version_label, dt, store, keys))

    return partitions


def process_partition(version: str, dt: str, store: str, keys: list):
    """处理一个分区。"""
    layer = "bronze"
    start_time = time.time()

    # ─── 1. MD5 比对，检查是否需要处理 ───
    current_md5s = compute_file_md5s(s3_client, DROPZONE_BUCKET, keys)

    if not BACKFILL_MODE and not checkpoint.needs_reprocess(layer, dt, store, current_md5s):
        print(f"[SKIP] {version}/{dt}/{store} — MD5 unchanged")
        return

    # ─── 2. 获取并发锁 ───
    if not checkpoint.acquire_lock(layer, dt, store, JOB_RUN_ID):
        print(f"[LOCKED] {version}/{dt}/{store} — skipping")
        return

    print(f"[PROCESS] {version}/{dt}/{store} — {len(keys)} files")

    try:
        # ─── 3. 读取数据 ───
        input_path = f"s3://{DROPZONE_BUCKET}/download_channel/{'narrow' if version == 'v1' else 'wide'}/"
        input_path += f"dt={dt}/store={store}/"

        # 尝试读 CSV（gzip 自动检测），失败则读 Parquet
        try:
            raw_df = spark.read.option("header", "true").csv(input_path)
        except Exception:
            raw_df = spark.read.parquet(input_path)

        in_count = raw_df.count()
        if in_count == 0:
            print(f"[EMPTY] {version}/{dt}/{store} — 0 rows, skipping")
            checkpoint.release_lock(
                layer, dt, store, status="succeeded",
                in_count=0, out_count=0, input_files=keys,
                file_md5s=current_md5s, job_run_id=JOB_RUN_ID,
            )
            return

        # ─── 4. Schema 校验 + 类型转换 ───
        if version == "v1":
            expected_cols = {f.name for f in NARROW_V1_SCHEMA.fields if f.name != "ingest_ts"}
            pk_cols = NARROW_V1_PK
        else:
            expected_cols = {f.name for f in WIDE_V2_SCHEMA.fields if f.name != "ingest_ts"}
            pk_cols = WIDE_V2_PK

        actual_cols = set(raw_df.columns)
        missing_cols = expected_cols - actual_cols
        if missing_cols:
            error_msg = f"Missing columns: {missing_cols}"
            print(f"[DLQ] {version}/{dt}/{store} — {error_msg}")
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
            return

        # 类型转换
        typed_df = raw_df
        for field in (NARROW_V1_SCHEMA if version == "v1" else WIDE_V2_SCHEMA).fields:
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
        window = Window.partitionBy(*pk_cols).orderBy(col("ingest_ts").desc())
        deduped_df = typed_df \
            .withColumn("_rn", row_number().over(window)) \
            .filter(col("_rn") == 1) \
            .drop("_rn")

        out_count = deduped_df.count()
        dedup_removed = in_count - out_count

        # ─── 6. 覆盖写 Bronze ───
        bronze_path = (
            f"s3://{BRONZE_BUCKET}/download_channel/{version}/"
            f"dt={dt}/store={store}/"
        )
        # 覆盖写：Spark overwrite 模式会先删旧文件再写新文件
        deduped_df.write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(bronze_path)

        duration = int(time.time() - start_time)
        print(
            f"[DONE] {version}/{dt}/{store} — "
            f"in={in_count:,} out={out_count:,} dedup_removed={dedup_removed:,} "
            f"duration={duration}s"
        )

        # ─── 7. 更新 checkpoint ───
        checkpoint.release_lock(
            layer, dt, store, status="succeeded",
            in_count=in_count, out_count=out_count,
            input_files=keys, file_md5s=current_md5s, job_run_id=JOB_RUN_ID,
        )

    except Exception as e:
        print(f"[ERROR] {version}/{dt}/{store} — {e}")
        # 写 DLQ error.json
        for key in keys:
            write_dlq_error_json(
                s3_client, BRONZE_BUCKET, key,
                error_type="PROCESSING_ERROR",
                error_message=str(e),
                job_run_id=JOB_RUN_ID,
                source_file=f"s3://{DROPZONE_BUCKET}/{key}",
            )
        checkpoint.release_lock(
            layer, dt, store, status="failed",
            in_count=0, out_count=0, dlq_count=1,
            input_files=keys, file_md5s=current_md5s, job_run_id=JOB_RUN_ID,
        )


# ─── Main ───
print(f"Bronze ETL starting: env={ENVIRONMENT}, backfill={BACKFILL_MODE}")
partitions = list_dropzone_partitions()
print(f"Found {len(partitions)} partitions to process")

for version, dt, store, keys in partitions:
    process_partition(version, dt, store, keys)

print("Bronze ETL complete")
job.commit()
