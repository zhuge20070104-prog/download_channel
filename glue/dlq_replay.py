# glue/dlq_replay.py
"""
Glue Batch Job: DLQ 重放

手动触发，将 DLQ 中的文件重新走 Bronze → Silver 流程。

前置条件:
  1. 人工检查 .error.json 确认问题已修复（如上游修了 schema）
  2. 通过 Makefile 触发: make dlq-replay DATE=2026-04-25

数据流:
  s3://{BRONZE_BUCKET}/dead_letter/{DATE}/*.csv.gz
    → 移回 dropzone 对应路径结构
    → 触发正常的 Bronze → Silver Workflow

  s3://{BRONZE_BUCKET}/dead_letter/{DATE}/dq_failure_dt=*/*.parquet
    → DQ 失败的数据，重新走 Silver DQ
"""

import sys
import uuid

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "BRONZE_BUCKET",
    "DROPZONE_BUCKET",
    "ENVIRONMENT",
    "REPLAY_DATE",       # 要重放的日期 YYYY-MM-DD
])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

BRONZE_BUCKET = args["BRONZE_BUCKET"]
DROPZONE_BUCKET = args["DROPZONE_BUCKET"]
REPLAY_DATE = args["REPLAY_DATE"]
JOB_RUN_ID = args.get("JOB_RUN_ID", str(uuid.uuid4()))

s3_client = boto3.client("s3")

# ─── 1. 扫描 DLQ 目录 ───
dlq_prefix = f"dead_letter/{REPLAY_DATE}/"
print(f"Scanning DLQ: s3://{BRONZE_BUCKET}/{dlq_prefix}")

paginator = s3_client.get_paginator("list_objects_v2")
dlq_files = []
error_jsons = []

for page in paginator.paginate(Bucket=BRONZE_BUCKET, Prefix=dlq_prefix):
    for obj in page.get("Contents", []):
        key = obj["Key"]
        if key.endswith(".error.json"):
            error_jsons.append(key)
        elif not key.endswith("/"):
            dlq_files.append(key)

print(f"Found {len(dlq_files)} DLQ files and {len(error_jsons)} error.json files")

if not dlq_files:
    print("No DLQ files to replay. Exiting.")
    job.commit()
    sys.exit(0)

# ─── 2. 把原始文件移回 dropzone ───
# DLQ 中的原始文件（非 .error.json、非 dq_failure parquet）
# 需要复制回 dropzone 对应路径
replay_count = 0
for key in dlq_files:
    file_name = key.split("/")[-1]

    # 跳过 DQ 失败的 parquet（那些在 dq_failure_dt=* 子目录下）
    if "dq_failure_dt=" in key:
        continue

    # 猜测原始路径：文件名通常包含 store 和 dt 信息
    # 保守策略：复制到一个 replay/ 临时目录，让人工确认
    replay_key = f"download_channel/replay/{REPLAY_DATE}/{file_name}"

    print(f"  Copying s3://{BRONZE_BUCKET}/{key} -> s3://{DROPZONE_BUCKET}/{replay_key}")
    s3_client.copy_object(
        Bucket=DROPZONE_BUCKET,
        Key=replay_key,
        CopySource={"Bucket": BRONZE_BUCKET, "Key": key},
    )
    replay_count += 1

# ─── 3. 把已重放的 DLQ 文件移到 replayed/ 归档 ───
for key in dlq_files + error_jsons:
    archive_key = key.replace("dead_letter/", "dead_letter_replayed/")
    s3_client.copy_object(
        Bucket=BRONZE_BUCKET,
        Key=archive_key,
        CopySource={"Bucket": BRONZE_BUCKET, "Key": key},
    )
    s3_client.delete_object(Bucket=BRONZE_BUCKET, Key=key)

print(f"Replay complete: {replay_count} files moved to dropzone replay/ prefix")
print(f"DLQ files archived to dead_letter_replayed/{REPLAY_DATE}/")
print("Next: trigger Bronze ETL to process the replayed files")

job.commit()
