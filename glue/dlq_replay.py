# glue/dlq_replay.py
"""
Glue Batch Job: DLQ 重放

手动触发，将某一天失败批次的 DLQ 文件移回 dropzone，
让正常的 Bronze → Silver Workflow 重新处理。

参数语义:
  FAILED_AT_DATE — 失败发生当天的日期（UTC，YYYY-MM-DD），即
                   DLQ 路径中 failed_at=<DATE> 的值。
                   注意：这不是数据的业务 dt；一次失败批次通常会涉及
                   多个业务 dt（例如 Bronze 一次扫 lookback 多天全挂）。

前置条件:
  1. 人工检查 .error.json 确认问题已修复（如上游修了 schema）
  2. 通过 Makefile 触发: make dlq-replay DATE=2026-04-25

DLQ 路径布局（由 lib/dlq.py 写入）:
  dead_letter/failed_at=<DATE>/<original_source_key>             ← Bronze 原文件 (copy_to_dlq)
  dead_letter/failed_at=<DATE>/<original_key>.error.json         ← 错误元数据
  dead_letter/failed_at=<DATE>/dq_failure_dt=<业务dt>/*.parquet  ← Silver DQ 失败数据

数据流:
  Bronze 失败原文件 → 解出原 source_key → 复制回 DROPZONE_BUCKET 同 key
                  → 下次 Bronze ETL 自然处理

  Silver DQ failure parquet 不在本 job 的回放范围（数据已是 Silver 宽表，
  无法直接喂回 Bronze 流；需要修数后单独 rerun Silver）。本 job 跳过它们，
  归档时一起移到 dead_letter_replayed/ 留痕。
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
    "FAILED_AT_DATE",   # DLQ 中 failed_at=<DATE> 的值，即失败发生当天 (YYYY-MM-DD)
])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

BRONZE_BUCKET = args["BRONZE_BUCKET"]
DROPZONE_BUCKET = args["DROPZONE_BUCKET"]
FAILED_AT_DATE = args["FAILED_AT_DATE"]
JOB_RUN_ID = args.get("JOB_RUN_ID", str(uuid.uuid4()))

s3_client = boto3.client("s3")

# ─── 1. 扫描 DLQ 目录 ───
dlq_prefix = f"dead_letter/failed_at={FAILED_AT_DATE}/"
print(f"Scanning DLQ: s3://{BRONZE_BUCKET}/{dlq_prefix}")

paginator = s3_client.get_paginator("list_objects_v2")
# (dlq_key, original_source_key) — 待回放到 dropzone 的 Bronze 原文件
replayables = []
# 只归档不回放：error.json 元数据 + Silver DQ failure parquet
archive_only = []

for page in paginator.paginate(Bucket=BRONZE_BUCKET, Prefix=dlq_prefix):
    for obj in page.get("Contents", []):
        key = obj["Key"]
        if key.endswith("/"):
            continue

        # 去掉 dead_letter/failed_at=<DATE>/ 前缀，剩下就是写入时保留的原 source_key
        original_source_key = key[len(dlq_prefix):]

        if original_source_key.endswith(".error.json"):
            archive_only.append(key)
        elif original_source_key.startswith("dq_failure_dt="):
            # Silver DQ 失败数据：宽表 schema，无法回 dropzone（dropzone 是 v1 窄表）
            archive_only.append(key)
        else:
            replayables.append((key, original_source_key))

print(
    f"Found {len(replayables)} files to replay, "
    f"{len(archive_only)} metadata/Silver-DQ files to archive only"
)

if not replayables and not archive_only:
    print("Nothing to replay or archive. Exiting.")
    job.commit()
    sys.exit(0)

# ─── 2. 把原文件按其原 source_key 复制回 dropzone ───
# 原 source_key 形如 download_channel/narrow/dt=YYYY-MM-DD/store=<store>/<file>，
# 复制回 DROPZONE_BUCKET 同 key 后，下次 Bronze ETL 会自然扫到并处理。
replay_count = 0
for dlq_key, original_source_key in replayables:
    print(
        f"  Replay: s3://{BRONZE_BUCKET}/{dlq_key} "
        f"-> s3://{DROPZONE_BUCKET}/{original_source_key}"
    )
    s3_client.copy_object(
        Bucket=DROPZONE_BUCKET,
        Key=original_source_key,
        CopySource={"Bucket": BRONZE_BUCKET, "Key": dlq_key},
    )
    replay_count += 1

# ─── 3. 把已处理的 DLQ 文件移到 dead_letter_replayed/ 归档 ───
for key in [k for k, _ in replayables] + archive_only:
    archive_key = key.replace("dead_letter/", "dead_letter_replayed/", 1)
    s3_client.copy_object(
        Bucket=BRONZE_BUCKET,
        Key=archive_key,
        CopySource={"Bucket": BRONZE_BUCKET, "Key": key},
    )
    s3_client.delete_object(Bucket=BRONZE_BUCKET, Key=key)

print(f"Replay complete: {replay_count} files restored to dropzone (original source_key)")
print(f"DLQ archived to dead_letter_replayed/failed_at={FAILED_AT_DATE}/")
print("Next: Bronze ETL will pick up the restored files on its next run "
      "(or trigger manually via `make bronze`).")

job.commit()
