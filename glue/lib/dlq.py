# glue/lib/dlq.py
"""
Dead Letter Queue 写入工具

将解析失败 / DQ 不通过的数据写入 DLQ S3 前缀。
每个失败批次写两个对象:
  1. 原文件拷贝（或失败的 DataFrame 导出为 Parquet）
  2. .error.json 描述失败原因
"""

import json
import logging
import traceback
from datetime import datetime, timezone

import boto3

logger = logging.getLogger(__name__)


def write_dlq_error_json(
    s3_client,
    dlq_bucket: str,
    original_key: str,
    error_type: str,
    error_message: str,
    job_run_id: str,
    source_file: str,
    extra: dict = None,
) -> str:
    """
    写一个 .error.json 到 DLQ 前缀。

    返回 DLQ 中 error.json 的 S3 key。
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    # 取 original_key 的文件名部分
    file_name = original_key.rstrip("/").split("/")[-1]
    error_key = f"dead_letter/{today}/{file_name}.error.json"

    error_doc = {
        "error_type": error_type,
        "error_message": error_message,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "job_run_id": job_run_id,
        "source_file": source_file,
        "original_key": original_key,
    }
    if extra:
        error_doc["extra"] = extra

    s3_client.put_object(
        Bucket=dlq_bucket,
        Key=error_key,
        Body=json.dumps(error_doc, indent=2, default=str),
        ContentType="application/json",
    )
    logger.warning("DLQ error.json written to s3://%s/%s", dlq_bucket, error_key)
    return error_key


def copy_to_dlq(
    s3_client,
    source_bucket: str,
    source_key: str,
    dlq_bucket: str,
) -> str:
    """
    将原文件拷贝到 DLQ 前缀。

    返回 DLQ 中文件副本的 S3 key。
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    file_name = source_key.rstrip("/").split("/")[-1]
    dlq_key = f"dead_letter/{today}/{file_name}"

    s3_client.copy_object(
        Bucket=dlq_bucket,
        Key=dlq_key,
        CopySource={"Bucket": source_bucket, "Key": source_key},
    )
    logger.warning(
        "DLQ file copied: s3://%s/%s -> s3://%s/%s",
        source_bucket, source_key, dlq_bucket, dlq_key,
    )
    return dlq_key


def write_dlq_dataframe(
    df,
    dlq_bucket: str,
    partition_dt: str,
    error_type: str,
    job_run_id: str,
) -> str:
    """
    将 DQ 失败的 DataFrame 写入 DLQ 为 Parquet。

    用于 Silver DQ 卡点失败时，把整个分区的问题数据存下来。
    返回 DLQ 路径。
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    dlq_path = (
        f"s3://{dlq_bucket}/dead_letter/{today}/"
        f"dq_failure_dt={partition_dt}/"
    )

    from pyspark.sql.functions import lit, current_timestamp

    dlq_df = df.withColumn("_dlq_error_type", lit(error_type)) \
               .withColumn("_dlq_job_run_id", lit(job_run_id)) \
               .withColumn("_dlq_timestamp", current_timestamp())

    dlq_df.write.mode("overwrite").parquet(dlq_path)
    logger.warning(
        "DLQ DataFrame written to %s (%d rows, error_type=%s)",
        dlq_path, df.count(), error_type,
    )
    return dlq_path
