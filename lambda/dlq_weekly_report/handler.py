# lambda/dlq_weekly_report/handler.py
"""
Lambda: DLQ 每周汇总报告

由 EventBridge 每周一 UTC 09:00 触发。
扫描 Bronze 桶 dead_letter/ 前缀，统计过去 7 天的 DLQ 文件，
通过 SNS 发送汇总邮件给 oncall。

环境变量:
  BRONZE_BUCKET: Bronze S3 桶名
  SNS_TOPIC_ARN: 告警 SNS Topic ARN
"""

import json
import os
from collections import Counter
from datetime import datetime, timedelta, timezone

import boto3


def handler(event, context):
    bucket = os.environ["BRONZE_BUCKET"]
    sns_topic = os.environ["SNS_TOPIC_ARN"]

    s3 = boto3.client("s3")
    sns = boto3.client("sns")

    # 扫描过去 7 天的 DLQ
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=7)

    prefix = "dead_letter/"
    paginator = s3.get_paginator("list_objects_v2")

    error_files = []
    error_types = Counter()
    total_dlq_files = 0

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            last_modified = obj["LastModified"]

            # 只看过去 7 天的
            if last_modified.replace(tzinfo=timezone.utc) < cutoff:
                continue

            total_dlq_files += 1

            if key.endswith(".error.json"):
                # 读取 error.json 统计错误类型
                try:
                    resp = s3.get_object(Bucket=bucket, Key=key)
                    error_doc = json.loads(resp["Body"].read())
                    error_types[error_doc.get("error_type", "UNKNOWN")] += 1
                    error_files.append({
                        "key": key,
                        "error_type": error_doc.get("error_type"),
                        "source_file": error_doc.get("source_file", ""),
                        "timestamp": error_doc.get("timestamp", ""),
                    })
                except Exception:
                    error_types["READ_ERROR"] += 1

    # 构建报告
    if total_dlq_files == 0:
        subject = "[DC-ETL] DLQ Weekly Report — All Clear"
        body = (
            f"Download Channel ETL — DLQ Weekly Report\n"
            f"Period: {cutoff.strftime('%Y-%m-%d')} to {now.strftime('%Y-%m-%d')}\n\n"
            f"No DLQ files found in the past 7 days. All clear.\n"
        )
    else:
        subject = f"[DC-ETL] DLQ Weekly Report — {total_dlq_files} files"
        lines = [
            f"Download Channel ETL — DLQ Weekly Report",
            f"Period: {cutoff.strftime('%Y-%m-%d')} to {now.strftime('%Y-%m-%d')}",
            f"",
            f"Total DLQ files: {total_dlq_files}",
            f"Error .json files: {len(error_files)}",
            f"",
            f"Error types breakdown:",
        ]
        for err_type, count in error_types.most_common():
            lines.append(f"  {err_type}: {count}")

        lines.append("")
        lines.append("Recent files (up to 20):")
        for ef in error_files[:20]:
            lines.append(
                f"  [{ef['error_type']}] {ef['source_file']} "
                f"({ef['timestamp']})"
            )

        if len(error_files) > 20:
            lines.append(f"  ... and {len(error_files) - 20} more")

        lines.append("")
        lines.append(
            "Action: Review errors and run `make dlq-replay DATE=<date>` "
            "after fixing the root cause."
        )
        body = "\n".join(lines)

    # 发送 SNS
    sns.publish(
        TopicArn=sns_topic,
        Subject=subject[:100],
        Message=body,
    )

    print(f"DLQ weekly report sent: {total_dlq_files} files")
    return {"statusCode": 200, "dlq_files": total_dlq_files}
