"""
Lambda: dropzone 数据缺失检测（PLAN.md §14 告警 #6）

由 EventBridge 每日 UTC 11:00 触发（ETL 在 UTC 10:00 触发，留 1 小时给上游晚到）。
扫描 dropzone S3 桶，检查当日 (UTC) 是否有上游 PUT 的文件。
对每个 (schema_version, store) 组合都要求至少有一个文件，否则告警。

PLAN.md §8 dropzone 路径布局:
  s3://<dropzone>/download_channel/narrow/dt=YYYY-MM-DD/store=<store>/*.csv.gz

dropzone 业务上只上传 narrow；wide 由 Silver 侧 pivot 生成，不在 dropzone 里出现。
所以 schema version 直接写死为 "narrow"，不再做参数。

环境变量:
  DROPZONE_BUCKET:        上游桶名
  DROPZONE_PREFIX:        默认 "download_channel/"
  EXPECTED_STORES:        逗号分隔，默认 "ios,google-play"
  CHECK_DATE_OFFSET_DAYS: 检查哪天的数据，默认 0 (今天 UTC)；可设 -1 检查昨天
  SNS_TOPIC_ARN:          告警 SNS Topic ARN
"""

import os
from datetime import datetime, timedelta, timezone

import boto3


SCHEMA_VERSION = "narrow"


def handler(event, context):
    bucket = os.environ["DROPZONE_BUCKET"]
    prefix_root = os.environ.get("DROPZONE_PREFIX", "download_channel/").rstrip("/") + "/"
    stores = [s.strip() for s in os.environ.get("EXPECTED_STORES", "ios,google-play").split(",") if s.strip()]
    offset_days = int(os.environ.get("CHECK_DATE_OFFSET_DAYS", "0"))
    sns_topic = os.environ["SNS_TOPIC_ARN"]

    target_date = (datetime.now(timezone.utc) + timedelta(days=offset_days)).date()
    dt_str = target_date.isoformat()

    s3 = boto3.client("s3")
    sns = boto3.client("sns")

    missing = []
    found = []

    for store in stores:
        partition_prefix = (
            f"{prefix_root}{SCHEMA_VERSION}/dt={dt_str}/store={store}/"
        )
        resp = s3.list_objects_v2(
            Bucket=bucket, Prefix=partition_prefix, MaxKeys=1
        )
        if resp.get("KeyCount", 0) == 0:
            missing.append(f"{SCHEMA_VERSION}/dt={dt_str}/store={store}")
        else:
            found.append(f"{SCHEMA_VERSION}/dt={dt_str}/store={store}")

    if not missing:
        print(f"All expected partitions present for {dt_str}: {found}")
        return {"statusCode": 200, "missing_count": 0, "found": found}

    lines = [
        "Download Channel ETL — Upstream Data Missing",
        f"Check date (UTC):  {dt_str}",
        f"Dropzone bucket:   {bucket}",
        f"Schema version:    {SCHEMA_VERSION}",
        f"Expected stores:   {stores}",
        "",
        f"Missing partitions ({len(missing)}):",
    ]
    for m in missing:
        lines.append(f"  s3://{bucket}/{prefix_root}{m}/")
    lines.append("")
    if found:
        lines.append(f"Partitions present ({len(found)}):")
        for f in found:
            lines.append(f"  {f}")
        lines.append("")
    lines.append(
        "Action: contact Data.ai to confirm whether the upload is delayed or skipped. "
        "If files arrive later today, the next scheduled ETL run will pick them up via "
        "the restate detection logic; no manual replay needed unless a full day is skipped."
    )

    body = "\n".join(lines)
    subject = f"[DC-ETL] Upstream data missing — {dt_str} ({len(missing)} partitions)"

    sns.publish(TopicArn=sns_topic, Subject=subject[:100], Message=body)
    print(f"Missing-data alert sent: {len(missing)} partitions for {dt_str}")
    return {
        "statusCode": 200,
        "missing_count": len(missing),
        "missing": missing,
        "found": found,
    }
