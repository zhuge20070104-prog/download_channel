"""
Lambda: 锁超时检测（PLAN.md §14 告警 #5）

由 EventBridge 每 30 分钟触发。
查询 DynamoDB sparse GSI `status-index`，找出 status="running" 且
lock_expires_at < 现在 的分区。这通常意味着上一轮 Glue Job 崩溃或被 Glue
Timeout 杀掉，但 cleanup 代码没跑成（status="running" 还停留在 item 上）。

成功完成的 Job 会在 release_lock 时把 status 和 lock_expires_at REMOVE 掉，
所以这两个字段在完成态 item 上不存在 → 不进入稀疏 GSI → 不参与扫描。

环境变量:
  CHECKPOINT_TABLE: DynamoDB 表名
  SNS_TOPIC_ARN:    告警 SNS Topic ARN
"""

import os
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Key

STATUS_INDEX_NAME = "status-index"


def handler(event, context):
    table_name = os.environ["CHECKPOINT_TABLE"]
    sns_topic = os.environ["SNS_TOPIC_ARN"]

    dynamodb = boto3.resource("dynamodb")
    sns = boto3.client("sns")
    table = dynamodb.Table(table_name)

    now_iso = datetime.now(timezone.utc).isoformat()

    stale = []
    # GSI 是稀疏的：只索引"含有 status 字段"的 item。完成态的 item 已经被
    # release_lock REMOVE 掉 status，不在索引里。这里直接 Query "running"
    # 分区，再用 SK 范围 lock_expires_at < now 过滤。无需 FilterExpression，
    # 也不会扫到完成态 item。
    query_kwargs = {
        "IndexName": STATUS_INDEX_NAME,
        "KeyConditionExpression": Key("status").eq("running") & Key("lock_expires_at").lt(now_iso),
        "ProjectionExpression": "partition_key, lock_expires_at, last_processed_at, job_run_id",
    }

    while True:
        resp = table.query(**query_kwargs)
        stale.extend(resp.get("Items", []))
        if "LastEvaluatedKey" not in resp:
            break
        query_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    if not stale:
        print("No stale locks found.")
        return {"statusCode": 200, "stale_count": 0}

    lines = [
        "Download Channel ETL — Stale Lock Detected",
        f"Scan time (UTC): {now_iso}",
        f"Table: {table_name}",
        f"Stale partitions: {len(stale)}",
        "",
        "Likely cause: a previous Glue Job crashed or was killed by Glue Timeout (120m)",
        "without releasing the DynamoDB lock. The lock_expires_at logic will let the next",
        "scheduled run steal the lock, but the original failure should be investigated.",
        "",
        "Affected partitions:",
    ]
    for item in stale[:30]:
        lines.append(
            f"  {item['partition_key']} "
            f"(lock_expires_at={item.get('lock_expires_at', '?')}, "
            f"job_run_id={item.get('job_run_id', '?')})"
        )
    if len(stale) > 30:
        lines.append(f"  ... and {len(stale) - 30} more")
    lines.append("")
    lines.append(
        "Action: check Glue Job run history (Console → Glue → Jobs → Run history) "
        "for the listed job_run_id. If the job is no longer running, the lock will be "
        "auto-released on the next scheduled invocation."
    )

    body = "\n".join(lines)
    subject = f"[DC-ETL] Stale lock alert — {len(stale)} partition(s)"

    sns.publish(TopicArn=sns_topic, Subject=subject[:100], Message=body)
    print(f"Stale lock alert sent: {len(stale)} partitions")
    return {"statusCode": 200, "stale_count": len(stale)}
