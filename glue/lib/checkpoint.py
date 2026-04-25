# glue/lib/checkpoint.py
"""
DynamoDB Checkpoint + 并发锁

功能:
  1. 记录每个 (layer, dt, store) 分区的处理状态和文件 MD5
  2. 通过条件写实现分布式锁，防止并发 Job 处理同一分区
  3. Restate 检测：比对文件 MD5 判断是否需要重新处理

DynamoDB 表 Schema:
  PK: partition_key  (String)  格式: "<layer>#<dt>#<store>"
  Attributes: status, lock_expires_at, last_processed_at, input_files,
              file_md5s, in_count, out_count, dlq_count, job_run_id
"""

import hashlib
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

LOCK_DURATION_HOURS = 2


def _make_partition_key(layer: str, dt: str, store: str) -> str:
    return f"{layer}#{dt}#{store}"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def compute_s3_md5(s3_client, bucket: str, key: str) -> str:
    """计算 S3 对象的 ETag（对单 part 上传即 MD5）。"""
    resp = s3_client.head_object(Bucket=bucket, Key=key)
    return resp["ETag"].strip('"')


def compute_file_md5s(
    s3_client, bucket: str, keys: List[str]
) -> Dict[str, str]:
    """批量获取文件 MD5 (ETag)。"""
    result = {}
    for key in keys:
        try:
            result[key] = compute_s3_md5(s3_client, bucket, key)
        except ClientError as e:
            logger.warning("Failed to get ETag for s3://%s/%s: %s", bucket, key, e)
            result[key] = "UNKNOWN"
    return result


class CheckpointManager:
    """
    DynamoDB checkpoint 管理器。

    用法:
        mgr = CheckpointManager(table_name="iodp-dc-checkpoint-dev")

        # 尝试获取锁
        locked = mgr.acquire_lock("bronze", "2026-04-25", "ios", job_run_id)
        if not locked:
            print("分区被锁，跳过")
            return

        # 检查是否需要 restate
        needs_reprocess = mgr.needs_reprocess("bronze", "2026-04-25", "ios", new_md5s)

        # 处理完成后释放锁
        mgr.release_lock("bronze", "2026-04-25", "ios",
                         status="succeeded", in_count=1000, out_count=999, ...)
    """

    def __init__(self, table_name: str, aws_region: str = "us-east-1"):
        self._dynamodb = boto3.resource("dynamodb", region_name=aws_region)
        self._table = self._dynamodb.Table(table_name)

    def acquire_lock(
        self, layer: str, dt: str, store: str, job_run_id: str
    ) -> bool:
        """
        尝试获取分布式锁。

        成功返回 True，失败（被其他 Job 锁住）返回 False。
        使用 DynamoDB 条件写保证原子性。
        """
        pk = _make_partition_key(layer, dt, store)
        now = _now_iso()
        expires = (
            datetime.now(timezone.utc) + timedelta(hours=LOCK_DURATION_HOURS)
        ).isoformat()

        try:
            self._table.put_item(
                Item={
                    "partition_key": pk,
                    "status": "running",
                    "lock_expires_at": expires,
                    "last_processed_at": now,
                    "job_run_id": job_run_id,
                },
                ConditionExpression=(
                    "attribute_not_exists(partition_key) "
                    "OR #s <> :running "
                    "OR lock_expires_at < :now"
                ),
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={
                    ":running": "running",
                    ":now": now,
                },
            )
            logger.info("Lock acquired for %s (job_run_id=%s)", pk, job_run_id)
            return True

        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                logger.warning(
                    "Lock NOT acquired for %s — another job is running", pk
                )
                return False
            raise

    def release_lock(
        self,
        layer: str,
        dt: str,
        store: str,
        status: str,
        in_count: int = 0,
        out_count: int = 0,
        dlq_count: int = 0,
        input_files: Optional[List[str]] = None,
        file_md5s: Optional[Dict[str, str]] = None,
        job_run_id: str = "",
    ) -> None:
        """处理完成后更新 checkpoint 并释放锁。"""
        pk = _make_partition_key(layer, dt, store)

        item = {
            "partition_key": pk,
            "status": status,
            "last_processed_at": _now_iso(),
            "in_count": in_count,
            "out_count": out_count,
            "dlq_count": dlq_count,
            "job_run_id": job_run_id,
        }
        # 清除锁的过期时间
        if status != "running":
            item["lock_expires_at"] = "1970-01-01T00:00:00Z"

        if input_files:
            item["input_files"] = input_files
        if file_md5s:
            item["file_md5s"] = file_md5s

        self._table.put_item(Item=item)
        logger.info("Checkpoint updated for %s: status=%s", pk, status)

    def needs_reprocess(
        self,
        layer: str,
        dt: str,
        store: str,
        current_md5s: Dict[str, str],
    ) -> bool:
        """
        检查是否需要重新处理（restate 检测）。

        比对 DynamoDB 中记录的文件 MD5 与当前文件的 MD5。
        如果:
          - 没有历史记录 → 需要处理（新数据）
          - MD5 不一致 → 需要处理（restate）
          - MD5 一致 → 跳过
        """
        pk = _make_partition_key(layer, dt, store)

        try:
            resp = self._table.get_item(Key={"partition_key": pk})
        except ClientError as e:
            logger.warning("Failed to read checkpoint for %s: %s", pk, e)
            return True  # 读不到就按需要处理

        item = resp.get("Item")
        if not item:
            logger.info("No checkpoint for %s — new data, needs processing", pk)
            return True

        if item.get("status") == "failed":
            logger.info("Previous run failed for %s — needs reprocessing", pk)
            return True

        old_md5s = item.get("file_md5s", {})
        if old_md5s != current_md5s:
            logger.info(
                "File MD5 changed for %s — restate detected, needs reprocessing",
                pk,
            )
            return True

        logger.info("File MD5 unchanged for %s — skipping", pk)
        return False

    def get_checkpoint(
        self, layer: str, dt: str, store: str
    ) -> Optional[dict]:
        """读取 checkpoint 记录。"""
        pk = _make_partition_key(layer, dt, store)
        try:
            resp = self._table.get_item(Key={"partition_key": pk})
            return resp.get("Item")
        except ClientError as e:
            logger.warning("Failed to read checkpoint for %s: %s", pk, e)
            return None
