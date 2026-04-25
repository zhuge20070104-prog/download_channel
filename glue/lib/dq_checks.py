# glue/lib/dq_checks.py
"""
Download Channel 数据质量卡点

在 Silver Glue Job 写入 Silver S3 之前执行。
参考 iodp data_quality.py 的 DQRule/DataQualityChecker 模式，
但针对 download_channel 的具体检查项做了定制。

五项检查:
  1. 行数对比 (Bronze count vs actual count)
  2. 关键列空值率
  3. 日期范围校验
  4. 数值范围 (非负)
  5. 等式校验 (total = featured + organic)
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, sum as spark_sum, when

logger = logging.getLogger(__name__)


@dataclass
class DQResult:
    """单项 DQ 检查结果。"""
    check_name: str
    passed: bool
    blocking: bool          # True = 不通过则阻断; False = 仅告警
    detail: str
    violation_count: int = 0
    total_count: int = 0


class DownloadChannelDQ:
    """
    Download Channel 专用 DQ 检查器。

    用法:
        dq = DownloadChannelDQ(
            partition_dt="2026-04-25",
            expected_count=850000000,  # 从 DynamoDB checkpoint 读来的 Bronze out_count
        )
        results = dq.run_all(df)
        if dq.has_blocking_failure(results):
            # 不写 Silver，走 DLQ
        else:
            # 写 Silver
    """

    # 阈值配置
    ROW_COUNT_DIFF_THRESHOLD = 0.01     # 1%
    NULL_RATE_THRESHOLD = 0.001         # 0.1%
    DATE_RANGE_TOLERANCE_DAYS = 7       # restate 窗口
    EQUATION_VIOLATION_THRESHOLD = 0.001  # 0.1%

    CRITICAL_COLUMNS = ["product_id", "dt", "country", "app_store"]
    DOWNLOAD_COLUMNS = [
        "downloads_total", "downloads_featured", "downloads_organic",
        "downloads_paid_featured", "downloads_paid_organic",
        "downloads_unpaid_featured", "downloads_unpaid_organic",
    ]

    def __init__(self, partition_dt: str, expected_count: Optional[int] = None):
        self.partition_dt = partition_dt
        self.expected_count = expected_count

    def run_all(self, df: DataFrame) -> List[DQResult]:
        """执行所有 DQ 检查，返回结果列表。"""
        results = []
        total = df.count()

        results.append(self._check_row_count(total))
        results.extend(self._check_null_rates(df, total))
        results.append(self._check_date_range(df, total))
        results.append(self._check_non_negative(df, total))
        results.append(self._check_equation(df, total))

        for r in results:
            level = "BLOCK" if r.blocking and not r.passed else (
                "WARN" if not r.passed else "PASS"
            )
            logger.info(
                "DQ [%s] %s: %s (violations=%d/%d)",
                level, r.check_name, r.detail, r.violation_count, r.total_count,
            )

        return results

    @staticmethod
    def has_blocking_failure(results: List[DQResult]) -> bool:
        """是否有任何阻断性检查失败。"""
        return any(r.blocking and not r.passed for r in results)

    @staticmethod
    def get_failures(results: List[DQResult]) -> List[DQResult]:
        """获取所有失败的检查（含阻断和告警）。"""
        return [r for r in results if not r.passed]

    # ─── 检查 #1: 行数对比 ───

    def _check_row_count(self, actual_count: int) -> DQResult:
        if self.expected_count is None:
            return DQResult(
                check_name="row_count",
                passed=True,
                blocking=True,
                detail="Skipped (no expected_count provided)",
                total_count=actual_count,
            )

        if self.expected_count == 0:
            return DQResult(
                check_name="row_count",
                passed=actual_count == 0,
                blocking=True,
                detail=f"Expected 0, got {actual_count}",
                violation_count=actual_count,
                total_count=actual_count,
            )

        diff_rate = abs(actual_count - self.expected_count) / self.expected_count
        passed = diff_rate <= self.ROW_COUNT_DIFF_THRESHOLD

        return DQResult(
            check_name="row_count",
            passed=passed,
            blocking=True,
            detail=(
                f"Expected ~{self.expected_count:,}, got {actual_count:,} "
                f"(diff={diff_rate:.4%}, threshold={self.ROW_COUNT_DIFF_THRESHOLD:.1%})"
            ),
            violation_count=abs(actual_count - self.expected_count),
            total_count=actual_count,
        )

    # ─── 检查 #2: 关键列空值率 ───

    def _check_null_rates(
        self, df: DataFrame, total: int
    ) -> List[DQResult]:
        results = []
        for col_name in self.CRITICAL_COLUMNS:
            null_count = df.filter(col(col_name).isNull()).count()
            null_rate = null_count / total if total > 0 else 0.0
            passed = null_rate <= self.NULL_RATE_THRESHOLD

            results.append(DQResult(
                check_name=f"null_rate_{col_name}",
                passed=passed,
                blocking=True,
                detail=(
                    f"{col_name}: {null_count:,} nulls / {total:,} rows "
                    f"= {null_rate:.4%} (threshold={self.NULL_RATE_THRESHOLD:.2%})"
                ),
                violation_count=null_count,
                total_count=total,
            ))
        return results

    # ─── 检查 #3: 日期范围校验 ───

    def _check_date_range(self, df: DataFrame, total: int) -> DQResult:
        from pyspark.sql.functions import datediff, to_date

        partition_date = datetime.strptime(self.partition_dt, "%Y-%m-%d").date()
        min_date = partition_date - timedelta(days=self.DATE_RANGE_TOLERANCE_DAYS)
        max_date = partition_date + timedelta(days=self.DATE_RANGE_TOLERANCE_DAYS)

        out_of_range = df.filter(
            (col("dt") < lit(str(min_date))) | (col("dt") > lit(str(max_date)))
        ).count()

        passed = out_of_range == 0

        return DQResult(
            check_name="date_range",
            passed=passed,
            blocking=True,
            detail=(
                f"Partition dt={self.partition_dt}, tolerance=+-{self.DATE_RANGE_TOLERANCE_DAYS}d. "
                f"Out-of-range rows: {out_of_range:,}"
            ),
            violation_count=out_of_range,
            total_count=total,
        )

    # ─── 检查 #4: 数值非负 ───

    def _check_non_negative(self, df: DataFrame, total: int) -> DQResult:
        # 构建条件: 任意下载列 < 0
        condition = None
        for col_name in self.DOWNLOAD_COLUMNS:
            c = col(col_name) < 0
            condition = c if condition is None else condition | c

        negative_count = df.filter(condition).count() if condition else 0
        passed = negative_count == 0

        return DQResult(
            check_name="non_negative_downloads",
            passed=passed,
            blocking=False,  # 仅告警，不阻断
            detail=f"Rows with negative download values: {negative_count:,}",
            violation_count=negative_count,
            total_count=total,
        )

    # ─── 检查 #5: 等式校验 ───

    def _check_equation(self, df: DataFrame, total: int) -> DQResult:
        # downloads_total should == downloads_featured + downloads_organic
        violation_count = df.filter(
            col("downloads_total")
            != (col("downloads_featured") + col("downloads_organic"))
        ).count()

        violation_rate = violation_count / total if total > 0 else 0.0
        passed = violation_rate <= self.EQUATION_VIOLATION_THRESHOLD

        return DQResult(
            check_name="equation_total_eq_featured_plus_organic",
            passed=passed,
            blocking=False,  # 仅告警，不阻断
            detail=(
                f"Rows where total != featured + organic: {violation_count:,} "
                f"({violation_rate:.4%}, threshold={self.EQUATION_VIOLATION_THRESHOLD:.2%})"
            ),
            violation_count=violation_count,
            total_count=total,
        )
