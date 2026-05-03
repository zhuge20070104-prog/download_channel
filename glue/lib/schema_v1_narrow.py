# glue/lib/schema_v1_narrow.py
"""
Download Channel 窄表 Schema 定义

Bronze 层保留原始窄表格式：每行一个 (date, app, country, device, channel) 的下载量。
Silver Glue Job 负责将窄表 pivot 成宽表。
"""

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Bronze 层窄表 schema（从 CSV/Parquet 读入后的目标 schema）
#
# 窄表语义：每行 = 一个 (date, app, country, device, channel) 组合的下载量。
# 同一个 (dt, product_id, app_store, country, device) 在表里**有 4 行**，
# 分别对应 paid_featured / paid_organic / unpaid_featured / unpaid_organic 四个 channel。
#
# 字段含义:
#   dt                — 数据日期 (UTC)，分区键
#   product_id        — Data.ai 统一的 App ID
#   app_store         — ios | google-play
#   country           — ISO 3166-1 alpha-2
#   device            — iphone | ipad | android-phone | android-tablet
#   channel           — 四象限之一 (见 VALID_CHANNELS)
#   downloads         — 该 channel 当日的估计下载数（绝对值）
#   share_pct         — 该 channel 占当日"该 (dt, app, store, country, device) 全 channel 总下载量"的比例。
#                       值域 [0, 1]，理论上 4 行 share_pct 之和 = 1。
#                       例：iOS / 美国 / iPhone / app=X / dt=...：
#                         paid_featured  : downloads=400 share_pct=0.4
#                         paid_organic   : downloads=100 share_pct=0.1
#                         unpaid_featured: downloads=300 share_pct=0.3
#                         unpaid_organic : downloads=200 share_pct=0.2
#                       注：share_pct 是冗余字段（可从 downloads 推算），保留是为了和上游字段对齐 +
#                       减少下游每次都做窗口聚合除法的开销。Silver pivot 后**不直接用**它，
#                       而是用 downloads 重算 paid_share / featured_share（避免精度漂移）。
#   is_estimate_final — 该行的下载量是否已 finalize（终值），决定下游能否信任这行数据。
#                       Data.ai 的下载量是估算值，**同一个 dt 的数据会经历两个生命阶段**：
#                         1) Preview 阶段（dt 当天 ~ dt+7 天内）：is_estimate_final=False。
#                            Data.ai 持续根据新到达的遥测数据修正估算，downloads 数值可能每天变。
#                            这就是 PLAN.md §4.1 提到的 "trailing 7 days restate" 的来源。
#                         2) Finalized（约 dt+7 天后，每周二 PT 8am 前后批量翻牌）：
#                            is_estimate_final=True，downloads 不再变。
#                       下游典型用法：
#                         - 严肃报表 (董事会/财务): WHERE is_estimate_final = TRUE  → 拿稳定值
#                         - 实时监控 (运营看趋势): 不过滤，但用 dt 限定最近 N 天
#                       数据质量：finalized 行理论上不该再变；如果见到 (dt, key) 已 finalized 但
#                       新文件又把 downloads 改了，DQ 检查应告警（潜在上游 bug）。
#   ingest_ts         — Glue Bronze Job 写入时间（不是 Data.ai 的源时间，仅记录"我们什么时候碰过这行"）
NARROW_V1_SCHEMA = StructType([
    StructField("dt",                DateType(),          False),
    StructField("product_id",        LongType(),          False),
    StructField("app_store",         StringType(),        False),
    StructField("country",           StringType(),        False),
    StructField("device",            StringType(),        False),
    StructField("channel",           StringType(),        False),
    StructField("downloads",         LongType(),          False),
    StructField("share_pct",         DecimalType(6, 4),   True),  # 该 channel 占当日同 key 全 channel 总和的比例，4 行加起来 ≈ 1
    StructField("is_estimate_final", BooleanType(),       True),
    StructField("ingest_ts",         TimestampType(),     False),
])

# 窄表逻辑主键
NARROW_V1_PK = ["dt", "product_id", "app_store", "country", "device", "channel"]

# channel 合法值
VALID_CHANNELS = {
    "paid_featured",
    "paid_organic",
    "unpaid_featured",
    "unpaid_organic",
}

# app_store 合法值
VALID_APP_STORES = {"ios", "google-play"}

# device 合法值
VALID_DEVICES = {"iphone", "ipad", "android-phone", "android-tablet"}
