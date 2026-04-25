# glue/lib/schema_v1_narrow.py
"""
Download Channel 窄表 (v1) Schema 定义

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
NARROW_V1_SCHEMA = StructType([
    StructField("dt",                DateType(),          False),
    StructField("product_id",        LongType(),          False),
    StructField("app_store",         StringType(),        False),
    StructField("country",           StringType(),        False),
    StructField("device",            StringType(),        False),
    StructField("channel",           StringType(),        False),
    StructField("downloads",         LongType(),          False),
    StructField("share_pct",         DecimalType(6, 4),   True),
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
