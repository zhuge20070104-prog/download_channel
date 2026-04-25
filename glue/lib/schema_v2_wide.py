# glue/lib/schema_v2_wide.py
"""
Download Channel 宽表 (v2) Schema 定义

Bronze 层 v2 保留原始宽表格式。
Silver 层统一使用此 schema（窄表由 Silver Job pivot 成此格式）。
Snowflake 的 SILVER.DC_WIDE 表也基于此 schema + _loaded_at 列。
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

# 宽表 schema（Bronze v2 / Silver 统一格式）
WIDE_V2_SCHEMA = StructType([
    StructField("dt",                        DateType(),          False),
    StructField("product_id",                LongType(),          False),
    StructField("app_store",                 StringType(),        False),
    StructField("country",                   StringType(),        False),
    StructField("device",                    StringType(),        False),
    StructField("downloads_total",           LongType(),          False),
    StructField("downloads_featured",        LongType(),          False),
    StructField("downloads_organic",         LongType(),          False),
    StructField("downloads_paid_featured",   LongType(),          True),
    StructField("downloads_paid_organic",    LongType(),          True),
    StructField("downloads_unpaid_featured", LongType(),          True),
    StructField("downloads_unpaid_organic",  LongType(),          True),
    StructField("paid_share",               DecimalType(6, 4),    True),
    StructField("featured_share",           DecimalType(6, 4),    True),
    StructField("is_estimate_final",         BooleanType(),       True),
    StructField("ingest_ts",                 TimestampType(),     False),
])

# 宽表逻辑主键
WIDE_V2_PK = ["dt", "product_id", "app_store", "country", "device"]

# Silver 输出列顺序（写 Parquet 时用）
SILVER_OUTPUT_COLUMNS = [f.name for f in WIDE_V2_SCHEMA.fields]
