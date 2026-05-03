# glue/lib/schema_v2_wide.py
"""
Download Channel 宽表 Schema 定义

Silver 层统一输出格式：Bronze 的窄表由 Silver Job pivot 成此 schema。
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

# 宽表 schema（Silver 统一格式）
#
# 宽表语义：每行 = 一个 (dt, product, store, country, device)，4 个 channel 都被 PIVOT 成列。
# 比窄表少一行 channel 维度——窄表 4 行在这里塌缩成 1 行。
#
# 下载量列分三组：
#   下载量总和:           downloads_total
#   一级维度（来源）:     downloads_featured (商店主动推荐) + downloads_organic (用户主动发现)
#   二级维度（四象限叶子）: downloads_{paid|unpaid}_{featured|organic}
#
# 不变量（DQ §12 等式校验依赖）:
#   downloads_total    = downloads_featured + downloads_organic
#   downloads_featured = downloads_paid_featured + downloads_unpaid_featured
#   downloads_organic  = downloads_paid_organic  + downloads_unpaid_organic
#
# Share 列说明（与窄表 share_pct 的对应关系）:
#   窄表 share_pct 是 per-channel 的占比（4 行各 1 个）；宽表把它变成 2 个**正交维度**的占比：
#
#     paid_share     = (downloads_paid_featured + downloads_paid_organic) / downloads_total
#                    ≡ 窄表里 share_pct(paid_featured) + share_pct(paid_organic)
#                    含义：付费投放贡献的下载占当日该 key 全部下载的比例
#
#     featured_share = (downloads_paid_featured + downloads_unpaid_featured) / downloads_total
#                    ≡ 窄表里 share_pct(paid_featured) + share_pct(unpaid_featured)
#                    含义：商店推荐位（含编辑精选/榜单/推广位）贡献的下载占总下载的比例
#
#   两者互不蕴含——一个 app 可以 paid_share=0.8（大量买量）且 featured_share=0.1（基本不上推荐位），
#   或反过来 paid_share=0.05 但 featured_share=0.6（被编辑选中、自然流量主导）。
#
# 注：窄表 → 宽表 pivot 时，Silver Job **重新用 downloads 字段计算** paid_share / featured_share，
#     不直接 sum 窄表的 share_pct（避免 4 行的 Decimal 累计精度漂移）。
WIDE_V2_SCHEMA = StructType([
    StructField("dt",                        DateType(),          False),
    StructField("product_id",                LongType(),          False),
    StructField("app_store",                 StringType(),        False),
    StructField("country",                   StringType(),        False),
    StructField("device",                    StringType(),        False),
    StructField("downloads_total",           LongType(),          False),  # = featured + organic
    StructField("downloads_featured",        LongType(),          False),  # 商店主动推荐流量（编辑精选 + 榜单 + 推广位）
    StructField("downloads_organic",         LongType(),          False),  # 用户主动发现流量（搜索 + 浏览 + 直达）
    StructField("downloads_paid_featured",   LongType(),          True),   # 四象限叶子，可空：上游某些维度可能未拆分
    StructField("downloads_paid_organic",    LongType(),          True),
    StructField("downloads_unpaid_featured", LongType(),          True),
    StructField("downloads_unpaid_organic",  LongType(),          True),
    StructField("paid_share",                DecimalType(6, 4),   True),   # paid 维度占总下载的比例（与 featured 正交）
    StructField("featured_share",            DecimalType(6, 4),   True),   # featured 维度占总下载的比例（与 paid 正交）
    # is_estimate_final: 整行的下载量是否已 finalize（终值）。
    #   False = preview（dt+7 天内，downloads 还会被 Data.ai 修正）
    #   True  = finalized（每周二 PT 8am 后批量翻牌，downloads 不再变）
    #   下游严肃报表应过滤 is_estimate_final = TRUE 拿稳定值；
    #   实时看板不过滤但限定最近 N 天。详见 NARROW_V1_SCHEMA 的同名字段注释。
    #   注：宽表是 4 channel 聚合后的一行，所有 4 channel 必须都 finalized 才能整行 = True；
    #   只要任一 channel 还是 preview，整行就 False（Silver pivot 用 MIN(is_estimate_final) 实现）。
    StructField("is_estimate_final",         BooleanType(),       True),
    StructField("ingest_ts",                 TimestampType(),     False),
])

# 宽表逻辑主键
WIDE_V2_PK = ["dt", "product_id", "app_store", "country", "device"]

# Silver 输出列顺序（写 Parquet 时用）
SILVER_OUTPUT_COLUMNS = [f.name for f in WIDE_V2_SCHEMA.fields]
