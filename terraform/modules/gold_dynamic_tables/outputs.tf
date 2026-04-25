# modules/gold_dynamic_tables/outputs.tf

output "dynamic_table_names" {
  description = "Names of Dynamic Tables created via SQL"
  value = [
    "DC_DAILY_BY_APP",
    "DC_DAILY_BY_COUNTRY",
    "DC_PAID_VS_ORGANIC_TREND",
  ]
}
