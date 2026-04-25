# modules/glue_catalog/outputs.tf

output "bronze_database_name" {
  value = aws_glue_catalog_database.bronze_dc.name
}

output "silver_database_name" {
  value = aws_glue_catalog_database.silver_dc.name
}
