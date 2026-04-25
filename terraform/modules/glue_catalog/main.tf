# modules/glue_catalog/main.tf

resource "aws_glue_catalog_database" "bronze_dc" {
  name = "iodp_dc_bronze_${var.environment}"

  tags = var.tags
}

resource "aws_glue_catalog_database" "silver_dc" {
  name = "iodp_dc_silver_${var.environment}"

  tags = var.tags
}
