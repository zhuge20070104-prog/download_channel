# modules/gold_dynamic_tables/main.tf
#
# Dynamic Tables are created via snowflake_sql/05_gold_dynamic_tables.sql.
# This module manages only the grants that need to stay in Terraform state.

# Grant SELECT on all future tables in GOLD to reader
resource "snowflake_grant_privileges_to_account_role" "gold_future_select_reader" {
  privileges        = ["SELECT"]
  account_role_name = var.reader_role_name
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = "\"${var.database_name}\".\"${var.gold_schema}\""
    }
  }
}

# Grant SELECT on all future dynamic tables in GOLD to reader
resource "snowflake_grant_privileges_to_account_role" "gold_future_select_dt_reader" {
  privileges        = ["SELECT"]
  account_role_name = var.reader_role_name
  on_schema_object {
    future {
      object_type_plural = "DYNAMIC TABLES"
      in_schema          = "\"${var.database_name}\".\"${var.gold_schema}\""
    }
  }
}

# Grant SELECT on all future tables/DTs in GOLD to transform
resource "snowflake_grant_privileges_to_account_role" "gold_future_all_transform" {
  privileges        = ["SELECT"]
  account_role_name = var.transform_role_name
  on_schema_object {
    future {
      object_type_plural = "DYNAMIC TABLES"
      in_schema          = "\"${var.database_name}\".\"${var.gold_schema}\""
    }
  }
}
