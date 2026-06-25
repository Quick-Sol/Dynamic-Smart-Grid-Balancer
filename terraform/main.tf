# terraform/main.tf
# Azure + Databricks infrastructure for Smart Grid

terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
  }
}

# Resource Group
resource "azurerm_resource_group" "grid" {
  name     = "rg-smartgrid-${var.environment}"
  location = var.location
  tags = {
    Project     = "SmartGrid-DemandBalancer"
    CostCenter  = "Utility-Ops"
  }
}

# ============================================
# KAFKA (Azure Event Hubs for Kafka)
# ============================================
resource "azurerm_eventhub_namespace" "kafka" {
  name                = "ehns-grid-kafka-${var.environment}"
  location            = azurerm_resource_group.grid.location
  resource_group_name = azurerm_resource_group.grid.name
  sku                 = "Standard"
  capacity            = 20  # 20 throughput units for millions of messages
  
  kafka_enabled       = true
  
  network_rulesets {
    default_action = "Deny"
    ip_rule {
      ip_mask = var.office_ip_range
      action  = "Allow"
    }
  }
}

resource "azurerm_eventhub" "meter_readings" {
  name                = "smart-meter-readings"
  namespace_name      = azurerm_eventhub_namespace.kafka.name
  resource_group_name = azurerm_resource_group.grid.name
  partition_count     = 32  # High parallelism for 500K TPS
  message_retention   = 1   # 1 day hot retention
}

# ============================================
# DATABRICKS WORKSPACE
# ============================================
resource "azurerm_databricks_workspace" "grid" {
  name                = "dbw-smartgrid-${var.environment}"
  resource_group_name = azurerm_resource_group.grid.name
  location            = azurerm_resource_group.grid.location
  sku                 = "premium"
  
  custom_parameters {
    virtual_network_id                                   = azurerm_virtual_network.grid.id
    public_subnet_name                                 = "public-subnet"
    private_subnet_name                                  = "private-subnet"
    public_subnet_network_security_group_association_id  = azurerm_subnet_network_security_group_association.public.id
    private_subnet_network_security_group_association_id = azurerm_subnet_network_security_group_association.private.id
  }
}

# Auto-scaling cluster for streaming
resource "databricks_cluster" "streaming" {
  cluster_name            = "grid-streaming-cluster"
  spark_version             = "13.3.x-scala2.12"
  node_type_id              = "Standard_DS13_v2"  # Memory optimized
  autotermination_minutes   = 0  # Never terminate (24/7 grid ops)
  
  autoscale {
    min_workers = 4
    max_workers = 50  # Scale for heatwave events
  }
  
  spark_conf = {
    "spark.databricks.delta.preview.enabled" = "true"
    "spark.sql.adaptive.enabled"             = "true"
    "spark.sql.adaptive.coalescePartitions.enabled" = "true"
  }
  
  cluster_log_conf {
    dbfs {
      destination = "dbfs:/cluster-logs/grid-streaming"
    }
  }
}

# SQL Warehouse for analysts
resource "databricks_sql_endpoint" "grid_analytics" {
  name                      = "Grid Analytics Warehouse"
  cluster_size              = "Large"
  min_num_clusters          = 1
  max_num_clusters          = 3
  auto_stop_mins            = 10
  enable_photon             = true  # Accelerated queries
  enable_serverless_compute = true
}

# ============================================
# AZURE BLOB STORAGE (Cold Tier)
# ============================================
resource "azurerm_storage_account" "cold" {
  name                     = "stgridcold${var.environment}"
  resource_group_name      = azurerm_resource_group.grid.name
  location                 = azurerm_resource_group.grid.location
  account_tier             = "Standard"
  account_replication_type = "GRS"  # Geo-redundant for disaster recovery
  access_tier              = "Archive"  # Start with archive tier
  
  blob_properties {
    versioning_enabled = true
    
    delete_retention_policy {
      days = 7
    }
  }
  
  network_rules {
    default_action = "Deny"
    ip_rules       = [var.office_ip_range]
    bypass         = ["AzureServices"]
  }
}

resource "azurerm_storage_container" "telemetry" {
  name                  = "grid-telemetry-archive"
  storage_account_name  = azurerm_storage_account.cold.name
  container_access_type = "private"
}

# Lifecycle policy: Move to coldest tier after 90 days
resource "azurerm_storage_management_policy" "cold_tier" {
  storage_account_id = azurerm_storage_account.cold.id

  rule {
    name    = "grid-telemetry-lifecycle"
    enabled = true
    filters {
      prefix_match = ["grid-telemetry-archive/"]
      blob_types   = ["blockBlob"]
    }
    actions {
      base_blob {
        tier_to_cool_after_days_since_modification_greater_than    = 30
        tier_to_archive_after_days_since_modification_greater_than = 90
        delete_after_days_since_modification_greater_than          = 2555  # 7 years retention
      }
    }
  }
}

# ============================================
# MONITORING & ALERTING
# ============================================
resource "azurerm_monitor_action_group" "grid_ops" {
  name                = "ag-grid-operations"
  resource_group_name = azurerm_resource_group.grid.name
  short_name          = "gridops"

  email_receiver {
    name          = "grid-ops-team"
    email_address = "grid-ops@utility.com"
  }
  
  webhook_receiver {
    name        = "pagerduty-grid"
    service_uri = var.pagerduty_webhook_url
  }
}

# Alert: Kafka lag (backpressure indicator)
resource "azurerm_monitor_metric_alert" "kafka_lag" {
  name                = "alert-kafka-consumer-lag"
  resource_group_name = azurerm_resource_group.grid.name
  scopes              = [azurerm_eventhub_namespace.kafka.id]
  description         = "Alert when Kafka consumer lag grows - indicates processing bottleneck"
  
  criteria {
    metric_namespace = "Microsoft.EventHub/namespaces"
    metric_name      = "IncomingMessages"
    aggregation      = "Total"
    operator         = "GreaterThan"
    threshold        = 100000000  # 100M messages backlog
  
    dimension {
      name     = "EntityName"
      operator = "Include"
      values   = ["smart-meter-readings"]
    }
  }
  
  action {
    action_group_id = azurerm_monitor_action_group.grid_ops.id
  }
}

# Alert: Predicted grid overload
resource "azurerm_monitor_scheduled_query_rules_alert" "overload_prediction" {
  name                = "alert-predicted-overload"
  location            = azurerm_resource_group.grid.location
  resource_group_name = azurerm_resource_group.grid.name
  
  action {
    action_group = [azurerm_monitor_action_group.grid_ops.id]
  }
  
  data_source {
    resource_id = azurerm_databricks_workspace.grid.id
  }
  
  query = <<-QUERY
    grid_predictions
    | where estimated_minutes_to_overload < 15
    | project zip_code, mins_to_overload, alert_level
  QUERY
  
  trigger {
    operator  = "GreaterThan"
    threshold = 0
  }
}

# ============================================
# COST OPTIMIZATION TAGS
# ============================================
locals {
  common_tags = {
    CostCenter    = "Utility-GridOps"
    Environment   = var.environment
    Project       = "SmartGrid-DemandBalancer"
    AutoShutdown  = "false"  # Critical infrastructure
    BudgetAlert   = "monthly-5000"
  }
}
