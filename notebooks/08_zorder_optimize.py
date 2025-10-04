# Databricks notebook source
# =============================================================================
# TRAVEL BOOKING SCD2 MERGE PROJECT - TABLE OPTIMIZATION: Z-ORDER & VACUUM
# =============================================================================
# This notebook optimizes Delta tables for query performance
# Purpose: Applies Z-ORDER clustering and VACUUM operations for optimal performance
# Performance: Improves query speed through data clustering and file cleanup
# Output: Optimized tables with better query performance and reduced storage

# =============================================================================
# PARAMETER EXTRACTION WITH DEFAULTS
# =============================================================================
# Extract widget parameters with fallback defaults for flexibility
# catalog: Unity Catalog name (defaults to travel_bookings)
# schema: Target schema (defaults to default)

try:
    catalog = dbutils.widgets.get("catalog")
except Exception:
    catalog = "travel_bookings"
try:
    schema = dbutils.widgets.get("schema")
except Exception:
    schema = "default"

# =============================================================================
# Z-ORDER CLUSTERING OPTIMIZATION
# =============================================================================
# Apply Z-ORDER clustering to improve query performance
# booking_fact: Clustered by customer_id and booking_type for common join patterns
# customer_dim: Clustered by customer_id for dimension lookups
# Z-ORDER co-locates related data for faster scans and joins

spark.sql(f"OPTIMIZE {catalog}.{schema}.booking_fact ZORDER BY (customer_id, booking_type)")
spark.sql(f"OPTIMIZE {catalog}.{schema}.customer_dim ZORDER BY (customer_id)")

# =============================================================================
# VACUUM OPERATIONS
# =============================================================================
# Clean up old files and optimize storage
# RETAIN 168 HOURS: Keeps files for 7 days to support time travel
# Removes files that are no longer referenced by the current table version
# Reduces storage costs and improves performance

spark.sql(f"VACUUM {catalog}.{schema}.booking_fact RETAIN 168 HOURS")
spark.sql(f"VACUUM {catalog}.{schema}.customer_dim RETAIN 168 HOURS")

print("Table optimization complete")
