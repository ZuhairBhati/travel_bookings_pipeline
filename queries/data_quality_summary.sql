-- =============================================================================
-- TRAVEL BOOKING SCD2 MERGE PROJECT - DATA QUALITY SUMMARY
-- =============================================================================
-- This script creates and populates daily data quality summary metrics
-- Purpose: Provides aggregated DQ metrics for monitoring and reporting
-- Business Value: DQ trend analysis, compliance reporting, operational monitoring
-- Dependencies: Requires ops.dq_results table to be populated by DQ notebooks

-- =============================================================================
-- DQ SUMMARY TABLE CREATION
-- =============================================================================
-- Create table for daily data quality summary metrics
-- business_date: Date dimension for time-based DQ analysis
-- dataset: Dataset name (booking_inc, customer_inc, etc.)
-- checks_passed: Number of successful DQ checks
-- checks_failed: Number of failed DQ checks
-- recorded_at: Timestamp when summary was calculated

CREATE TABLE IF NOT EXISTS travel_bookings.ops.dq_daily_summary (
  business_date DATE,
  dataset STRING,
  checks_passed INT,
  checks_failed INT,
  recorded_at TIMESTAMP
) USING DELTA;

-- =============================================================================
-- DQ SUMMARY CALCULATION AND MERGE
-- =============================================================================
-- Calculate daily DQ summary metrics from detailed DQ results
-- Aggregates check results by dataset for high-level monitoring
-- Uses MERGE for idempotent updates (can be run multiple times safely)
-- Parameter binding allows flexible date specification

MERGE INTO travel_bookings.ops.dq_daily_summary t
USING (
  SELECT
    COALESCE(TRY_CAST(:arrival_date AS DATE), current_date()) AS business_date,
    dataset,
    SUM(CASE WHEN constraint_status = 'Success' THEN 1 ELSE 0 END) AS checks_passed,
    SUM(CASE WHEN constraint_status <> 'Success' THEN 1 ELSE 0 END) AS checks_failed,
    current_timestamp() AS recorded_at
  FROM travel_bookings.ops.dq_results
  WHERE business_date = COALESCE(TRY_CAST(:arrival_date AS DATE), current_date())
  GROUP BY dataset
) s
ON t.business_date = s.business_date AND t.dataset = s.dataset
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;