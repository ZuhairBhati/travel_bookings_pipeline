-- =============================================================================
-- TRAVEL BOOKING SCD2 MERGE PROJECT - DAILY REVENUE ANALYTICS
-- =============================================================================
-- This script creates and populates daily revenue analytics by booking type
-- Purpose: Provides daily revenue metrics for business reporting and analysis
-- Business Value: Revenue tracking, booking type performance, daily trends
-- Dependencies: Requires bronze.booking_inc table to be populated

-- =============================================================================
-- DAILY REVENUE TABLE CREATION
-- =============================================================================
-- Create table for daily revenue analytics with auto-optimization
-- business_date: Date dimension for time-based analysis
-- booking_type: Booking type dimension for segmentation
-- total_amount: Net revenue after discounts
-- total_quantity: Total number of bookings

CREATE TABLE IF NOT EXISTS travel_bookings.analytics.daily_revenue_by_type (
  business_date DATE,
  booking_type STRING,
  total_amount DOUBLE,
  total_quantity BIGINT
) USING DELTA
TBLPROPERTIES (
  delta.autoOptimize.optimizeWrite = true,    -- Auto-optimize writes
  delta.autoOptimize.autoCompact = true       -- Auto-compact small files
);

-- =============================================================================
-- IDEMPOTENT DATA LOADING
-- =============================================================================
-- Delete existing data for the specified date to ensure idempotency
-- Uses parameter binding for flexible date specification
-- Falls back to current date if no parameter provided

DELETE FROM travel_bookings.analytics.daily_revenue_by_type
WHERE business_date = COALESCE(TRY_CAST(:arrival_date AS DATE), current_date());

-- =============================================================================
-- DAILY REVENUE CALCULATION
-- =============================================================================
-- Calculate daily revenue metrics from bronze booking data
-- Net amount: amount - discount for accurate revenue calculation
-- Aggregates by booking type for business segmentation
-- Uses explicit casting for data type consistency

INSERT INTO travel_bookings.analytics.daily_revenue_by_type (business_date, booking_type, total_amount, total_quantity)
SELECT
  COALESCE(TRY_CAST(:arrival_date AS DATE), current_date()) AS business_date,
  booking_type,
  CAST(SUM(CAST(amount AS DOUBLE) - CAST(discount AS DOUBLE)) AS DOUBLE) AS total_amount,
  CAST(SUM(CAST(quantity AS BIGINT)) AS BIGINT) AS total_quantity
FROM travel_bookings.bronze.booking_inc
WHERE business_date = COALESCE(TRY_CAST(:arrival_date AS DATE), current_date())
GROUP BY booking_type;