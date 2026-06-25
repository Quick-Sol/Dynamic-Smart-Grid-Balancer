-- ============================================
-- PREDICTIVE GRID ANALYTICS SQL
-- Run on Databricks SQL Warehouse or Spark SQL
-- ============================================

-- 1. IMMINENT OVERLOAD DETECTION (Last 15 min acceleration)
WITH load_acceleration AS (
  SELECT 
    zip_code,
    window_start,
    minute_load_kw,
    
    -- LAG: Previous reading for rate-of-change
    LAG(minute_load_kw, 1) OVER (PARTITION BY zip_code ORDER BY window_start) as prev_load,
    
    -- LEAD: Look ahead to see if trend continues
    LEAD(minute_load_kw, 5) OVER (PARTITION BY zip_code ORDER BY window_start) as future_5min_load,
    
    -- Cumulative load over sliding 15-minute window
    SUM(minute_load_kw) OVER (
      PARTITION BY zip_code 
      ORDER BY window_start 
      ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
    ) as cumulative_15min_mw,
    
    -- Moving average trend
    AVG(minute_load_kw) OVER (
      PARTITION BY zip_code 
      ORDER BY window_start 
      ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
    ) as ma_15min,
    
    -- Standard deviation (volatility indicator)
    STDDEV(minute_load_kw) OVER (
      PARTITION BY zip_code 
      ORDER BY window_start 
      ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
    ) as load_volatility
    
  FROM grid.minute_loads
  WHERE window_start >= current_timestamp() - INTERVAL 20 MINUTES
),
predictions AS (
  SELECT 
    zip_code,
    window_start,
    minute_load_kw,
    
    -- Acceleration calculation
    (minute_load_kw - prev_load) / 1.0 as load_acceleration_mw_per_min,
    
    -- Linear projection: if this acceleration continues...
    minute_load_kw + ((minute_load_kw - prev_load) * 15) as projected_15min_load,
    
    -- Risk score: how close to zip capacity (assume 50MW typical capacity)
    (cumulative_15min_mw / 1000.0) / 50.0 * 100 as capacity_utilization_pct,
    
    -- Volatility-adjusted risk
    CASE 
      WHEN load_volatility > 5000 AND (minute_load_kw - prev_load) > 1000 
      THEN 'EXPLOSIVE_GROWTH'
      WHEN (minute_load_kw - prev_load) > 500 
      THEN 'RAPID_ACCEL'
      WHEN (minute_load_kw - prev_load) > 0 
      THEN 'MODERATE_GROWTH'
      ELSE 'STABLE_OR_DECLINING'
    END as growth_regime,
    
    -- Time-to-overload estimate (minutes until 80% capacity)
    CASE 
      WHEN (minute_load_kw - prev_load) > 0 
      THEN GREATEST(0, (50000 - minute_load_kw) / NULLIF((minute_load_kw - prev_load), 0))
      ELSE 999
    END as estimated_minutes_to_overload

  FROM load_acceleration
)
SELECT 
  zip_code,
  window_start,
  ROUND(minute_load_kw / 1000, 2) as current_load_mw,
  ROUND(load_acceleration_mw_per_min / 1000, 2) as accel_mw_per_min,
  ROUND(projected_15min_load / 1000, 2) as projected_load_mw,
  ROUND(capacity_utilization_pct, 1) as capacity_pct,
  growth_regime,
  ROUND(estimated_minutes_to_overload, 1) as mins_to_overload,
  
  -- FINAL ALERT LOGIC
  CASE 
    WHEN estimated_minutes_to_overload < 5 THEN '🔴 CRITICAL: Load shedding required NOW'
    WHEN estimated_minutes_to_overload < 15 THEN '🟠 HIGH: Spin up reserves immediately'
    WHEN estimated_minutes_to_overload < 30 THEN '🟡 MEDIUM: Alert operators, prep DR'
    ELSE '🟢 NORMAL'
  END as grid_alert_status,
  
  -- RECOMMENDED ACTIONS
  CASE 
    WHEN estimated_minutes_to_overload < 5 THEN 
      'Execute emergency load shedding. Disconnect non-essential industrial. Deploy battery reserves.'
    WHEN estimated_minutes_to_overload < 15 THEN 
      'Start peaker plants. Send demand response signals to smart thermostats. Import power from neighboring grid.'
    WHEN estimated_minutes_to_overload < 30 THEN 
      'Pre-position crews. Notify large industrial customers of potential curtailment.'
    ELSE 'Continue normal operations'
  END as recommended_action

FROM predictions
WHERE window_start >= current_timestamp() - INTERVAL 5 MINUTES
  AND estimated_minutes_to_overload < 60  -- Only show concerning zones
ORDER BY estimated_minutes_to_overload ASC, window_start DESC;

-- ============================================
-- 2. CORRELATED SURGE DETECTION (Heatwave Pattern)
-- Detects when multiple zip codes surge simultaneously
-- ============================================

WITH zip_trends AS (
  SELECT 
    zip_code,
    window_start,
    minute_load_kw,
    LAG(minute_load_kw, 3) OVER (PARTITION BY zip_code ORDER BY window_start) as load_3min_ago,
    -- 3-minute rate of change
    (minute_load_kw - LAG(minute_load_kw, 3) OVER (PARTITION BY zip_code ORDER BY window_start)) / 3.0 as roc_3min
  FROM grid.minute_loads
  WHERE window_start >= current_timestamp() - INTERVAL 10 MINUTES
),
surging_zips AS (
  SELECT 
    window_start,
    zip_code,
    roc_3min,
    CASE WHEN roc_3min > 2000 THEN 1 ELSE 0 END as is_surging  -- 2MW/3min threshold
  FROM zip_trends
  WHERE roc_3min > 1000  -- Only significant changes
)
SELECT 
  window_start,
  COUNT(DISTINCT zip_code) as surging_zones,
  SUM(roc_3min) / 1000.0 as total_surge_mw,
  COLLECT_LIST(zip_code) as affected_zips,
  
  CASE 
    WHEN COUNT(DISTINCT zip_code) > 5 AND SUM(roc_3min) > 50000 THEN 'WIDESPREAD_GRID_EMERGENCY'
    WHEN COUNT(DISTINCT zip_code) > 3 THEN 'MULTI_ZONE_EVENT'
    ELSE 'LOCALIZED_EVENT'
  END as event_classification

FROM surging_zips
GROUP BY window_start
HAVING COUNT(DISTINCT zip_code) >= 2  -- At least 2 zones surging
ORDER BY window_start DESC;

-- ============================================
-- 3. RENEWABLE REALLOCATION OPTIMIZER
-- Suggests where to route excess renewable capacity
-- ============================================

WITH current_state AS (
  SELECT 
    z.zip_code,
    z.load_mw,
    z.avg_temp_f,
    p.overload_risk_score,
    p.predicted_15min / 1000.0 as predicted_load_mw,
    r.solar_generation_mw,
    r.wind_generation_mw,
    r.battery_soc_pct,
    -- Available renewable surplus
    GREATEST(0, (r.solar_generation_mw + r.wind_generation_mw) - z.load_mw) as renewable_surplus_mw
  FROM grid.zip_code_loads z
  JOIN grid.overload_predictions p ON z.zip_code = p.zip_code 
    AND z.window_start = p.window_start
  LEFT JOIN grid.renewable_assets r ON z.zip_code = r.zip_code
  WHERE z.window_start >= current_timestamp() - INTERVAL 2 MINUTES
),
reallocation_plan AS (
  SELECT 
    zip_code,
    load_mw,
    predicted_load_mw,
    overload_risk_score,
    renewable_surplus_mw,
    
    -- Deficit zones need power
    CASE 
      WHEN predicted_load_mw > (load_mw * 1.2) THEN predicted_load_mw - load_mw 
      ELSE 0 
    END as projected_deficit_mw,
    
    -- Priority score: higher risk = higher priority for receiving power
    overload_risk_score * (predicted_load_mw / NULLIF(load_mw, 0)) as reallocation_priority
    
  FROM current_state
)
SELECT 
  zip_code,
  ROUND(load_mw, 2) as current_load,
  ROUND(predicted_load_mw, 2) as predicted_load,
  ROUND(overload_risk_score, 1) as risk_score,
  ROUND(renewable_surplus_mw, 2) as surplus_mw,
  ROUND(projected_deficit_mw, 2) as deficit_mw,
  ROUND(reallocation_priority, 2) as priority_score,
  
  CASE 
    WHEN renewable_surplus_mw > 0 AND overload_risk_score < 30 THEN 'EXPORT_ZONE'
    WHEN projected_deficit_mw > 0 OR overload_risk_score > 60 THEN 'IMPORT_ZONE'
    ELSE 'BALANCED_ZONE'
  END as grid_role,
  
  -- Specific reallocation commands
  CASE 
    WHEN renewable_surplus_mw > 5 THEN 
      'TRANSFER ' || ROUND(renewable_surplus_mw * 0.8, 1) || ' MW TO NEAREST IMPORT ZONE'
    WHEN projected_deficit_mw > 2 THEN 
      'REQUEST ' || ROUND(projected_deficit_mw, 1) || ' MW FROM NEAREST EXPORT ZONE'
    ELSE 'MAINTAIN_CURRENT_FLOWS'
  END as grid_command

FROM reallocation_plan
ORDER BY reallocation_priority DESC;
