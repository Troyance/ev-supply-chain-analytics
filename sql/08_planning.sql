-- planning: turns the latest forecasts into a part-level demand plan and buy signals,
-- and lines up old forecasts against what actually happened.

-- part demand per site per week for the next 13 weeks.
-- plants: forecast builds x bom (dependent demand). service dc: the service parts forecast directly.
CREATE OR REPLACE TABLE workspace.ev_supply_chain.gold_part_demand_plan AS
WITH latest_run AS (
  SELECT run_id, forecast_type
  FROM workspace.ev_supply_chain.forecast_run
  WHERE run_type = 'production'
  QUALIFY row_number() OVER (PARTITION BY forecast_type ORDER BY created_at DESC) = 1
),
fc AS (
  SELECT f.*
  FROM workspace.ev_supply_chain.fct_forecast f
  JOIN latest_run USING (run_id, forecast_type)
)
SELECT
  fc.run_id,
  fc.site_id,
  b.part_id,
  fc.target_week,
  fc.horizon_weeks,
  'dependent'                                 AS demand_type,
  sum(fc.forecast_p10 * b.qty_per_vehicle)    AS demand_p10,
  sum(fc.forecast_p50 * b.qty_per_vehicle)    AS demand_p50,
  -- adding p90s across models assumes they all run high together. conservative, which is fine here
  sum(fc.forecast_p90 * b.qty_per_vehicle)    AS demand_p90
FROM fc
JOIN workspace.ev_supply_chain.dim_bom b ON b.model_id = fc.model_id
WHERE fc.forecast_type = 'vehicle_builds'
GROUP BY ALL

UNION ALL

SELECT run_id, site_id, part_id, target_week, horizon_weeks, 'independent',
       forecast_p10, forecast_p50, forecast_p90
FROM fc
WHERE forecast_type = 'service_parts';

-- one row per site x part: run rate, safety stock, reorder point, and what to do about it today.
CREATE OR REPLACE TABLE workspace.ev_supply_chain.gold_buy_signals AS
WITH plan AS (
  SELECT
    site_id,
    part_id,
    any_value(demand_type)                                       AS demand_type,
    sum(demand_p50) / (13 * 7)                                   AS drr,
    -- (p90 - p50) / 1.28 ~ one std dev of weekly demand; spread it over 7 days
    sqrt(avg(pow((demand_p90 - demand_p50) / 1.2816, 2)) / 7)    AS demand_sd_daily
  FROM workspace.ev_supply_chain.gold_part_demand_plan
  GROUP BY site_id, part_id
),
params AS (
  SELECT
    p.*,
    pp.part_number, pp.part_name, pp.component_category, pp.supplier_id, pp.supplier_name, pp.supplier_region,
    pp.supplier_tier, pp.unit_cost, pp.buffer_class, pp.planning_lead_time_days,
    pp.lead_time_stddev_days, pp.review_period_days, pp.service_level_z, pp.pack_size, pp.moq,
    coalesce(i.snapshot_date, DATE'2026-11-21')                  AS snapshot_date,
    coalesce(i.on_hand_qty, 0)                                   AS on_hand_qty,
    coalesce(i.on_order_qty, 0)                                  AS on_order_qty,
    CASE pp.buffer_class
      WHEN 'JIT'    THEN p.drr * 0.5    -- half a day of cover, deliveries are daily
      WHEN 'KANBAN' THEN p.drr * 3      -- min/max bins, 3 days of safety
      -- standard formula: covers both demand noise over the lead time and lead time noise
      ELSE pp.service_level_z * sqrt(
             pp.planning_lead_time_days * pow(p.demand_sd_daily, 2)
             + pow(p.drr * pp.lead_time_stddev_days, 2))
    END                                                          AS safety_stock
  FROM plan p
  JOIN workspace.ev_supply_chain.dim_part_planning pp USING (part_id)
  LEFT JOIN workspace.ev_supply_chain.fct_inventory_snapshot i USING (site_id, part_id)
),
levels AS (
  SELECT
    *,
    drr * (planning_lead_time_days + review_period_days) + safety_stock     AS reorder_point,
    drr * (planning_lead_time_days + 2 * review_period_days) + safety_stock AS order_up_to,
    on_hand_qty + on_order_qty                                              AS inventory_position,
    on_hand_qty / nullif(drr, 0)                                            AS days_of_cover
  FROM params
),
signals AS (
  SELECT
    *,
    CASE
      WHEN drr = 0                                THEN 'NO DEMAND'
      WHEN on_hand_qty < safety_stock             THEN 'EXPEDITE'   -- already into the safety buffer
      WHEN inventory_position < reorder_point     THEN 'ORDER'
      WHEN inventory_position > 2 * order_up_to   THEN 'EXCESS'
      ELSE 'OK'
    END AS signal
  FROM levels
)
SELECT
  snapshot_date,
  site_id,
  part_id,
  part_number,
  part_name,
  component_category,
  demand_type,
  buffer_class,
  supplier_id,
  supplier_name,
  supplier_region,
  supplier_tier,
  round(drr, 2)                                    AS daily_run_rate,
  round(demand_sd_daily, 2)                        AS demand_sd_daily,
  planning_lead_time_days,
  round(lead_time_stddev_days, 1)                  AS lead_time_stddev_days,
  review_period_days,
  round(safety_stock)                              AS safety_stock,
  round(safety_stock / nullif(drr, 0), 1)          AS safety_stock_days,
  round(reorder_point)                             AS reorder_point,
  round(order_up_to)                               AS order_up_to,
  on_hand_qty,
  on_order_qty,
  inventory_position,
  round(days_of_cover, 1)                          AS days_of_cover,
  days_of_cover < planning_lead_time_days          AS stockout_before_next_receipt,
  date_add(snapshot_date, cast(floor(days_of_cover) AS int)) AS projected_stockout_date,
  signal,
  -- order up to the max, at least the moq, rounded to whole packs
  CASE WHEN signal IN ('ORDER', 'EXPEDITE')
       THEN cast(greatest(ceil((order_up_to - inventory_position) / pack_size) * pack_size, moq) AS bigint)
       ELSE 0 END                                  AS suggested_order_qty,
  CASE WHEN signal IN ('ORDER', 'EXPEDITE')
       THEN round(greatest(ceil((order_up_to - inventory_position) / pack_size) * pack_size, moq) * unit_cost, 2)
       ELSE 0 END                                  AS suggested_order_value,
  date_add(snapshot_date, planning_lead_time_days) AS expected_receipt_date,
  unit_cost,
  pack_size,
  moq
FROM signals;

-- every backtest forecast next to what actually happened, at the lowest grain.
-- keep wape/bias out of this table on purpose: ratios have to be computed after aggregating
-- (that's what the metric view is for), averaging row-level wapes gives the wrong answer.
CREATE OR REPLACE TABLE workspace.ev_supply_chain.gold_forecast_accuracy AS
WITH latest_backtest AS (
  SELECT run_id, forecast_type, model_name
  FROM workspace.ev_supply_chain.forecast_run
  WHERE run_type = 'backtest'
  QUALIFY row_number() OVER (PARTITION BY forecast_type, model_name ORDER BY created_at DESC) = 1
),
actuals AS (
  SELECT 'vehicle_builds' AS forecast_type, week_start, site_id, model_id, CAST(NULL AS BIGINT) AS part_id, units
  FROM workspace.ev_supply_chain.fct_vehicle_builds
  UNION ALL
  SELECT 'service_parts', week_start, site_id, CAST(NULL AS STRING), part_id, units
  FROM workspace.ev_supply_chain.fct_service_demand
)
SELECT
  f.run_id,
  f.forecast_type,
  f.model_name,
  f.origin_week,
  f.target_week,
  f.horizon_weeks,
  f.site_id,
  f.model_id,
  f.part_id,
  coalesce(pp.component_category, 'Vehicle')        AS component_category,
  f.forecast_p10,
  f.forecast_p50,
  f.forecast_p90,
  a.units                                           AS actual_units,
  abs(f.forecast_p50 - a.units)                     AS abs_error,
  f.forecast_p50 - a.units                          AS error,
  CASE WHEN f.forecast_p10 IS NOT NULL
       THEN a.units BETWEEN f.forecast_p10 AND f.forecast_p90 END AS within_interval
FROM workspace.ev_supply_chain.fct_forecast f
JOIN latest_backtest USING (run_id, forecast_type, model_name)
JOIN actuals a
  ON  a.forecast_type = f.forecast_type
  AND a.week_start = f.target_week
  AND a.site_id = f.site_id
  AND a.model_id <=> f.model_id
  AND a.part_id <=> f.part_id
LEFT JOIN workspace.ev_supply_chain.dim_part_planning pp ON pp.part_id = f.part_id;
