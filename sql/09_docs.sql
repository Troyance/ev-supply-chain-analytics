-- unity catalog docs for everything gold + the forecast tables.
-- genie and ai/bi read these, so they are written for someone who has never seen the project.

COMMENT ON SCHEMA workspace.ev_supply_chain IS 'Supply chain analytics for an electric vehicle manufacturer: supplier performance, sourcing risk, vehicle build and service parts forecasts, and replenishment buy signals. Supplier master data comes from the TPC-H sample set. Production, BOM, purchase orders and planning data are synthetic but calibrated to a realistic EV maker (about 95k vehicles a year, about $45k of parts per vehicle, about 93 percent supplier on-time delivery). Tables prefixed gold_ are the business-facing ones.';

-- supplier scorecard
COMMENT ON TABLE workspace.ev_supply_chain.gold_supplier_scorecard IS 'One row per supplier. Delivery reliability (on-time rate, lead time and its variability), total spend, a reliability tier, and whether the supplier is getting worse. Built from all purchase order delivery lines since production started in 2021. Use this to rank suppliers or find ones that need attention.';
ALTER TABLE workspace.ev_supply_chain.gold_supplier_scorecard ALTER COLUMN supplier_id COMMENT 'Supplier key. Joins to supplier_id in other tables.';
ALTER TABLE workspace.ev_supply_chain.gold_supplier_scorecard ALTER COLUMN supplier_name COMMENT 'Supplier display name, e.g. Supplier#000012345.';
ALTER TABLE workspace.ev_supply_chain.gold_supplier_scorecard ALTER COLUMN country COMMENT 'Country the supplier ships from.';
ALTER TABLE workspace.ev_supply_chain.gold_supplier_scorecard ALTER COLUMN region COMMENT 'World region the supplier ships from: America, Asia, Europe, Africa or Middle East. Asia has the longest lead times (ocean freight), America the shortest (nearshore).';
ALTER TABLE workspace.ev_supply_chain.gold_supplier_scorecard ALTER COLUMN po_lines COMMENT 'Number of purchase order lines received from this supplier.';
ALTER TABLE workspace.ev_supply_chain.gold_supplier_scorecard ALTER COLUMN spend COMMENT 'Total net spend with this supplier in USD (extended price after discount).';
ALTER TABLE workspace.ev_supply_chain.gold_supplier_scorecard ALTER COLUMN on_time_pct COMMENT 'Percent of PO lines received on or before the promised (commit) date, 0-100.';
ALTER TABLE workspace.ev_supply_chain.gold_supplier_scorecard ALTER COLUMN on_time_pct_recent COMMENT 'On-time percent for orders in the most recent 12 months of data.';
ALTER TABLE workspace.ev_supply_chain.gold_supplier_scorecard ALTER COLUMN on_time_pct_prior COMMENT 'On-time percent for orders in the 12 months before that.';
ALTER TABLE workspace.ev_supply_chain.gold_supplier_scorecard ALTER COLUMN on_time_change_pts COMMENT 'Recent minus prior on-time percent, in percentage points. Noisy at this volume - use is_declining for alerting instead.';
ALTER TABLE workspace.ev_supply_chain.gold_supplier_scorecard ALTER COLUMN avg_lead_time_days COMMENT 'Average days from order placed to goods received.';
ALTER TABLE workspace.ev_supply_chain.gold_supplier_scorecard ALTER COLUMN lead_time_stddev COMMENT 'Standard deviation of lead time in days. Higher means less predictable deliveries, which drives safety stock up.';
ALTER TABLE workspace.ev_supply_chain.gold_supplier_scorecard ALTER COLUMN p90_lead_time_days COMMENT '90th percentile lead time in days - a realistic worst case.';
ALTER TABLE workspace.ev_supply_chain.gold_supplier_scorecard ALTER COLUMN avg_days_late_when_late COMMENT 'For late lines only, the average number of days past the promised date.';
ALTER TABLE workspace.ev_supply_chain.gold_supplier_scorecard ALTER COLUMN lateness_trend_days_per_month COMMENT 'Slope of days late over the last 24 months: how many more days late the supplier gets each month, averaged over all deliveries. Around 0 is stable, positive is getting worse.';
ALTER TABLE workspace.ev_supply_chain.gold_supplier_scorecard ALTER COLUMN tier COMMENT 'Reliability tier from overall on-time rate: A (97 percent or better), B (90-97), C (under 90).';
ALTER TABLE workspace.ev_supply_chain.gold_supplier_scorecard ALTER COLUMN is_declining COMMENT 'True when the supplier is getting at least 0.15 days later per month on average (lateness_trend_days_per_month >= 0.15). The main early-warning flag.';

-- single source risk
COMMENT ON TABLE workspace.ev_supply_chain.gold_single_source_risk IS 'One row per part where a single supplier gets 80 percent or more of the spend. Shows the dependent supplier and how reliable they are. is_high_risk marks parts that are both concentrated and depend on a tier C supplier. Most auto parts are single-sourced on purpose (tooling is expensive), so the useful question is which of them depend on an unreliable supplier.';
ALTER TABLE workspace.ev_supply_chain.gold_single_source_risk ALTER COLUMN part_id COMMENT 'Part key.';
ALTER TABLE workspace.ev_supply_chain.gold_single_source_risk ALTER COLUMN part_number COMMENT 'Part number, prefixed by category: BAT, ELE, WIR, BDY, FST.';
ALTER TABLE workspace.ev_supply_chain.gold_single_source_risk ALTER COLUMN part_name COMMENT 'Part description, e.g. Cell module or Flange bolt M8x25.';
ALTER TABLE workspace.ev_supply_chain.gold_single_source_risk ALTER COLUMN component_category COMMENT 'Component category: Battery materials, Electronics, Wiring & harnesses, Body & chassis, Fasteners & fittings.';
ALTER TABLE workspace.ev_supply_chain.gold_single_source_risk ALTER COLUMN top_supplier_id COMMENT 'The supplier that receives most of the spend for this part.';
ALTER TABLE workspace.ev_supply_chain.gold_single_source_risk ALTER COLUMN top_supplier_name COMMENT 'Name of the dominant supplier.';
ALTER TABLE workspace.ev_supply_chain.gold_single_source_risk ALTER COLUMN top_supplier_country COMMENT 'Country of the dominant supplier.';
ALTER TABLE workspace.ev_supply_chain.gold_single_source_risk ALTER COLUMN top_supplier_on_time_pct COMMENT 'Overall on-time percent of the dominant supplier.';
ALTER TABLE workspace.ev_supply_chain.gold_single_source_risk ALTER COLUMN top_supplier_tier COMMENT 'Reliability tier (A/B/C) of the dominant supplier.';
ALTER TABLE workspace.ev_supply_chain.gold_single_source_risk ALTER COLUMN suppliers_used COMMENT 'How many different suppliers this part was actually bought from.';
ALTER TABLE workspace.ev_supply_chain.gold_single_source_risk ALTER COLUMN total_spend COMMENT 'Total spend on this part in USD.';
ALTER TABLE workspace.ev_supply_chain.gold_single_source_risk ALTER COLUMN top_supplier_share_pct COMMENT 'Percent of the part spend going to the dominant supplier, 80-100.';
ALTER TABLE workspace.ev_supply_chain.gold_single_source_risk ALTER COLUMN is_high_risk COMMENT 'True when the dominant supplier is tier C - concentrated and unreliable.';

-- geography
COMMENT ON TABLE workspace.ev_supply_chain.gold_spend_by_geography IS 'One row per supplier country. Spend, share of total spend, lead time and on-time rate by where suppliers ship from. Use for tariff, freight and disruption exposure questions.';
ALTER TABLE workspace.ev_supply_chain.gold_spend_by_geography ALTER COLUMN region COMMENT 'World region.';
ALTER TABLE workspace.ev_supply_chain.gold_spend_by_geography ALTER COLUMN country COMMENT 'Supplier country.';
ALTER TABLE workspace.ev_supply_chain.gold_spend_by_geography ALTER COLUMN suppliers COMMENT 'Number of suppliers in the country with at least one PO line.';
ALTER TABLE workspace.ev_supply_chain.gold_spend_by_geography ALTER COLUMN spend COMMENT 'Total spend in USD.';
ALTER TABLE workspace.ev_supply_chain.gold_spend_by_geography ALTER COLUMN spend_share_pct COMMENT 'Percent of all spend, 0-100. Sums to 100 across all rows.';
ALTER TABLE workspace.ev_supply_chain.gold_spend_by_geography ALTER COLUMN avg_lead_time_days COMMENT 'Average order-to-receipt days.';
ALTER TABLE workspace.ev_supply_chain.gold_spend_by_geography ALTER COLUMN on_time_pct COMMENT 'Percent of lines received on time, 0-100.';

-- monthly performance
COMMENT ON TABLE workspace.ev_supply_chain.gold_monthly_performance IS 'One row per order month and component category. Volume, spend, on-time rate and lead time over time. The chip shortage (June 2021 to December 2022, worst around April 2022) shows up as longer lead times and lower on-time rates for Electronics.';
ALTER TABLE workspace.ev_supply_chain.gold_monthly_performance ALTER COLUMN order_month COMMENT 'First day of the month the orders were placed. Electronics ordered from mid 2021 to end of 2022 were hit by the chip shortage.';
ALTER TABLE workspace.ev_supply_chain.gold_monthly_performance ALTER COLUMN component_category COMMENT 'Component category.';
ALTER TABLE workspace.ev_supply_chain.gold_monthly_performance ALTER COLUMN po_lines COMMENT 'Number of PO lines ordered in the month.';
ALTER TABLE workspace.ev_supply_chain.gold_monthly_performance ALTER COLUMN spend COMMENT 'Spend in USD.';
ALTER TABLE workspace.ev_supply_chain.gold_monthly_performance ALTER COLUMN on_time_pct COMMENT 'Percent of lines received on time, 0-100. Do not average across months - recompute from line counts or use the metric view.';
ALTER TABLE workspace.ev_supply_chain.gold_monthly_performance ALTER COLUMN avg_lead_time_days COMMENT 'Average order-to-receipt days.';

-- forecast data model
COMMENT ON TABLE workspace.ev_supply_chain.forecast_run IS 'One row per forecast run. Append-only. run_type is backtest (scored against history) or production (the live forward forecast). Join to fct_forecast on run_id. The latest production run per forecast_type is the current plan.';
ALTER TABLE workspace.ev_supply_chain.forecast_run ALTER COLUMN run_id COMMENT 'Unique id for the run.';
ALTER TABLE workspace.ev_supply_chain.forecast_run ALTER COLUMN run_type COMMENT 'backtest or production.';
ALTER TABLE workspace.ev_supply_chain.forecast_run ALTER COLUMN forecast_type COMMENT 'vehicle_builds (weekly units per plant and vehicle model) or service_parts (weekly units per part at the service DC).';
ALTER TABLE workspace.ev_supply_chain.forecast_run ALTER COLUMN model_name COMMENT 'Which model produced it. naive_mean is the simple baseline, the others are LightGBM models registered in Unity Catalog.';
ALTER TABLE workspace.ev_supply_chain.forecast_run ALTER COLUMN model_version COMMENT 'Unity Catalog model version, null for the baseline.';
ALTER TABLE workspace.ev_supply_chain.forecast_run ALTER COLUMN mlflow_run_id COMMENT 'MLflow run with params, metrics and feature importance.';
ALTER TABLE workspace.ev_supply_chain.forecast_run ALTER COLUMN history_end_week COMMENT 'Last week of actuals the model was trained on.';
ALTER TABLE workspace.ev_supply_chain.forecast_run ALTER COLUMN origin_week_start COMMENT 'First forecast origin week in the run.';
ALTER TABLE workspace.ev_supply_chain.forecast_run ALTER COLUMN origin_week_end COMMENT 'Last forecast origin week in the run.';
ALTER TABLE workspace.ev_supply_chain.forecast_run ALTER COLUMN horizon_weeks COMMENT 'How many weeks ahead each origin forecasts.';
ALTER TABLE workspace.ev_supply_chain.forecast_run ALTER COLUMN wape COMMENT 'Backtest weighted absolute percent error as a fraction (0.1 = 10 percent). Null for production runs.';
ALTER TABLE workspace.ev_supply_chain.forecast_run ALTER COLUMN bias COMMENT 'Backtest bias as a fraction of actuals. Positive means over-forecasting.';
ALTER TABLE workspace.ev_supply_chain.forecast_run ALTER COLUMN created_at COMMENT 'When the run was written (UTC).';

COMMENT ON TABLE workspace.ev_supply_chain.fct_forecast IS 'Every forecast ever made, one row per run, series and target week. Append-only - never updated, so past forecasts can be compared with what actually happened. A series is a plant + vehicle model (vehicle_builds) or a site + part (service_parts).';
ALTER TABLE workspace.ev_supply_chain.fct_forecast ALTER COLUMN run_id COMMENT 'Forecast run. Join to forecast_run.';
ALTER TABLE workspace.ev_supply_chain.fct_forecast ALTER COLUMN forecast_type COMMENT 'vehicle_builds or service_parts.';
ALTER TABLE workspace.ev_supply_chain.fct_forecast ALTER COLUMN model_name COMMENT 'Model that produced the forecast.';
ALTER TABLE workspace.ev_supply_chain.fct_forecast ALTER COLUMN origin_week COMMENT 'The week the forecast was made from (last week of known actuals).';
ALTER TABLE workspace.ev_supply_chain.fct_forecast ALTER COLUMN target_week COMMENT 'The week being forecast (Monday start).';
ALTER TABLE workspace.ev_supply_chain.fct_forecast ALTER COLUMN horizon_weeks COMMENT 'Weeks between origin and target, 1-13.';
ALTER TABLE workspace.ev_supply_chain.fct_forecast ALTER COLUMN site_id COMMENT 'PLT1 (Plant 1 Midwest), PLT2 (Plant 2 Southeast) or SDC1 (Service Parts DC).';
ALTER TABLE workspace.ev_supply_chain.fct_forecast ALTER COLUMN model_id COMMENT 'Vehicle model for vehicle_builds forecasts: PU (Pickup), SUV, VAN (Delivery Van). Null for service parts.';
ALTER TABLE workspace.ev_supply_chain.fct_forecast ALTER COLUMN part_id COMMENT 'Part for service_parts forecasts. Null for vehicle builds.';
ALTER TABLE workspace.ev_supply_chain.fct_forecast ALTER COLUMN forecast_p10 COMMENT 'Low end: actuals should come in below this about 10 percent of the time.';
ALTER TABLE workspace.ev_supply_chain.fct_forecast ALTER COLUMN forecast_p50 COMMENT 'Point forecast in units. Use this one unless you need a range.';
ALTER TABLE workspace.ev_supply_chain.fct_forecast ALTER COLUMN forecast_p90 COMMENT 'High end: actuals should come in above this about 10 percent of the time.';

COMMENT ON TABLE workspace.ev_supply_chain.gold_forecast_accuracy IS 'Backtest forecasts lined up against actuals, one row per run, series and target week, for the latest backtest of each model including the naive baseline. Compute WAPE as sum(abs_error) / sum(actual_units) after grouping - never average row-level errors. Compare model_name values to see how much the ML model beats the baseline.';
ALTER TABLE workspace.ev_supply_chain.gold_forecast_accuracy ALTER COLUMN run_id COMMENT 'Backtest run.';
ALTER TABLE workspace.ev_supply_chain.gold_forecast_accuracy ALTER COLUMN forecast_type COMMENT 'vehicle_builds or service_parts.';
ALTER TABLE workspace.ev_supply_chain.gold_forecast_accuracy ALTER COLUMN model_name COMMENT 'naive_mean (baseline) or the LightGBM model name.';
ALTER TABLE workspace.ev_supply_chain.gold_forecast_accuracy ALTER COLUMN origin_week COMMENT 'Week the forecast was made from.';
ALTER TABLE workspace.ev_supply_chain.gold_forecast_accuracy ALTER COLUMN target_week COMMENT 'Week being forecast.';
ALTER TABLE workspace.ev_supply_chain.gold_forecast_accuracy ALTER COLUMN horizon_weeks COMMENT 'Weeks ahead, 1-13. Error normally grows with horizon.';
ALTER TABLE workspace.ev_supply_chain.gold_forecast_accuracy ALTER COLUMN site_id COMMENT 'Site id.';
ALTER TABLE workspace.ev_supply_chain.gold_forecast_accuracy ALTER COLUMN model_id COMMENT 'Vehicle model, null for service parts.';
ALTER TABLE workspace.ev_supply_chain.gold_forecast_accuracy ALTER COLUMN part_id COMMENT 'Part, null for vehicle builds.';
ALTER TABLE workspace.ev_supply_chain.gold_forecast_accuracy ALTER COLUMN component_category COMMENT 'Part category for service parts, Vehicle for build forecasts.';
ALTER TABLE workspace.ev_supply_chain.gold_forecast_accuracy ALTER COLUMN forecast_p10 COMMENT 'Low end of the forecast range. Null for the baseline.';
ALTER TABLE workspace.ev_supply_chain.gold_forecast_accuracy ALTER COLUMN forecast_p50 COMMENT 'Point forecast in units.';
ALTER TABLE workspace.ev_supply_chain.gold_forecast_accuracy ALTER COLUMN forecast_p90 COMMENT 'High end of the forecast range. Null for the baseline.';
ALTER TABLE workspace.ev_supply_chain.gold_forecast_accuracy ALTER COLUMN actual_units COMMENT 'What actually happened that week.';
ALTER TABLE workspace.ev_supply_chain.gold_forecast_accuracy ALTER COLUMN abs_error COMMENT 'abs(forecast_p50 - actual_units).';
ALTER TABLE workspace.ev_supply_chain.gold_forecast_accuracy ALTER COLUMN error COMMENT 'forecast_p50 - actual_units. Positive is over-forecast.';
ALTER TABLE workspace.ev_supply_chain.gold_forecast_accuracy ALTER COLUMN within_interval COMMENT 'True if the actual landed between p10 and p90. Should be true about 80 percent of the time if the range is well calibrated.';

-- demand plan
COMMENT ON TABLE workspace.ev_supply_chain.gold_part_demand_plan IS 'Forward 13-week part demand per site, from the latest production forecasts. Plant demand is dependent (forecast vehicle builds times BOM quantity). Service DC demand is independent (forecast directly). This is the input to buy signals.';
ALTER TABLE workspace.ev_supply_chain.gold_part_demand_plan ALTER COLUMN run_id COMMENT 'Production forecast run the plan came from.';
ALTER TABLE workspace.ev_supply_chain.gold_part_demand_plan ALTER COLUMN site_id COMMENT 'PLT1, PLT2 or SDC1.';
ALTER TABLE workspace.ev_supply_chain.gold_part_demand_plan ALTER COLUMN part_id COMMENT 'Part key.';
ALTER TABLE workspace.ev_supply_chain.gold_part_demand_plan ALTER COLUMN target_week COMMENT 'Week of demand (Monday start).';
ALTER TABLE workspace.ev_supply_chain.gold_part_demand_plan ALTER COLUMN horizon_weeks COMMENT 'Weeks from now, 1-13.';
ALTER TABLE workspace.ev_supply_chain.gold_part_demand_plan ALTER COLUMN demand_type COMMENT 'dependent (driven by vehicle builds through the BOM) or independent (service parts).';
ALTER TABLE workspace.ev_supply_chain.gold_part_demand_plan ALTER COLUMN demand_p10 COMMENT 'Low-end weekly demand in units.';
ALTER TABLE workspace.ev_supply_chain.gold_part_demand_plan ALTER COLUMN demand_p50 COMMENT 'Expected weekly demand in units.';
ALTER TABLE workspace.ev_supply_chain.gold_part_demand_plan ALTER COLUMN demand_p90 COMMENT 'High-end weekly demand in units.';

-- buy signals
COMMENT ON TABLE workspace.ev_supply_chain.gold_buy_signals IS 'Replenishment recommendations as of the inventory snapshot, one row per site and part. Combines the forecast daily run rate, supplier lead time and its variability, and buffer class into safety stock and a reorder point, then compares with on-hand and on-order stock. signal says what to do: EXPEDITE, ORDER, OK, EXCESS or NO DEMAND.';
ALTER TABLE workspace.ev_supply_chain.gold_buy_signals ALTER COLUMN snapshot_date COMMENT 'Date of the inventory snapshot the signals are based on.';
ALTER TABLE workspace.ev_supply_chain.gold_buy_signals ALTER COLUMN site_id COMMENT 'PLT1 (Plant 1 Midwest), PLT2 (Plant 2 Southeast) or SDC1 (Service Parts DC).';
ALTER TABLE workspace.ev_supply_chain.gold_buy_signals ALTER COLUMN part_id COMMENT 'Part key.';
ALTER TABLE workspace.ev_supply_chain.gold_buy_signals ALTER COLUMN part_number COMMENT 'Part number, prefixed by category: BAT, ELE, WIR, BDY, FST.';
ALTER TABLE workspace.ev_supply_chain.gold_buy_signals ALTER COLUMN part_name COMMENT 'Part description.';
ALTER TABLE workspace.ev_supply_chain.gold_buy_signals ALTER COLUMN component_category COMMENT 'Component category.';
ALTER TABLE workspace.ev_supply_chain.gold_buy_signals ALTER COLUMN demand_type COMMENT 'dependent (plant, from builds x BOM) or independent (service parts).';
ALTER TABLE workspace.ev_supply_chain.gold_buy_signals ALTER COLUMN buffer_class COMMENT 'Replenishment strategy. JIT: nearby body and wiring suppliers, daily deliveries, half a day of buffer. KANBAN: fasteners, min/max bins. STANDARD: 95 percent service level safety stock. STRATEGIC: battery materials and electronics, 99 percent service level over the full supplier lead time.';
ALTER TABLE workspace.ev_supply_chain.gold_buy_signals ALTER COLUMN supplier_id COMMENT 'Main supplier the part is ordered from.';
ALTER TABLE workspace.ev_supply_chain.gold_buy_signals ALTER COLUMN supplier_name COMMENT 'Main supplier name.';
ALTER TABLE workspace.ev_supply_chain.gold_buy_signals ALTER COLUMN supplier_region COMMENT 'Region the main supplier ships from.';
ALTER TABLE workspace.ev_supply_chain.gold_buy_signals ALTER COLUMN supplier_tier COMMENT 'Reliability tier of the main supplier (A/B/C).';
ALTER TABLE workspace.ev_supply_chain.gold_buy_signals ALTER COLUMN daily_run_rate COMMENT 'DRR: forecast average daily usage in units over the next 13 weeks.';
ALTER TABLE workspace.ev_supply_chain.gold_buy_signals ALTER COLUMN demand_sd_daily COMMENT 'Standard deviation of daily demand, from the forecast range.';
ALTER TABLE workspace.ev_supply_chain.gold_buy_signals ALTER COLUMN planning_lead_time_days COMMENT 'Lead time used for planning, in days. Transit time for JIT and KANBAN parts, full supplier lead time otherwise.';
ALTER TABLE workspace.ev_supply_chain.gold_buy_signals ALTER COLUMN lead_time_stddev_days COMMENT 'Variability of the supplier lead time in days.';
ALTER TABLE workspace.ev_supply_chain.gold_buy_signals ALTER COLUMN review_period_days COMMENT 'How often the part is reviewed for ordering, in days.';
ALTER TABLE workspace.ev_supply_chain.gold_buy_signals ALTER COLUMN safety_stock COMMENT 'Buffer units held against demand and lead time surprises. z x sqrt(LT x demand_sd^2 + (DRR x LT_sd)^2) for STANDARD and STRATEGIC, a fixed number of days of DRR for JIT and KANBAN.';
ALTER TABLE workspace.ev_supply_chain.gold_buy_signals ALTER COLUMN safety_stock_days COMMENT 'Safety stock expressed as days of run rate.';
ALTER TABLE workspace.ev_supply_chain.gold_buy_signals ALTER COLUMN reorder_point COMMENT 'Order when inventory position falls below this. DRR x (lead time + review period) + safety stock.';
ALTER TABLE workspace.ev_supply_chain.gold_buy_signals ALTER COLUMN order_up_to COMMENT 'Target inventory position after ordering (the max level).';
ALTER TABLE workspace.ev_supply_chain.gold_buy_signals ALTER COLUMN on_hand_qty COMMENT 'Units physically on hand at the site.';
ALTER TABLE workspace.ev_supply_chain.gold_buy_signals ALTER COLUMN on_order_qty COMMENT 'Units on open purchase orders, not yet received.';
ALTER TABLE workspace.ev_supply_chain.gold_buy_signals ALTER COLUMN inventory_position COMMENT 'on_hand_qty + on_order_qty.';
ALTER TABLE workspace.ev_supply_chain.gold_buy_signals ALTER COLUMN days_of_cover COMMENT 'How many days on-hand stock lasts at the run rate.';
ALTER TABLE workspace.ev_supply_chain.gold_buy_signals ALTER COLUMN stockout_before_next_receipt COMMENT 'True if on-hand runs out before a new order placed today could arrive.';
ALTER TABLE workspace.ev_supply_chain.gold_buy_signals ALTER COLUMN projected_stockout_date COMMENT 'Date on-hand stock runs out at the run rate, ignoring open orders.';
ALTER TABLE workspace.ev_supply_chain.gold_buy_signals ALTER COLUMN signal COMMENT 'EXPEDITE: on-hand is below safety stock. ORDER: inventory position is below the reorder point. EXCESS: more than twice the max level. OK: nothing to do. NO DEMAND: no forecast usage.';
ALTER TABLE workspace.ev_supply_chain.gold_buy_signals ALTER COLUMN suggested_order_qty COMMENT 'Units to order now, rounded up to the pack size. 0 unless signal is ORDER or EXPEDITE.';
ALTER TABLE workspace.ev_supply_chain.gold_buy_signals ALTER COLUMN suggested_order_value COMMENT 'suggested_order_qty x unit_cost, in USD.';
ALTER TABLE workspace.ev_supply_chain.gold_buy_signals ALTER COLUMN expected_receipt_date COMMENT 'When an order placed today should arrive.';
ALTER TABLE workspace.ev_supply_chain.gold_buy_signals ALTER COLUMN unit_cost COMMENT 'Unit cost from the main supplier, USD.';
ALTER TABLE workspace.ev_supply_chain.gold_buy_signals ALTER COLUMN pack_size COMMENT 'Order multiple - orders are rounded up to this.';

-- master data from production and purchasing
COMMENT ON TABLE workspace.ev_supply_chain.dim_part_catalog IS 'One row per part used in the vehicles (about 1,900). Part number, name, category and standard cost. Standard costs are calibrated so a vehicle BOM comes to about $45k.';
ALTER TABLE workspace.ev_supply_chain.dim_part_catalog ALTER COLUMN part_id COMMENT 'Part key.';
ALTER TABLE workspace.ev_supply_chain.dim_part_catalog ALTER COLUMN part_number COMMENT 'Part number, prefixed by category: BAT, ELE, WIR, BDY, FST.';
ALTER TABLE workspace.ev_supply_chain.dim_part_catalog ALTER COLUMN part_name COMMENT 'Part description.';
ALTER TABLE workspace.ev_supply_chain.dim_part_catalog ALTER COLUMN component_category COMMENT 'Battery materials, Electronics, Wiring & harnesses, Body & chassis, or Fasteners & fittings.';
ALTER TABLE workspace.ev_supply_chain.dim_part_catalog ALTER COLUMN standard_cost COMMENT 'Standard unit cost in USD.';

COMMENT ON TABLE workspace.ev_supply_chain.dim_sourcing IS 'Which supplier provides each part. One primary row per part, plus a secondary row for the roughly 13 percent of parts that are dual-sourced. volume_share is the split of deliveries.';
ALTER TABLE workspace.ev_supply_chain.dim_sourcing ALTER COLUMN part_id COMMENT 'Part key.';
ALTER TABLE workspace.ev_supply_chain.dim_sourcing ALTER COLUMN supplier_id COMMENT 'Supplier key.';
ALTER TABLE workspace.ev_supply_chain.dim_sourcing ALTER COLUMN sourcing_role COMMENT 'primary or secondary.';
ALTER TABLE workspace.ev_supply_chain.dim_sourcing ALTER COLUMN volume_share COMMENT 'Share of the part volume this supplier gets, 0-1. 1.0 for single-sourced parts, 0.7 and 0.3 for dual-sourced.';
ALTER TABLE workspace.ev_supply_chain.dim_sourcing ALTER COLUMN unit_price COMMENT 'Price paid per unit in USD. Second sources cost about 8 percent more.';

COMMENT ON TABLE workspace.ev_supply_chain.fct_po_lines_scenario IS 'Purchase order delivery lines, one row per delivery. Generated from the build schedule: weekly requirement = builds x BOM quantity, delivered daily by local body and wiring suppliers and weekly by everyone else. This is the fact table behind the supplier scorecard.';
ALTER TABLE workspace.ev_supply_chain.fct_po_lines_scenario ALTER COLUMN po_line_id COMMENT 'Delivery line key.';
ALTER TABLE workspace.ev_supply_chain.fct_po_lines_scenario ALTER COLUMN site_id COMMENT 'Plant receiving the delivery: PLT1 or PLT2.';
ALTER TABLE workspace.ev_supply_chain.fct_po_lines_scenario ALTER COLUMN part_id COMMENT 'Part key.';
ALTER TABLE workspace.ev_supply_chain.fct_po_lines_scenario ALTER COLUMN supplier_id COMMENT 'Supplier key.';
ALTER TABLE workspace.ev_supply_chain.fct_po_lines_scenario ALTER COLUMN sourcing_role COMMENT 'primary or secondary source for the part.';
ALTER TABLE workspace.ev_supply_chain.fct_po_lines_scenario ALTER COLUMN quantity COMMENT 'Units delivered.';
ALTER TABLE workspace.ev_supply_chain.fct_po_lines_scenario ALTER COLUMN net_amount COMMENT 'quantity x unit price, USD.';
ALTER TABLE workspace.ev_supply_chain.fct_po_lines_scenario ALTER COLUMN order_date COMMENT 'When the order was placed (promised date minus quoted lead time).';
ALTER TABLE workspace.ev_supply_chain.fct_po_lines_scenario ALTER COLUMN ship_date COMMENT 'When the goods left the supplier.';
ALTER TABLE workspace.ev_supply_chain.fct_po_lines_scenario ALTER COLUMN promised_date COMMENT 'Date the supplier committed to deliver.';
ALTER TABLE workspace.ev_supply_chain.fct_po_lines_scenario ALTER COLUMN received_date COMMENT 'Date the goods actually arrived.';
ALTER TABLE workspace.ev_supply_chain.fct_po_lines_scenario ALTER COLUMN ship_mode COMMENT 'Truck or Rail for north american suppliers, Ship for everyone else.';
ALTER TABLE workspace.ev_supply_chain.fct_po_lines_scenario ALTER COLUMN lead_time_days COMMENT 'Days from order to receipt.';
ALTER TABLE workspace.ev_supply_chain.fct_po_lines_scenario ALTER COLUMN days_late COMMENT 'Received minus promised, in days. Negative or zero is on time.';
ALTER TABLE workspace.ev_supply_chain.fct_po_lines_scenario ALTER COLUMN is_on_time COMMENT 'True when received on or before the promised date.';
