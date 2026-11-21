-- gold: business-facing tables built off the scenario version of the PO lines.
-- "recent" means the last 12 months of orders in the data, compared against the 12 before that.

-- one row per supplier: how reliable are they, how much do we spend, and are they getting worse?
CREATE OR REPLACE TABLE workspace.ev_supply_chain.gold_supplier_scorecard AS
WITH as_of AS (
  SELECT max(order_date) AS as_of_date FROM workspace.ev_supply_chain.fct_po_lines_scenario
),
lines AS (
  SELECT f.*,
         CASE WHEN f.order_date >  add_months(a.as_of_date, -12) THEN 'recent'
              WHEN f.order_date >  add_months(a.as_of_date, -24) THEN 'prior'
         END AS period,
         months_between(f.order_date, add_months(a.as_of_date, -24)) AS month_idx
  FROM workspace.ev_supply_chain.fct_po_lines_scenario f
  CROSS JOIN as_of a
),
stats AS (
  SELECT
    supplier_id,
    count(*)                                                        AS po_lines,
    sum(net_amount)                                                 AS spend,
    avg(is_on_time::int)                                            AS on_time_rate,
    avg(lead_time_days)                                             AS avg_lead_time_days,
    stddev(lead_time_days)                                          AS lead_time_stddev,
    percentile(lead_time_days, 0.9)                                 AS p90_lead_time_days,
    avg(CASE WHEN days_late > 0 THEN days_late END)                 AS avg_days_late_when_late,
    avg(CASE WHEN period = 'recent' THEN is_on_time::int END)       AS on_time_rate_recent,
    avg(CASE WHEN period = 'prior'  THEN is_on_time::int END)       AS on_time_rate_prior,
    -- days later per month over the last 24 months. uses every line in the window,
    -- so it's way less noisy than comparing two yearly on-time percentages
    regr_slope(CASE WHEN period IS NOT NULL THEN days_late END,
               CASE WHEN period IS NOT NULL THEN month_idx END)     AS lateness_trend
  FROM lines
  GROUP BY supplier_id
)
SELECT
  s.supplier_id,
  d.supplier_name,
  d.country,
  d.region,
  s.po_lines,
  round(s.spend, 2)                                     AS spend,
  round(s.on_time_rate * 100, 1)                        AS on_time_pct,
  round(s.on_time_rate_recent * 100, 1)                 AS on_time_pct_recent,
  round(s.on_time_rate_prior * 100, 1)                  AS on_time_pct_prior,
  round((s.on_time_rate_recent - s.on_time_rate_prior) * 100, 1) AS on_time_change_pts,
  round(s.avg_lead_time_days, 1)                        AS avg_lead_time_days,
  round(s.lead_time_stddev, 1)                          AS lead_time_stddev,
  s.p90_lead_time_days,
  round(s.avg_days_late_when_late, 1)                   AS avg_days_late_when_late,
  round(s.lateness_trend, 2)                            AS lateness_trend_days_per_month,
  CASE WHEN s.on_time_rate >= 0.97 THEN 'A'
       WHEN s.on_time_rate >= 0.90 THEN 'B'
       ELSE 'C' END                                     AS tier,
  s.lateness_trend >= 0.15                              AS is_declining
FROM stats s
JOIN workspace.ev_supply_chain.dim_supplier d USING (supplier_id);

-- parts where one supplier gets most of the money (most parts, in auto). ranked by how much spend rides on that one supplier.
CREATE OR REPLACE TABLE workspace.ev_supply_chain.gold_single_source_risk AS
WITH part_supplier_spend AS (
  SELECT part_id, supplier_id, sum(net_amount) AS spend
  FROM workspace.ev_supply_chain.fct_po_lines_scenario
  GROUP BY part_id, supplier_id
),
by_part AS (
  SELECT
    part_id,
    sum(spend)                      AS total_spend,
    max(spend)                      AS top_supplier_spend,
    max_by(supplier_id, spend)      AS top_supplier_id,
    count(*)                        AS suppliers_used
  FROM part_supplier_spend
  GROUP BY part_id
)
SELECT
  b.part_id,
  p.part_number,
  p.part_name,
  p.component_category,
  b.top_supplier_id,
  sc.supplier_name                                  AS top_supplier_name,
  sc.country                                        AS top_supplier_country,
  sc.on_time_pct                                    AS top_supplier_on_time_pct,
  sc.tier                                           AS top_supplier_tier,
  b.suppliers_used,
  round(b.total_spend, 2)                           AS total_spend,
  round(b.top_supplier_spend / b.total_spend * 100, 1) AS top_supplier_share_pct,
  -- a concentrated part is worse when the supplier you depend on is also unreliable
  sc.tier = 'C'                                     AS is_high_risk
FROM by_part b
JOIN workspace.ev_supply_chain.dim_part_catalog p USING (part_id)
JOIN workspace.ev_supply_chain.gold_supplier_scorecard sc ON sc.supplier_id = b.top_supplier_id
WHERE b.top_supplier_spend / b.total_spend >= 0.80;

-- where the money goes, geographically
CREATE OR REPLACE TABLE workspace.ev_supply_chain.gold_spend_by_geography AS
SELECT
  d.region,
  d.country,
  count(DISTINCT f.supplier_id)                              AS suppliers,
  round(sum(f.net_amount), 2)                                AS spend,
  round(sum(f.net_amount) / sum(sum(f.net_amount)) OVER () * 100, 2) AS spend_share_pct,
  round(avg(f.lead_time_days), 1)                            AS avg_lead_time_days,
  round(avg(f.is_on_time::int) * 100, 1)                     AS on_time_pct
FROM workspace.ev_supply_chain.fct_po_lines_scenario f
JOIN workspace.ev_supply_chain.dim_supplier d USING (supplier_id)
GROUP BY d.region, d.country;

-- monthly trend by component category - this is where the chip shortage should jump out
CREATE OR REPLACE TABLE workspace.ev_supply_chain.gold_monthly_performance AS
SELECT
  date_trunc('MONTH', f.order_date)::date        AS order_month,
  p.component_category,
  count(*)                                       AS po_lines,
  round(sum(f.net_amount), 2)                    AS spend,
  round(avg(f.is_on_time::int) * 100, 1)         AS on_time_pct,
  round(avg(f.lead_time_days), 1)                AS avg_lead_time_days
FROM workspace.ev_supply_chain.fct_po_lines_scenario f
JOIN workspace.ev_supply_chain.dim_part_catalog p USING (part_id)
GROUP BY 1, 2;
