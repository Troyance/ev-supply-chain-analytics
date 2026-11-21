-- one row per part. how dependent we are on one supplier, and how reliable that supplier is.
-- most auto parts are single-sourced on purpose, so the useful flag is single-sourced *and* tier C.
with part_supplier_spend as (
    select part_key, supplier_key, sum(net_amount) as spend
    from {{ ref('fct_po_deliveries') }}
    group by all
),

by_part as (
    select
        part_key,
        sum(spend)                        as total_spend,
        max(spend)                        as top_supplier_spend,
        max_by(supplier_key, spend)       as top_supplier_key
    from part_supplier_spend
    group by part_key
),

sources as (
    select part_key, count(*) as approved_sources
    from {{ ref('bridge_part_supplier') }}
    group by part_key
)

select
    b.part_key,
    p.part_id,
    p.part_number,
    p.part_name,
    p.component_category,
    b.top_supplier_key,
    s.supplier_name                                                        as top_supplier_name,
    s.country                                                              as top_supplier_country,
    s.region                                                               as top_supplier_region,
    s.current_tier                                                         as top_supplier_tier,
    cast(src.approved_sources as int)                                      as approved_sources,
    src.approved_sources = 1                                               as is_single_sourced,
    cast(b.total_spend as decimal(18, 2))                                  as total_spend,
    cast(b.top_supplier_spend / b.total_spend as double)                   as top_supplier_share,
    src.approved_sources = 1 and s.current_tier = 'C'                      as is_high_risk
from by_part b
join {{ ref('dim_part') }} p using (part_key)
join {{ ref('dim_supplier') }} s on s.supplier_key = b.top_supplier_key
join sources src using (part_key)
