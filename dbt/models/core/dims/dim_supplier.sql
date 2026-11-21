select
    {{ dbt_utils.generate_surrogate_key(['s.supplier_id']) }} as supplier_key,
    s.supplier_id,
    t.supplier_name,
    n.country,
    r.region,
    s.quoted_lead_time_days,
    s.transit_days,
    coalesce(cur.tier, 'Unrated')                                  as current_tier
from {{ ref('stg_erp__suppliers') }} s
join {{ ref('stg_tpch__suppliers') }} t using (supplier_id)
join {{ ref('stg_tpch__nations') }} n using (nation_id)
join {{ ref('stg_tpch__regions') }} r using (region_id)
-- current tier comes from the latest month in the shared monthly model (not from dim_supplier_tier_history,
-- which points back here with a foreign key and would make a cycle)
left join (
    select supplier_id, max_by(tier, month_start) as tier
    from {{ ref('int_supplier_monthly_performance') }}
    group by supplier_id
) cur on cur.supplier_id = s.supplier_id
