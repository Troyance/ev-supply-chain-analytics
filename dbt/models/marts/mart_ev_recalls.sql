{{ config(databricks_tags={"layer": "gold", "domain": "vehicle_quality", "certified": "true", "model_type": "mart", "source": "nhtsa_public"}) }}
-- real nhtsa data: recall campaigns that involve battery-electric vehicles, one row per campaign x ev make
with ev_lines as (
    select distinct recall_campaign_key, make
    from {{ ref('fct_recall_vehicle_line') }}
    where is_ev
)

select
    c.recall_campaign_key,
    c.campaign_id,
    e.make,
    c.manufacturer,
    c.report_date,
    year(c.report_date)                                  as report_year,
    c.primary_component_category,
    c.units_affected,
    c.is_ev_only,
    c.is_ota_remedy,
    c.do_not_drive,
    c.initiated_by,
    c.models_affected,
    c.defect_summary
from {{ ref('fct_recall_campaign') }} c
join ev_lines e using (recall_campaign_key)
