-- grain: one recall campaign. units affected is a campaign-level number that nhtsa repeats on every vehicle line,
-- so it lives here - summing it from the line table would count each recall several times over.
with lines as (
    select * from {{ ref('int_recall_vehicle_lines') }}
),

component_mode as (
    select campaign_id, max_by(component_category, n) as primary_component_category
    from (select campaign_id, component_category, count(*) as n from lines group by all)
    group by campaign_id
)

select
    {{ dbt_utils.generate_surrogate_key(['l.campaign_id']) }}                       as recall_campaign_key,
    {{ date_key('min(l.report_date)') }}                                            as report_date_key,
    l.campaign_id,
    any_value(l.manufacturer)                                                       as manufacturer,
    min(l.report_date)                                                              as report_date,
    max(l.units_affected)                                                           as units_affected,
    any_value(l.initiated_by)                                                       as initiated_by,
    any_value(cm.primary_component_category)                                        as primary_component_category,
    bool_or(l.is_ev)                                                                as involves_ev,
    -- units affected only means "ev units" when every vehicle in the campaign is an ev. mixed campaigns (an airbag
    -- recall covering one ev and millions of petrol cars) report units for the whole campaign
    bool_and(l.is_ev)                                                               as is_ev_only,
    -- the flat file has no ota flag (the api does), so read it from the remedy text
    bool_or(lower(l.corrective_action) rlike 'over[- ]the[- ]air|\\bota\\b')         as is_ota_remedy,
    bool_or(l.do_not_drive)                                                         as do_not_drive,
    bool_or(l.park_outside)                                                         as park_outside,
    count(*)                                                                        as vehicle_lines,
    count(distinct concat(l.make, l.model))                                         as models_affected,
    any_value(l.defect_summary)                                                     as defect_summary
from lines l
join component_mode cm using (campaign_id)
group by l.campaign_id
