-- recursive bom explosion. one anchor per "as of" date: the start of every prototype build, plus today.
-- each step walks one level down using only the lines valid on that date, multiplying quantities as it goes.
-- the result answers "what parts, and how many, were in the vehicle on this date"
with recursive anchors as (
    select b.build_id as as_of_label, b.actual_start_date as as_of_date, p.vehicle_model_id
    from {{ ref('stg_plm__prototype_builds') }} b
    join {{ ref('stg_plm__programs') }} p using (program_id)
    union all
    select 'current', current_date(), vehicle_model_id
    from {{ ref('stg_plm__programs') }}
    group by vehicle_model_id
),

vehicles as (
    select item_id, substring(item_number, 5) as vehicle_model_id
    from {{ ref('stg_plm__items') }}
    where item_type = 'vehicle'
),

lines as (
    select parent_item_id, child_item_id, qty, effective_from, coalesce(effective_to, date'9999-12-31') as effective_to
    from {{ ref('stg_plm__bom_lines') }}
),

explode (as_of_label, as_of_date, vehicle_model_id, item_id, bom_level, extended_qty, path) as (
    select a.as_of_label, a.as_of_date, a.vehicle_model_id, v.item_id, 0, cast(1 as bigint), cast(v.item_id as string)
    from anchors a
    join vehicles v using (vehicle_model_id)

    union all

    select e.as_of_label, e.as_of_date, e.vehicle_model_id, l.child_item_id, e.bom_level + 1,
           e.extended_qty * l.qty, concat(e.path, ' > ', cast(l.child_item_id as string))
    from explode e
    join lines l
        on l.parent_item_id = e.item_id
        and e.as_of_date between l.effective_from and l.effective_to
    where e.bom_level < 6
)

select * from explode
