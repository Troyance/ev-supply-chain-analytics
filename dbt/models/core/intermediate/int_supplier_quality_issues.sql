-- issues caused by supplier quality, attributed to the part's primary supplier
select
    b.supplier_key,
    i.issue_key,
    i.severity,
    i.found_phase,
    i.found_date
from {{ ref('fct_issue') }} i
join {{ ref('bridge_part_supplier') }} b
    on b.part_key = i.part_key and b.sourcing_role = 'primary'
where i.root_cause = 'Supplier quality'
