-- the multi-level bom: one row per parent -> child line per version. effective-dated like an scd2,
-- so the bom on any date is the lines where that date falls between valid_from and valid_to
select
    {{ dbt_utils.generate_surrogate_key(['parent_item_id', 'child_item_id', 'effective_from']) }}   as bom_line_key,
    {{ dbt_utils.generate_surrogate_key(['parent_item_id']) }}                                      as parent_item_key,
    {{ dbt_utils.generate_surrogate_key(['child_item_id']) }}                                       as child_item_key,
    case when eco_id is not null then {{ dbt_utils.generate_surrogate_key(['eco_id']) }} end       as eco_key,
    parent_item_id,
    child_item_id,
    qty,
    effective_from                                                     as valid_from,
    coalesce(effective_to, date'9999-12-31')                           as valid_to,
    effective_to is null                                               as is_current
from {{ ref('stg_plm__bom_lines') }}
