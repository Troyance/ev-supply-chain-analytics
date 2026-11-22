-- one row per nhtsa record (campaign x vehicle x component). each file is a full snapshot, so keep only the latest
-- copy of each record. dates come as yyyymmdd text, blanks are blank strings, model year 9999 means unknown.
with latest as (
    select *
    from {{ source('nhtsa', 'nhtsa_recalls') }}
    qualify row_number() over (partition by record_id order by _ingested_at desc, _source_file desc) = 1
)

select
    cast(record_id as bigint)                                           as record_id,
    trim(campno)                                                        as campaign_id,
    upper(trim(maketxt))                                                as make,
    upper(trim(modeltxt))                                               as model,
    cast(nullif(trim(yeartxt), '9999') as int)                         as model_year,
    trim(compname)                                                      as component_name,
    trim(mfgname)                                                       as manufacturer,
    trim(rcltypecd)                                                     as recall_type,
    try_cast(nullif(trim(potaff), '') as bigint)                       as units_affected,
    trim(influenced_by)                                                 as initiated_by,
    try_to_date(nullif(trim(rcdate), ''), 'yyyyMMdd')                  as report_date,
    try_to_date(nullif(trim(odate), ''), 'yyyyMMdd')                   as owner_notification_date,
    trim(desc_defect)                                                   as defect_summary,
    trim(conequence_defect)                                             as consequence_summary,
    trim(corrective_action)                                             as corrective_action,
    upper(trim(do_not_drive)) = 'YES'                                   as do_not_drive,
    upper(trim(park_outside)) = 'YES'                                   as park_outside,
    _ingested_at
from latest
