
{{ config (materialized='table') }}

select
    pk_bb_meta_data,
    kafka_offset,
    kafka_mottatt_dato,
    melding
from {{ source('fam_bb', 'fam_bb_meta_data') }}
where type_stonad = 'FORSKUDD'