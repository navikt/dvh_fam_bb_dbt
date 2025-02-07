{{
    config(
        materialized='table'
    )
}}

with bb_meta_data as (
select pk_bb_meta_data, kafka_offset, kafka_mottatt_dato, melding
from {{ source ('fam_bb', 'fam_bb_meta_data') }}
where kafka_offset < 250

)

select * from bb_meta_data