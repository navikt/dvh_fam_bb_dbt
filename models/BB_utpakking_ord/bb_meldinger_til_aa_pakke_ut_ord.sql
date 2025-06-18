{{
    config(
        materialized='table'
    )
}}

with bb_meta_data as (
  select pk_bb_meta_data, kafka_offset, kafka_mottatt_dato, melding from {{ source ('fam_bb', 'fam_bb_meta_data') }} 
    where kafka_mottatt_dato >= sysdate - 30 and kafka_offset not in (
      select kafka_offset from {{ source ('fam_bb', 'fam_bb_fagsak_ord') }})
      and stonadstype = 'BIDRAG'
)

select * from bb_meta_data