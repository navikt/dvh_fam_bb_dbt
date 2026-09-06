{{
    config(
        materialized='incremental'
    )
}}

with forskudds_perioder as (
    select * from {{ref ('int_bb_inntekt')}}
)


select
    pk_bb_inntekt
   ,fk_bb_forskudds_periode
   ,type_inntekt
   ,inntekt
   ,kafka_offset
   ,localtimestamp as lastet_dato
from forskudds_perioder

{% if is_incremental() %}
    WHERE kafka_offset > COALESCE(( SELECT MAX(t.kafka_offset) FROM {{ this }} t ), 0)
{% endif %}