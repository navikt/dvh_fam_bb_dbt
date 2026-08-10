with forskudds_perioder as (
    select * from {{ref ('int_bb_inntekt')}}
)


select
    pk_bb_inntekt
   ,fk_bb_forskudds_periode
   ,type_inntekt
   ,belop
   ,kafka_offset
   ,localtimestamp as lastet_dato
from forskudds_perioder