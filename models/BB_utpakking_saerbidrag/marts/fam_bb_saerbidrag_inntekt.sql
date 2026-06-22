{{
    config(
        materialized='incremental'
    )
}}

with sb_inn as (
    select * from {{ref ('int_bb_saerbidrag_inntekt')}}
),


final as (
select pk_bb_inntekt_saerbidrag
,fk_bb_saerbidrag_fagsak
,kafka_offset
,vedtaksid
,saksnr
--,skyldner
--,kravhaver
--,mottaker
,type_inntekt
,belop
,inntekt_flagg
,localtimestamp as lastet_dato 
from sb_inn
)

select * from final