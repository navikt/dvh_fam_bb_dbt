with sb_inn as (
    select * from {{ref ('stg_bb_saerbidrag_inntekt')}}
),


final as (
select kafka_offset as key_saerbidrag_fagsak
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