with sb_inn as (
    select * from {{ref ('stg_bb_saerbidrag_inntekt')}}
),


final as (
select kafka_offset
,STANDARD_HASH(vedtaksid || saksnr || kravhaver, 'MD5') as fk_bb_saerbidrag_fagsak
,row_number() over (partition by vedtaksid, inntekt_for, type_inntekt order by kafka_offset) as type_inntekt_nr
,vedtaksid
,saksnr
,type_inntekt
,belop
,inntekt_for
,case when historisk_vedtak = 'true' then 1 else 0 end as historisk_flagg
--,localtimestamp as lastet_dato 
from sb_inn
)

select final.*
,standard_hash(vedtaksid || '|' || inntekt_for || '|' || type_inntekt || '|' || type_inntekt_nr,'MD5') as pk_bb_inntekt_saerbidrag
 from final