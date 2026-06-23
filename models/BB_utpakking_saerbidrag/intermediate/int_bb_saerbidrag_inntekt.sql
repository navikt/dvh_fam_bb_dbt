with sb_inn as (
    select * from {{ref ('stg_bb_saerbidrag_inntekt')}}
),


final as (
select kafka_offset
,STANDARD_HASH(t1.vedtaksid || t1.saksnr || kravhaver, 'MD5') as fk_bb_saerbidrag_fagsak
,row_number() over (partition by vedtaksid, inntekt_flagg, type_inntekt order by kafka_offset) as type_inntekt_nr
,vedtaksid
,saksnr
,nvl(t2.fk_person1, -1 ) as fk_person1_kravhaver
--,skyldner
--,kravhaver
--,mottaker
,type_inntekt
,belop
,inntekt_flagg
--,localtimestamp as lastet_dato 
from sb_inn t1

    left outer join {{ source('person', 'ident_off_id_til_fk_person1_ikke_skjermet') }} t2 on
    t1.kravhaver = t2.off_id
    and t2.gyldig_fra_dato <= t1.vedtakstidspunkt
    and t2.gyldig_til_dato >= t1.vedtakstidspunkt
)

select final.*
,standard_hash(vedtaksid || '|' || inntekt_flagg || '|' || type_inntekt || '|' || type_inntekt_nr,'MD5') as pk_bb_inntekt_saerbidrag
 from final