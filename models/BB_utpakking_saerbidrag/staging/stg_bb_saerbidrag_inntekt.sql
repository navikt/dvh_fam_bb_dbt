with bb_meta_data as (
    select * from {{ref ('stg_bb_saerbidrag_meta_data')}}
),
 
bp as (
    select * 
    from bb_meta_data
        ,json_table(melding, '$'
            columns (
                vedtaks_id  VARCHAR2(255 CHAR) PATH '$.vedtaksid'
                ,vedtaks_tidspunkt TIMESTAMP(3) PATH '$.vedtakstidspunkt'
                ,saksnr varchar2(255 char) PATH '$.saksnr'
                ,skyldner varchar2(255 char) PATH '$.skyldner'
                ,kravhaver varchar2(255 char) PATH '$.kravhaver'
                ,mottaker varchar2(255 char) PATH '$.mottaker'
                ,historisk_vedtak varchar2(255 char) PATH '$.historiskVedtak'

                ,nested PATH '$.bpinntektListe[*]'
                    columns(
                        type_inntekt VARCHAR2(255)   PATH '$.type'
                        ,belop    NUMBER(16,2)    PATH '$.beløp'
                    )
            )
        ) j
    where belop is not null
),

bm as (
    select * 
    from bb_meta_data
        ,json_table(melding, '$'
            columns (
                vedtaks_id  VARCHAR2(255 CHAR) PATH '$.vedtaksid'
                ,vedtaks_tidspunkt TIMESTAMP(3) PATH '$.vedtakstidspunkt'
                ,saksnr varchar2(255 char) PATH '$.saksnr'
                ,skyldner varchar2(255 char) PATH '$.skyldner'
                ,kravhaver varchar2(255 char) PATH '$.kravhaver'
                ,mottaker varchar2(255 char) PATH '$.mottaker'
                ,historisk_vedtak varchar2(255 char) PATH '$.historiskVedtak'

                ,nested PATH '$.bminntektListe[*]'
                    columns(
                        type_inntekt VARCHAR2(255)   PATH '$.type'
                        ,belop    NUMBER(16,2)    PATH '$.beløp'
                    )
            )
        ) j
    where belop is not null
),

final as (
select kafka_offset
,vedtaks_id
,saksnr
,vedtaks_tidspunkt
--,skyldner
,kravhaver
--,mottaker
,type_inntekt
,belop
,'p' as inntekt_for
,historisk_vedtak
--,localtimestamp as lastet_dato 
from bp
 
union all

 select kafka_offset
,vedtaks_id
,saksnr
,vedtaks_tidspunkt
--,skyldner
,kravhaver
--,mottaker
,type_inntekt
,belop
,'m' as inntekt_for
,historisk_vedtak
--,localtimestamp as lastet_dato 
from bm
)

select * from final