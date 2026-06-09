with bb_meta_data as (
    select * from {{ref ('stg_bb_saerbidrag_meta_data')}}
),
 
bp as (
    select * from (
    select *
    from bb_meta_data
        ,json_table(melding, '$'
            columns (
                vedtaksid  VARCHAR2(255 CHAR) PATH '$.vedtaksid'
                ,saksnr varchar2(255 char) PATH '$.saksnr'
                ,fk_person1_skyldner varchar2(255 char) PATH '$.skyldner'
                ,fk_person1_kravhaver varchar2(255 char) PATH '$.kravhaver'
                ,fk_person1_mottaker varchar2(255 char) PATH '$.mottaker',
                    nested PATH '$.bpinntektListe[*]'
                        columns(
                            type_inntekt VARCHAR2(255)   PATH '$.type',
                            belop    NUMBER(16,2)    PATH '$.beløp'
                    )
               )
        ) j
    ) where belop is not null
),

bm as (
    select * from (
    select *
    from bb_meta_data
        ,json_table(melding, '$'
            columns (
                vedtaksid  VARCHAR2(255 CHAR) PATH '$.vedtaksid'
                ,saksnr varchar2(255 char) PATH '$.saksnr'
                ,fk_person1_skyldner varchar2(255 char) PATH '$.skyldner'
                ,fk_person1_kravhaver varchar2(255 char) PATH '$.kravhaver'
                ,fk_person1_mottaker varchar2(255 char) PATH '$.mottaker'
                
                ,nested PATH '$.bpinntektListe[*]'
                    columns(
                        type_inntekt VARCHAR2(255)   PATH '$.type',
                        belop    NUMBER(16,2)    PATH '$.beløp'
                    )
               )
        ) j
    ) where belop is not null
),

final as (
select kafka_offset
,vedtaksid
,saksnr
,fk_person1_skyldner
,fk_person1_kravhaver
,fk_person1_mottaker
,type_inntekt
,belop
,'p' as flagg
,localtimestamp as lastet_dato 
from bp
 
union all

 select kafka_offset
,vedtaksid
,saksnr
,fk_person1_skyldner
,fk_person1_kravhaver
,fk_person1_mottaker
,type_inntekt
,belop
,'m' as flagg
,localtimestamp as lastet_dato 
from bm
)

select * from final