with bb_meta_data as (
    select * from {{ref ('stg_bb_saerbidrag_meta_data')}}
),
 
bp as (
    select * 
    from bb_meta_data
        ,json_table(melding, '$'
            columns (
                saksnr varchar2(255 char) PATH '$.saksnr'
                ,vedtaks_id  VARCHAR2(255 CHAR) PATH '$.vedtaksid'
                ,vedtakstidspunkt TIMESTAMP(3) PATH '$.vedtakstidspunkt'
                ,skyldner varchar2(255 char) PATH '$.skyldner'
                ,kravhaver varchar2(255 char) PATH '$.kravhaver'
                ,mottaker varchar2(255 char) PATH '$.mottaker'
                ,historisk_vedtak varchar2(255 char) PATH '$.historiskVedtak'

                ,nested PATH '$.bpinntektListe[*]'
                    columns(
                        inntekt_kategori VARCHAR2(255)   PATH '$.type'
                        ,gjelder_kravhaver   VARCHAR2(255)   PATH '$.gjelderKravhaver'
                        ,inntekt_type VARCHAR2(255)   PATH '$.inntektstype'
                        ,inntekt_belop    NUMBER(16,2)    PATH '$.beløp'

                    )
            )
        ) j
    where inntekt_belop is not null
),

bm as (
    select * 
    from bb_meta_data
        ,json_table(melding, '$'
            columns (
                saksnr varchar2(255 char) PATH '$.saksnr'
                ,vedtaks_id  VARCHAR2(255 CHAR) PATH '$.vedtaksid'
                ,vedtakstidspunkt TIMESTAMP(3) PATH '$.vedtakstidspunkt'
                ,skyldner varchar2(255 char) PATH '$.skyldner'
                ,kravhaver varchar2(255 char) PATH '$.kravhaver'
                ,mottaker varchar2(255 char) PATH '$.mottaker'
                ,historisk_vedtak varchar2(255 char) PATH '$.historiskVedtak'

                ,nested PATH '$.bminntektListe[*]'
                    columns(
                        inntekt_kategori VARCHAR2(255)   PATH '$.type'
                        ,gjelder_kravhaver   VARCHAR2(255)   PATH '$.gjelderKravhaver'
                        ,inntekt_type VARCHAR2(255)   PATH '$.inntektstype'
                        ,inntekt_belop    NUMBER(16,2)    PATH '$.beløp'
                    )
            )
        ) j
    where inntekt_belop is not null
),

final as (
    select kafka_offset
        ,saksnr
        ,vedtaks_id
        ,vedtakstidspunkt
        ,kravhaver
        ,gjelder_kravhaver
        ,historisk_vedtak
        ,inntekt_kategori
        ,inntekt_type
        ,'p' as inntekt_for -- p for pliktig/skyldner
        ,inntekt_belop
    from bp
 
    union all

    select kafka_offset
        ,saksnr
        ,vedtaks_id
        ,vedtakstidspunkt
        ,kravhaver
        ,gjelder_kravhaver
        ,historisk_vedtak
        ,inntekt_kategori
        ,inntekt_type
        ,'m' as inntekt_for -- m for mottaker
        ,inntekt_belop
    from bm
)

select * from final