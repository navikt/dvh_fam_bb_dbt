with bb_meta_data as (
    select * from {{ref ('stg_bb_saerbidrag_meta_data')}}
),
 
pre_final as (
    select * from bb_meta_data
        ,json_table(melding, '$'
            columns (
                saksnr varchar2(255 char) PATH '$.saksnr'
                ,vedtaks_id  VARCHAR2(255 CHAR) PATH '$.vedtaksid'
                ,omgjor_vedtaks_id varchar2(255 char) PATH '$.omgjørVedtakId'
                ,vedtaks_ts TIMESTAMP(3) PATH '$.vedtakstidspunkt'
                ,behandlings_type varchar2(50 char) PATH '$.type'
                ,kategori varchar2(255 char) PATH '$.kategori'
                ,skyldner varchar2(255 char) PATH '$.skyldner'
                ,kravhaver varchar2(255 char) PATH '$.kravhaver'
                ,mottaker varchar2(255 char) PATH '$.mottaker'
                ,innkreving_flagg varchar2(255 char) PATH '$.innkreving'
                ,historisk_vedtak varchar2(255 char) PATH '$.historiskVedtak'
                ,resultat varchar2(255 char) PATH '$.resultat'
                ,valuta_kode varchar2(5 char) PATH '$.valutakode'
                ,belop number PATH '$.beløp'
                ,krav_belop number PATH '$.kravbeløp'
                ,godkjent_belop number PATH '$.godkjentBeløp'
                ,betalt_belop number PATH '$.betaltBeløp'
               )
        ) j
),


final as (
    select kafka_offset
        ,pk_bb_meta_data as fk_bb_meta_data
        ,saksnr
        ,vedtaks_id
        ,omgjor_vedtaks_id
        ,vedtaks_ts
        ,behandlings_type
        ,kategori
        ,skyldner
        ,kravhaver
        ,mottaker
        ,innkreving_flagg
        ,historisk_vedtak
        ,resultat
        ,valuta_kode
        ,belop
        ,krav_belop
        ,godkjent_belop
        ,betalt_belop
    from pre_final
)

select * from final
 