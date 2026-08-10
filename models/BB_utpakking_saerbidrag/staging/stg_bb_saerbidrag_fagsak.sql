with bb_meta_data as (
    select * from {{ref ('stg_bb_saerbidrag_meta_data')}}
),
 
pre_final as (
    select * from bb_meta_data
        ,json_table(melding, '$'
            columns (
                vedtaks_id  VARCHAR2(255 CHAR) PATH '$.vedtaksid'
                ,vedtaks_tidspunkt TIMESTAMP(3) PATH '$.vedtakstidspunkt'
                ,type varchar2(50 char) PATH '$.type'
                ,kategori varchar2(255 char) PATH '$.kategori'
                ,saksnr varchar2(255 char) PATH '$.saksnr'
                ,skyldner varchar2(255 char) PATH '$.skyldner'
                ,kravhaver varchar2(255 char) PATH '$.kravhaver'
                ,mottaker varchar2(255 char) PATH '$.mottaker'
                ,belop number PATH '$.beløp'
                ,valuta_kode varchar2(5 char) PATH '$.valutakode'
                ,resultat varchar2(255 char) PATH '$.resultat'
                ,innkreving_flagg varchar2(255 char) PATH '$.innkreving'
                ,omgjor_vedtaks_id varchar2(255 char) PATH '$.omgjørVedtakId'
                ,historisk_vedtak varchar2(255 char) PATH '$.historiskVedtak'
                ,krav_belop number PATH '$.kravbeløp'
                ,godkjent_belop number PATH '$.godkjentBeløp'
                ,betalt_belop number PATH '$.betaltBeløp'
               )
        ) j
),


final as (
    select kafka_offset
        ,pk_bb_meta_data as fk_bb_meta_data
        ,vedtaks_id
        ,vedtaks_tidspunkt
        ,type
        ,kategori
        ,saksnr
        ,skyldner
        ,kravhaver
        ,mottaker
        ,belop
        ,valuta_kode
        ,resultat
        ,innkreving_flagg
        ,omgjor_vedtaks_id
        ,historisk_vedtak
        ,krav_belop
        ,godkjent_belop
        ,betalt_belop
    from pre_final
)

select * from final
 