with bb_meta_data as (
    select * from {{ref ('stg_bb_saerbidrag_meta_data')}}
),
 
pre_final as (
    select * from bb_meta_data
        ,json_table(melding, '$'
            columns (
                vedtaksid  VARCHAR2(255 CHAR) PATH '$.vedtaksid'
                ,vedtakstidspunkt TIMESTAMP(3) PATH '$.vedtakstidspunkt'
                ,type varchar2(50 char) PATH '$.type'
                ,kategori varchar2(255 char) PATH '$.kategori'
                ,saksnr varchar2(255 char) PATH '$.saksnr'
                ,skyldner varchar2(255 char) PATH '$.skyldner'
                ,kravhaver varchar2(255 char) PATH '$.kravhaver'
                ,mottaker varchar2(255 char) PATH '$.mottaker'
                ,belop number PATH '$.beløp'
                ,valutakode varchar2(5 char) PATH '$.valutakode'
                ,resultat varchar2(255 char) PATH '$.resultat'
                ,innkreving_flagg varchar2(1 char) PATH '$.innkreving'
                ,omgjor_vedtak_id varchar2(255 char) PATH '$.omgjørVedtakId'
                ,historisk_vedtak varchar2(255 char) PATH '$.historiskVedtak'
                ,kravbelop number PATH '$.kravbeløp'
                ,godkjent_belop number PATH '$.godkjentBeløp'
                ,betalt_belop number PATH '$.betaltBeløp'
               )
        ) j
),


final as (
    select kafka_offset
    ,pk_bb_meta_data as fk_bb_meta_data
    ,vedtaksid
    ,vedtakstidspunkt
    ,type
    ,kategori
    ,saksnr
    ,skyldner
    ,kravhaver
    ,mottaker
    ,belop
    ,valutakode
    ,resultat
    ,innkreving_flagg
    ,omgjor_vedtak_id
    ,historisk_vedtak
    ,kravbelop
    ,godkjent_belop
    ,betalt_belop
    ,localtimestamp as lastet_dato 
    from pre_final
)

select * from final
 