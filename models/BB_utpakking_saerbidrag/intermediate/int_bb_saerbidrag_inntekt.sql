with sb_inn as (
    select * from {{ref ('stg_bb_saerbidrag_inntekt')}}
),


final as (
    select kafka_offset
        ,STANDARD_HASH(vedtaks_id || saksnr || kravhaver, 'MD5') as fk_bb_saerbidrag_fagsak
        ,row_number() over (partition by vedtaks_id, inntekt_for, inntekt_kategori order by kafka_offset) as type_inntekt_nr
        ,saksnr
        ,vedtaks_id
        ,vedtakstidspunkt
        ,case when historisk_vedtak = 'true' then 1 else 0 end as historisk_flagg
        ,inntekt_kategori
        ,inntekt_type
        ,inntekt_for
        ,inntekt_belop
    from sb_inn
)





select standard_hash(vedtaks_id || '|' || inntekt_for || '|' || inntekt_kategori || '|' || type_inntekt_nr,'MD5') as pk_bb_inntekt_saerbidrag
    ,final.*

 from final