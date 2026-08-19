
with sb as (
    select * from {{ref ('stg_bb_saerbidrag_fagsak')}}
),


final as (
    select kafka_offset
        ,fk_bb_meta_data
        ,STANDARD_HASH(t1.vedtaks_id || t1.saksnr || t1.kravhaver, 'MD5') as pk_bb_saerbidrag_fagsak
        ,saksnr
        ,referanse
        ,vedtaks_id
        ,omgjor_vedtaks_id
        ,vedtakstidspunkt
        ,behandlings_type
        ,kategori
        ,nvl(t2.fk_person1, -5 ) as fk_person1_skyldner
        ,nvl(t3.fk_person1, -5 ) as fk_person1_kravhaver
        ,nvl(t4.fk_person1, -5 ) as fk_person1_mottaker
        ,case when innkreving_flagg = 'true' then 1 else 0 end as innkreving_flagg
        ,case when historisk_vedtak = 'true' then 1 else 0 end as historisk_flagg
        ,resultat
        ,valuta_kode
        ,belop
        ,krav_belop
        ,godkjent_belop
        ,betalt_belop
    from sb t1
    left outer join {{ source('person', 'ident_off_id_til_fk_person1_ikke_skjermet') }} t2 
    on t1.skyldner = t2.off_id
    and t2.gyldig_fra_dato <= t1.vedtakstidspunkt
    and t2.gyldig_til_dato >= t1.vedtakstidspunkt

    left outer join {{ source('person', 'ident_off_id_til_fk_person1_ikke_skjermet') }} t3
    on t1.kravhaver = t3.off_id
    and t3.gyldig_fra_dato <= t1.vedtakstidspunkt
    and t3.gyldig_til_dato >= t1.vedtakstidspunkt

    left outer join {{ source('person', 'ident_off_id_til_fk_person1_ikke_skjermet') }} t4 
    on t1.mottaker = t4.off_id
    and t4.gyldig_fra_dato <= t1.vedtakstidspunkt
    and t4.gyldig_til_dato >= t1.vedtakstidspunkt
)

select * from final
 

 