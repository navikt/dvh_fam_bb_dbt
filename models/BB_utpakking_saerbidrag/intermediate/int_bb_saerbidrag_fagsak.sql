
with sb as (
    select * from {{ref ('stg_bb_saerbidrag_fagsak')}}
),


final as (
    select kafka_offset
    ,fk_bb_meta_data
    ,vedtaks_id
    ,vedtaks_tidspunkt
    ,type as bidragstype
    ,kategori
    ,saksnr
    ,STANDARD_HASH(t1.vedtaks_id || t1.saksnr || t1.kravhaver, 'MD5') as pk_bb_saerbidrag_fagsak
    ,nvl(t2.fk_person1, -1 ) as fk_person1_skyldner
    ,nvl(t3.fk_person1, -1 ) as fk_person1_kravhaver
    ,nvl(t4.fk_person1, -1 ) as fk_person1_mottaker
    ,belop
    ,valuta_kode
    ,resultat
    ,case when innkreving_flagg = 'true' then 1 else 0 end as innkreving_flagg
    ,omgjor_vedtaks_id
    ,case when historisk_vedtak = 'true' then 1 else 0 end as historisk_flagg
    ,krav_belop
    ,godkjent_belop
    ,betalt_belop
    --,localtimestamp as lastet_dato 
    from sb t1
    left outer join {{ source('person', 'ident_off_id_til_fk_person1_ikke_skjermet') }} t2 on
    t1.skyldner = t2.off_id
    and t2.gyldig_fra_dato <= t1.vedtaks_tidspunkt
    and t2.gyldig_til_dato >= t1.vedtaks_tidspunkt

    left outer join {{ source('person', 'ident_off_id_til_fk_person1_ikke_skjermet') }} t3 on
    t1.kravhaver = t3.off_id
    and t3.gyldig_fra_dato <= t1.vedtaks_tidspunkt
    and t3.gyldig_til_dato >= t1.vedtaks_tidspunkt

    left outer join {{ source('person', 'ident_off_id_til_fk_person1_ikke_skjermet') }} t4 on
    t1.mottaker = t4.off_id
    and t4.gyldig_fra_dato <= t1.vedtaks_tidspunkt
    and t4.gyldig_til_dato >= t1.vedtaks_tidspunkt
)



select * from final
where fk_person1_kravhaver <> -1
 

 