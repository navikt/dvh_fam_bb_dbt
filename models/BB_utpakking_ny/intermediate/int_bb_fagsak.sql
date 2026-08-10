with fagsak as (
    select * from {{ref ('stg_bb_fagsak')}}
),

pre_final as (
    select distinct
        fagsak.vedtaks_id,
        fagsak.behandlings_type,
        fagsak.fnr_kravhaver,
        fagsak.fnr_mottaker,
        fagsak.saksnr,
        fagsak.pk_bb_meta_data as fk_bb_meta_data,
        fagsak.vedtakstidspunkt,
        fagsak.historisk_vedtak,
        nvl(ident_krav.fk_person1, -1) as fk_person1_kravhaver,
        nvl(ident_mottaker.fk_person1, -1) as fk_person1_mottaker,
        fagsak.kafka_offset

    from fagsak 
    left join {{ source ('person', 'ident_off_id_til_fk_person1_ikke_skjermet') }} ident_krav
        on fagsak.fnr_kravhaver = ident_krav.off_id
        and fagsak.vedtakstidspunkt between ident_krav.gyldig_fra_dato and ident_krav.gyldig_til_dato

    left join {{ source ('person', 'ident_off_id_til_fk_person1_ikke_skjermet') }} ident_mottaker
        on fagsak.fnr_mottaker = ident_mottaker.off_id
        and fagsak.vedtakstidspunkt between ident_mottaker.gyldig_fra_dato and ident_mottaker.gyldig_til_dato
),

final as (
    select 
        p.*,
        STANDARD_HASH(p.vedtaks_id || '|' || p.fk_person1_kravhaver, 'MD5') as pk_bb_fagsak
    from pre_final p
)

select 
    * 
from final 
where fk_person1_kravhaver <> -1

