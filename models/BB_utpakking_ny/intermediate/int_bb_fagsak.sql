with fagsak as (
    select * from {{ref ('stg_bb_fagsak')}}
),

pre_final as (
    select distinct
        fagsak.vedtaks_id
        ,fagsak.behandlings_type
        ,fagsak.fnr_kravhaver
        ,fagsak.fnr_mottaker
        ,fagsak.saksnr
        ,fagsak.vedtakstidspunkt
        ,fagsak.historisk_vedtak
        ,fagsak.fk_bb_meta_data
        ,nvl(ident_krav.fk_person1, -1) as fk_person1_kravhaver
        ,nvl(ident_mottaker.fk_person1, -1) as fk_person1_mottaker
        ,fagsak.kafka_offset

    from fagsak 
    --bruker ident_off_id_til_fk_person1 istedenfor ident_off_id_til_fk_person1_ikke_skjermet fordi vi vil ha treff på alle fnr
    --spiller ingen rolle om det er kode 6/7. Etter en prat med Hans, 
    --så har kode6/7 ingen lokasjon info i utpakket tabellene og fk_person1 i seg selv er ikke identifiserende
    left join {{ source ('person', 'ident_off_id_til_fk_person1') }} ident_krav
        on fagsak.fnr_kravhaver = ident_krav.off_id
        and fagsak.vedtakstidspunkt between ident_krav.gyldig_fra_dato and ident_krav.gyldig_til_dato

    left join {{ source ('person', 'ident_off_id_til_fk_person1') }} ident_mottaker
        on fagsak.fnr_mottaker = ident_mottaker.off_id
        and fagsak.vedtakstidspunkt between ident_mottaker.gyldig_fra_dato and ident_mottaker.gyldig_til_dato
),

final as (
    select 
        STANDARD_HASH(p.vedtaks_id || '|' || p.fk_person1_kravhaver, 'MD5') as pk_bb_fagsak
        ,p.*
    from pre_final p
)

select 
    * 
from final 

