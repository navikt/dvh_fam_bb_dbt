{{ config(
    materialized='table'
    ) 
}}

with sb as (
    select * from {{ref ('int_bb_saerbidrag_fagsak')}}
),

final as (
    select pk_bb_saerbidrag_fagsak
    ,kafka_offset
    ,fk_bb_meta_data
    ,vedtaksid
    ,vedtakstidspunkt
    ,bidragstype
    ,kategori
    ,saksnr
    ,fk_person1_skyldner
    ,fk_person1_kravhaver
    ,fk_person1_mottaker
    ,belop
    ,valutakode
    ,resultat
    ,innkreving_flagg
    ,omgjor_vedtak_id
    ,historisk_flagg
    ,kravbelop
    ,godkjent_belop
    ,betalt_belop
    ,localtimestamp as lastet_dato 
    from sb
)

select * from final