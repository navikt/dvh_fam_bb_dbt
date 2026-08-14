{{
    config(
        materialized='incremental'
    )
}}

with sb as (
    select * from {{ref ('int_bb_saerbidrag_fagsak')}}
),

map_resultat as (
    select resultat_fra
        ,resultat_til as resultat
    from {{ source ('fam_bb', 'fam_bb_bidrag_resultat_mapping') }}
),

final as (
    select pk_bb_saerbidrag_fagsak
        ,kafka_offset
        ,fk_bb_meta_data
        ,saksnr
        ,vedtaks_id
        ,omgjor_vedtaks_id
        ,vedtaks_tid
        ,behandlings_type
        ,kategori
        ,fk_person1_skyldner
        ,fk_person1_kravhaver
        ,fk_person1_mottaker
        ,innkreving_flagg
        ,historisk_flagg
        ,case when t2.resultat is null then t1.resultat else t2.resultat end as resultat
        ,valuta_kode
        ,belop
        ,krav_belop
        ,godkjent_belop
        ,betalt_belop
        ,localtimestamp as lastet_dato 
    from sb t1
    left join map_resultat t2
    on t1.resultat = t2.resultat_fra

)

select * from final

{% if is_incremental() %}
    WHERE kafka_offset > COALESCE(( SELECT MAX(t.kafka_offset) FROM {{ this }} t ), 0)
{% endif %}
