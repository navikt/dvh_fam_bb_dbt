{{
    config(
        materialized='incremental'
    )
}}

with sb as (
    select * from {{ref ('int_bb_saerbidrag_fagsak')}}
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
        ,resultat
        ,valuta_kode
        ,belop
        ,krav_belop
        ,godkjent_belop
        ,betalt_belop
        ,localtimestamp as lastet_dato 
    from sb
)

select * from final

{% if is_incremental() %}
    WHERE kafka_offset > COALESCE(( SELECT MAX(t.kafka_offset) FROM {{ this }} t ), 0)
{% endif %}
