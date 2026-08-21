{{
    config(
        materialized='incremental'
    )
}}


with sb_inn as (
    select * from {{ref ('int_bb_saerbidrag_inntekt')}}
),


final as (
    select pk_bb_inntekt_saerbidrag
        ,fk_bb_saerbidrag_fagsak
        ,kafka_offset
        ,saksnr
        ,vedtaks_id
        ,vedtakstidspunkt
        ,historisk_flagg
        ,inntekt_kategori
        ,inntekt_type
        ,inntekt_for
        ,inntekt_belop
        ,localtimestamp as lastet_dato 
    from sb_inn
)

select * from final

{% if is_incremental() %}
    WHERE kafka_offset > COALESCE(( SELECT MAX(t.kafka_offset) FROM {{ this }} t ), 0)
{% endif %}