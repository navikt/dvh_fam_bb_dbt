{{
    config(
        materialized='table'
    )
}}

with inn as (
    select * from {{ref ('fam_bb_saerbidrag_inntekt')}}
),
 
final as (
 SELECT * 
FROM ( 
    SELECT
    FK_BB_SAERBIDRAG_FAGSAK,
    SAKSNR,
    VEDTAKS_ID,
    vedtakstidspunkt,
    inntekt_kategori,
    inntekt_type,
    INNTEKT_FOR,
    inntekt_belop,
    lastet_dato as mart_lastet_dato
    FROM inn
) 
PIVOT ( 
    SUM(inntekt_belop)  
    FOR INNTEKT_FOR IN ( 
        'm' AS inntekt_mottaker,
        'p' AS inntekt_skyldner
    ) 
) piv
)

select     
    RAWTOHEX(FK_BB_SAERBIDRAG_FAGSAK)  as key_fak_bb_saerbidrag,
    VEDTAKS_ID,
    SAKSNR,
    vedtakstidspunkt,
    inntekt_kategori,
    inntekt_type,
    inntekt_mottaker,
    inntekt_skyldner,
    '{{ var("gyldig_flagg") }}'  as gyldig_flagg,
    mart_lastet_dato,
    localtimestamp as lastet_dato  
 from final