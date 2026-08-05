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
    VEDTAKS_ID,
    SAKSNR,
    vedtaks_tidspunkt,
    TYPE_INNTEKT,
    INNTEKT_FOR,
    BELOP,
    lastet_dato as mart_lastet_dato
    FROM inn
) 
PIVOT ( 
    SUM(BELOP)  
    FOR INNTEKT_FOR IN ( 
        'm' AS inntekt_mottaker,
        'p' AS inntekt_pliktig
    ) 
) piv
)

select     
    FK_BB_SAERBIDRAG_FAGSAK,
    VEDTAKS_ID,
    SAKSNR,
    vedtaks_tidspunkt,
    TYPE_INNTEKT,
    inntekt_mottaker,
    inntekt_pliktig,
    '{{ var("gyldig_flagg") }}'  as gyldig_flagg,
    mart_lastet_dato,
    localtimestamp as lastet_dato  
 from final