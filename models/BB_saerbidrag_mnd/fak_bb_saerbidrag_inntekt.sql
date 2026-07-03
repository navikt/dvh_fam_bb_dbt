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
    VEDTAKSID,
    SAKSNR,
    TYPE_INNTEKT,
    INNTEKT_FOR,
    BELOP,
    lastet_dato as mart_lastet_dato,
    localtimestamp as lastet_dato 
    FROM inn
) 
PIVOT ( 
    SUM(BELOP)  
    FOR INNTEKT_FOR IN ( 
        'm' AS mottaker,
        'p' AS pliktig
    ) 
) piv
)

select * from final