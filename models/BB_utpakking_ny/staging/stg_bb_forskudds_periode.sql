with bb_meta_data as (
    select 
        kafka_offset, melding 
    from {{ref ('stg_bb_meta_data')}}
),

final as (
    select *
    from bb_meta_data
        ,json_table(melding, '$'
            columns (
                vedtaks_id varchar2(255) path '$.vedtaksid',
                nested path '$.forskuddPeriodeListe[*]'
                columns (
                    periode_fra                 varchar2(255) path '$.periodeFra'
                   ,periode_til                 varchar2(255) path '$.periodeTil'
                   ,belop                       varchar2(255) path '$.beløp'
                   ,resultat                    varchar2(255) path '$.resultat'
                   ,barnets_alders_gruppe       varchar2(255) path '$.barnetsAldersgruppe'
                   ,antall_barn_i_egen_husstand varchar2(255) path '$.antallBarnIEgenHusstand'
                   ,sivilstand                  varchar2(255) path '$.sivilstand'
                   ,barn_bor_med_bm             varchar2(255) path '$.barnBorMedBM'
                   ))
        ) j
    where json_value (melding, '$.forskuddPeriodeListe.size()' ) > 0
)

select 
    vedtaks_id
    ,periode_fra
    ,periode_til
    ,belop
    ,resultat
    ,barnets_alders_gruppe
    ,antall_barn_i_egen_husstand
    ,sivilstand
    ,barn_bor_med_bm
    ,kafka_offset
from final