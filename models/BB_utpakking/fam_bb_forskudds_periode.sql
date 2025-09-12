{{
    config(
        materialized='incremental'
    )
}}

with bb_meta_data as (
    select * from {{ref ('bb_meldinger_til_aa_pakke_ut')}}
),

bb_fagsak as (
    select vedtaks_id, pk_bb_fagsak, kafka_offset
    from {{ref ('fam_bb_fagsak')}}
),

pre_final as (
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
),

final as (
    select
        to_date(periode_fra,'yyyy-mm-dd') as periode_fra
       ,to_date(periode_til,'yyyy-mm-dd') as periode_til
       ,belop
       ,resultat
       ,barnets_alders_gruppe
       ,antall_barn_i_egen_husstand
       ,sivilstand
       ,case
           when barn_bor_med_bm = 'true' then '1'
           when barn_bor_med_bm = 'false' then '0'
           else barn_bor_med_bm  
        end barn_bor_med_bm
       ,pre_final.kafka_offset
       ,bb_fagsak.pk_bb_fagsak as fk_bb_fagsak
    from pre_final
    join bb_fagsak
    on pre_final.kafka_offset = bb_fagsak.kafka_offset
    and pre_final.vedtaks_id = bb_fagsak.vedtaks_id
)

select dvh_fam_bb.dvh_fambb_kafka.nextval as pk_bb_forskudds_periode
    ,fk_bb_fagsak
    ,periode_fra
    ,periode_til
    ,belop
    ,resultat
    ,barnets_alders_gruppe
    ,antall_barn_i_egen_husstand
    ,sivilstand
    ,barn_bor_med_bm
    ,kafka_offset
    ,localtimestamp as lastet_dato
from final