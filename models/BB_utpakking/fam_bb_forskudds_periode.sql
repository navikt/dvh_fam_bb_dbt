{{
    config(
        materialized='incremental'
    )
}}

with bb_meta_data as (
  select * from {{ref ('bb_meldinger_til_aa_pakke_ut')}}
),

bb_fagsak as (
  select VEDTAKS_ID, pk_bb_fagsak, kafka_offset from {{ref ('fam_bb_fagsak')}}
),

pre_final as (
select * from bb_meta_data,
  json_table(melding, '$'
    COLUMNS (
            VEDTAKS_ID    VARCHAR2 PATH '$.vedtaksid',
        NESTED PATH '$.forskuddPeriodeListe[*]'
        COLUMNS (
           PERIODE_FRA VARCHAR2 PATH '$.periodeFra'
          ,PERIODE_TIL VARCHAR2 PATH '$.periodeTil'
          ,BELOP VARCHAR2 PATH '$.beløp'
          ,RESULTAT VARCHAR2 PATH '$.resultat'
          ,BARNETS_ALDERS_GRUPPE VARCHAR2 PATH '$.barnetsAldersgruppe'
          ,ANTALL_BARN_I_EGEN_HUSSTAND VARCHAR2 PATH '$.antallBarnIEgenHusstand'
          ,SIVILSTAND VARCHAR2 PATH '$.sivilstand'
          ,BARN_BOR_MED_BM VARCHAR2 PATH '$.barnBorMedBM'
         ))
        ) j
    where json_value (melding, '$.forskuddPeriodeListe.size()' ) > 0
),

final as (
  select
     to_date(PERIODE_FRA,'yyyy-mm-dd') as PERIODE_FRA
    ,to_date(PERIODE_TIL,'yyyy-mm-dd') as PERIODE_TIL
    ,BELOP
    ,RESULTAT
    ,BARNETS_ALDERS_GRUPPE
    ,ANTALL_BARN_I_EGEN_HUSSTAND
    ,SIVILSTAND
    ,BARN_BOR_MED_BM
    ,pre_final.kafka_offset
    ,bb_fagsak.pk_bb_fagsak as fk_bb_fagsak
  from pre_final
  join bb_fagsak
  on pre_final.kafka_offset = bb_fagsak.kafka_offset
  and pre_final.vedtaks_id = bb_fagsak.vedtaks_id
)

select dvh_fam_bb.DVH_FAMBB_KAFKA.nextval as PK_BB_FORSKUDDS_PERIODE
    ,fk_bb_fagsak
    ,PERIODE_FRA
    ,PERIODE_TIL
    ,BELOP
    ,RESULTAT
    ,BARNETS_ALDERS_GRUPPE
    ,ANTALL_BARN_I_EGEN_HUSSTAND
    ,SIVILSTAND
    ,BARN_BOR_MED_BM
    ,kafka_offset
    ,localtimestamp as lastet_dato
from final