{{
    config(
        materialized='incremental'
    )
}}

with bb_meta_data as (
  select * from {{ref ('bb_meldinger_til_aa_pakke_ut_ord')}} 
),

bb_fagsak as (
  select VEDTAKS_ID, pk_bb_fagsak, kafka_offset from {{ref ('fam_bb_fagsak_ord')}}
),

pre_final as (
select * from bb_meta_data,
  json_table(melding, '$'
    COLUMNS (
            VEDTAKS_ID    VARCHAR2 PATH '$.vedtaksid',
        NESTED PATH '$.bidragPeriodeListe[*]'
        COLUMNS (
           PERIODE_FRA VARCHAR2 PATH '$.periodeFra'
          ,PERIODE_TIL VARCHAR2 PATH '$.periodeTil'
          ,BELOP NUMBER PATH '$.beløp'
          ,RESULTAT VARCHAR2 PATH '$.resultat'
          ,BIDRAGSEVNE NUMBER PATH '$.bidragsevne'
          ,UNDERHOLDSKOSTNAD NUMBER PATH '$.underholdskostnad'
          ,SAMVAERSFRADRAG NUMBER PATH '$.samværsfradrag'
          ,NETTO_BARNETILLEGG_BP NUMBER PATH '$.nettoBarnetilleggBP'
          ,NETTO_BARNETILLEGG_BM NUMBER PATH '$.nettoBarnetilleggBM'
          ,SAMVAESKLASSE VARCHAR2 PATH '$.samværsklasse'
          ,BPS_ANDEL_UNDERHOLDSKOSTNAD NUMBER PATH '$.bpsAndelUnderholdskostnad'
          ,BPBOR_MED_ANDRE_VOKSNE VARCHAR2 PATH '$.bpborMedAndreVoksne'
         ))
        ) j
        where PERIODE_FRA is not null
    --where json_value (melding, '$.forskuddPeriodeListe.size()' ) > 0
),

final as (
  select
     to_date(PERIODE_FRA,'yyyy-mm-dd') as PERIODE_FRA
    ,to_date(PERIODE_TIL,'yyyy-mm-dd') as PERIODE_TIL
    ,BELOP
    ,RESULTAT
    ,BIDRAGSEVNE
    ,UNDERHOLDSKOSTNAD
    ,SAMVAERSFRADRAG
    ,NETTO_BARNETILLEGG_BP
    ,NETTO_BARNETILLEGG_BM
    ,SAMVAESKLASSE
    ,BPS_ANDEL_UNDERHOLDSKOSTNAD
    ,CASE
        WHEN BPBOR_MED_ANDRE_VOKSNE = 'true' THEN '1'
        WHEN BPBOR_MED_ANDRE_VOKSNE = 'false' THEN '0'
        ELSE BPBOR_MED_ANDRE_VOKSNE  
    END BPBOR_MED_ANDRE_VOKSNE
    ,pre_final.kafka_offset
    ,bb_fagsak.pk_bb_fagsak as fk_bb_fagsak
  from pre_final
  join bb_fagsak
  on pre_final.kafka_offset = bb_fagsak.kafka_offset
  and pre_final.vedtaks_id = bb_fagsak.vedtaks_id
)

select dvh_fam_bb.DVH_FAMBB_KAFKA.nextval as PK_BB_BIDRAGS_PERIODE
    ,fk_bb_fagsak
    ,PERIODE_FRA
    ,PERIODE_TIL
    ,BELOP
    ,RESULTAT
    ,BIDRAGSEVNE
    ,UNDERHOLDSKOSTNAD
    ,SAMVAERSFRADRAG
    ,NETTO_BARNETILLEGG_BP
    ,NETTO_BARNETILLEGG_BM
    ,SAMVAESKLASSE
    ,BPS_ANDEL_UNDERHOLDSKOSTNAD
    ,BPBOR_MED_ANDRE_VOKSNE
    ,kafka_offset
    ,localtimestamp as lastet_dato
from final