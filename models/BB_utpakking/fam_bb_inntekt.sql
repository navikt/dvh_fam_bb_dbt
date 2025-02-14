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


bb_forskudds_periode AS (
  SELECT PERIODE_FRA, PERIODE_TIL, pk_bb_forskudds_periode, fk_bb_fagsak from {{ref ('fam_bb_forskudds_periode')}}
),

pre_final as (
select * from bb_meta_data,
  json_table(melding, '$'
    COLUMNS (
      VEDTAKS_ID    VARCHAR2 PATH '$.vedtaksid',
      NESTED PATH '$.forskuddPeriodeListe[*]'
        COLUMNS (
          PERIODE_FRA VARCHAR2 PATH '$.periodeFra'
          ,PERIODE_TIL VARCHAR2 PATH '$.periodeTil',
          NESTED PATH '$.inntektListe[*]'
          COLUMNS (
            TYPE_INNTEKT VARCHAR2 PATH '$.type'
            ,BELOP VARCHAR2 PATH '$.beløp'
            )
          )
       )
    ) j
    where json_value (melding, '$.forskuddPeriodeListe.inntektListe.size()' ) > 0
),

final as (
  select
    TYPE_INNTEKT
    ,BELOP
    ,bb_forskudds_periode.PK_BB_FORSKUDDS_PERIODE as FK_BB_FORSKUDDS_PERIODE
    ,bb_forskudds_periode.PERIODE_FRA
    ,bb_forskudds_periode.PERIODE_TIL
    ,pre_final.kafka_offset
  from pre_final
  inner join bb_fagsak
  on pre_final.kafka_offset = bb_fagsak.kafka_offset
  and pre_final.vedtaks_id = bb_fagsak.vedtaks_id
  inner join bb_forskudds_periode
  on nvl(to_date(pre_final.PERIODE_FRA,'yyyy-mm-dd'),to_date('2099-12-31', 'yyyy-mm-dd')) = nvl(bb_forskudds_periode.PERIODE_FRA,to_date('2099-12-31', 'yyyy-mm-dd'))
  and nvl(to_date(pre_final.PERIODE_TIL,'yyyy-mm-dd'),to_date('2099-12-31', 'yyyy-mm-dd')) = nvl(bb_forskudds_periode.PERIODE_TIL,to_date('2099-12-31', 'yyyy-mm-dd'))
  and bb_forskudds_periode.fk_bb_fagsak = bb_fagsak.pk_bb_fagsak
)

select 
  dvh_fam_bb.DVH_FAMBB_KAFKA.nextval as PK_BB_INNTEKT
  ,FK_BB_FORSKUDDS_PERIODE
  ,CASE WHEN TYPE_INNTEKT = 'true' THEN '1'
        WHEN TYPE_INNTEKT = 'false' THEN '0'
        ELSE TYPE_INNTEKT
  END TYPE_INNTEKT
  ,BELOP
  ,kafka_offset
  ,localtimestamp as lastet_dato
from final