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


bb_bidrags_periode AS (
  SELECT PERIODE_FRA, PERIODE_TIL, pk_bb_bidrags_periode, fk_bb_fagsak from {{ref ('fam_bb_bidrags_periode_ord')}}
),

bp AS (
  SELECT
    kafka_offset,
    vedtaks_id,
    periode_fra,
    periode_til,
    type_inntekt,
    belop,
    'P' AS flagg
  FROM bb_meta_data,
  JSON_TABLE(melding, '$'
    COLUMNS (
      vedtaks_id VARCHAR2 PATH '$.vedtaksid',
      NESTED PATH '$.bidragPeriodeListe[*]'
      COLUMNS (
        periode_fra VARCHAR2 PATH '$.periodeFra',
        periode_til VARCHAR2 PATH '$.periodeTil',
        NESTED PATH '$.bpinntektListe[*]'
        COLUMNS (
          type_inntekt VARCHAR2 PATH '$.type',
          belop NUMBER PATH '$.beløp'
        )
      )
    )
  )j
  where type_inntekt is not null
),

bm AS (
  SELECT
    kafka_offset,
    vedtaks_id,
    periode_fra,
    periode_til,
    type_inntekt,
    belop,
    'M' AS flagg
  FROM bb_meta_data,
  JSON_TABLE(melding, '$'
    COLUMNS (
      vedtaks_id VARCHAR2 PATH '$.vedtaksid',
      NESTED PATH '$.bidragPeriodeListe[*]'
      COLUMNS (
        periode_fra VARCHAR2 PATH '$.periodeFra',
        periode_til VARCHAR2 PATH '$.periodeTil',
        NESTED PATH '$.bminntektListe[*]'
        COLUMNS (
          type_inntekt VARCHAR2 PATH '$.type',
          belop NUMBER PATH '$.beløp'
        )
      )
    )
  )j
  where type_inntekt is not null
),

pre_final AS (
  SELECT * FROM bp
  UNION ALL
  SELECT * FROM bm
),

final AS (
  SELECT
    pf.type_inntekt,
    pf.belop,
    pf.flagg,
    bb.pk_bb_bidrags_periode AS fk_bb_bidrags_periode,
    bb.periode_fra,
    bb.periode_til,
    pf.kafka_offset
  FROM pre_final pf
  JOIN bb_fagsak bf ON pf.kafka_offset = bf.kafka_offset AND pf.vedtaks_id = bf.vedtaks_id
  JOIN bb_bidrags_periode bb ON
    NVL(TO_DATE(pf.periode_fra, 'yyyy-mm-dd'), TO_DATE('2099-12-31', 'yyyy-mm-dd')) = NVL(bb.periode_fra, TO_DATE('2099-12-31', 'yyyy-mm-dd')) AND
    NVL(TO_DATE(pf.periode_til, 'yyyy-mm-dd'), TO_DATE('2099-12-31', 'yyyy-mm-dd')) = NVL(bb.periode_til, TO_DATE('2099-12-31', 'yyyy-mm-dd')) AND
    bb.fk_bb_fagsak = bf.pk_bb_fagsak
)

SELECT 
  dvh_fam_bb.DVH_FAMBB_KAFKA.nextval AS pk_bb_inntekt,
  fk_bb_bidrags_periode,
  type_inntekt,
  belop,
  flagg,
  kafka_offset,
  LOCALTIMESTAMP AS lastet_dato
FROM final


