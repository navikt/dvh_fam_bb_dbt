{{
    config(
        materialized='incremental'
    )
}}

with bb_meta_data as (
  select * from {{ref ('bb_meldinger_til_aa_pakke_ut')}}
),

pre_final as (
select * from bb_meta_data,
  json_table(melding, '$'
    COLUMNS (
          VEDTAKS_ID    VARCHAR2 PATH '$.vedtaksid'
          ,VEDTAKSTIDSPUNKT VARCHAR2 PATH '$.vedtakstidspunkt'
          ,BEHANDLINGS_TYPE VARCHAR2 PATH '$.type'
          ,saksr            VARCHAR2 PATH '$.saksr'
          ,FNR_KRAVHAVER VARCHAR2 PATH '$.kravhaver'
          ,FNR_MOTTAKER VARCHAR2 PATH '$.mottaker'
          )
        ) j
)
,

final as (
  select
    p.VEDTAKS_ID
    ,p.BEHANDLINGS_TYPE
    ,p.FNR_KRAVHAVER
    ,p.FNR_MOTTAKER
    ,p.saksr 
    ,CASE
      WHEN LENGTH(p.VEDTAKSTIDSPUNKT) = 25 THEN CAST(to_timestamp_tz(p.VEDTAKSTIDSPUNKT, 'yyyy-mm-dd"T"hh24:mi:ss TZH:TZM') AT TIME ZONE 'Europe/Belgrade' AS TIMESTAMP)
      ELSE CAST(to_timestamp_tz(p.VEDTAKSTIDSPUNKT, 'YYYY-MM-DD"T"HH24:MI:SS.ff') AT TIME ZONE 'Europe/Belgrade' AS TIMESTAMP)
      END VEDTAKSTIDSPUNKT
    ,p.pk_bb_meta_data as fk_bb_meta_data
    ,p.kafka_offset
    ,nvl(ident.fk_person1, -1) as fk_person1
  from pre_final p
  left outer join dt_person.ident_off_id_til_fk_person1 ident
  on p.person = ident.off_id
  and endret_tid between ident.gyldig_fra_dato and ident.gyldig_til_dato
  and ident.skjermet_kode = 0
)

select 
  dvh_fam_bb.DVH_FAMBB_KAFKA.nextval as pk_bb_fagsak
  ,VEDTAKS_ID
  ,VEDTAKSTIDSPUNKT
  ,BEHANDLINGS_TYPE
  ,saksr 
  ,FNR_KRAVHAVER
  ,FNR_MOTTAKER
  ,-1 AS FK_PERSON1_KRAVHAVER
  ,-1 AS FK_PERSON1_MOTTAKER
  ,kafka_offset
  ,localtimestamp as lastet_dato
  ,localtimestamp as OPPDATERT_DATO
  ,fk_bb_meta_data
from final