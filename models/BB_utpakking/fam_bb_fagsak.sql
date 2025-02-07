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
          ,VEDTAKSTIDSPUNKT timestamp PATH '$.vedtakstidspunkt'
          ,BEHANDLINGS_TYPE VARCHAR2 PATH '$.type'
          ,saksnr            VARCHAR2 PATH '$.saksnr'
          ,FNR_KRAVHAVER VARCHAR2 PATH '$.kravhaver'
          ,FNR_MOTTAKER VARCHAR2 PATH '$.mottaker'
          ,historisk_vedtak VARCHAR2 PATH '$.historiskVedtak'
          )
        ) j
),

final as (
  select
    p.VEDTAKS_ID
    ,p.BEHANDLINGS_TYPE
    ,p.FNR_KRAVHAVER
    ,p.FNR_MOTTAKER
    ,p.saksnr 
    ,p.pk_bb_meta_data as fk_bb_meta_data
    ,p.VEDTAKSTIDSPUNKT
    ,p.historisk_vedtak
    ,nvl(ident_krav.fk_person1, -1) as fk_person1_kravhaver
    ,nvl(ident_mottaker.fk_person1, -1) as fk_person1_mottaker
    ,p.kafka_offset
  from pre_final p
  left outer join dt_person.ident_off_id_til_fk_person1 ident_krav
  on p.FNR_KRAVHAVER = ident_krav.off_id
  and VEDTAKSTIDSPUNKT between ident_krav.gyldig_fra_dato and ident_krav.gyldig_til_dato
  and ident_krav.skjermet_kode = 0
  left outer join dt_person.ident_off_id_til_fk_person1 ident_mottaker
  on p.FNR_MOTTAKER = ident_mottaker.off_id
  and VEDTAKSTIDSPUNKT between ident_krav.gyldig_fra_dato and ident_krav.gyldig_til_dato
  and ident_krav.skjermet_kode = 0  
)

select 
  dvh_fam_bb.DVH_FAMBB_KAFKA.nextval as pk_bb_fagsak
  ,VEDTAKS_ID
  ,kafka_offset
  ,VEDTAKSTIDSPUNKT
  ,BEHANDLINGS_TYPE
  ,saksnr 
  ,fk_person1_kravhaver
  ,fk_person1_mottaker
  ,CASE WHEN fk_person1_kravhaver = -1  THEN FNR_KRAVHAVER
      ELSE NULL
    END FNR_KRAVHAVER
  ,CASE WHEN fk_person1_mottaker = -1  THEN FNR_MOTTAKER
      ELSE NULL
    END FNR_MOTTAKER
  ,CASE WHEN historisk_vedtak = 'true' THEN 1
    ELSE 0
    END historisk_vedtak
  ,fk_bb_meta_data
  ,localtimestamp as lastet_dato
from final