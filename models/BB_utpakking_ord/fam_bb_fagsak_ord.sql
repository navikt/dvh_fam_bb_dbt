{{
    config(
        materialized = 'incremental'
    )
}}

with bb_meta_data as (
  select * from {{ref ('bb_meldinger_til_aa_pakke_ut_ord')}}
),

pre_final as (
select * from bb_meta_data,
  json_table(melding, '$'
    COLUMNS (
          VEDTAKS_ID    VARCHAR2 PATH '$.vedtaksid'
          ,VEDTAKSTIDSPUNKT timestamp PATH '$.vedtakstidspunkt'
          ,type     VARCHAR2 PATH '$.type'
          ,saksnr            VARCHAR2 PATH '$.saksnr'
          ,skyldner     VARCHAR2 PATH '$.skyldner'
          ,FNR_KRAVHAVER VARCHAR2 PATH '$.kravhaver'
          ,FNR_MOTTAKER VARCHAR2 PATH '$.mottaker'
          ,historisk_vedtak VARCHAR2 PATH '$.historiskVedtak'
          )
        ) j
),

final AS (
    SELECT DISTINCT 
        p.VEDTAKS_ID,
        p.type,
        p.skyldner,
        p.saksnr,
        p.FNR_KRAVHAVER,
        p.FNR_MOTTAKER,
        p.pk_bb_meta_data AS fk_bb_meta_data,
        p.VEDTAKSTIDSPUNKT,
        p.historisk_vedtak,
        NVL(ident_skyldner.fk_person1, -1) AS fk_person1_skyldner,
        NVL(ident_krav.fk_person1, -1) AS fk_person1_kravhaver,
        NVL(ident_mottaker.fk_person1, -1) AS fk_person1_mottaker,
        p.kafka_offset
    FROM pre_final p
    LEFT JOIN dt_person.ident_off_id_til_fk_person1 ident_krav
      ON p.FNR_KRAVHAVER = ident_krav.off_id
     AND p.VEDTAKSTIDSPUNKT BETWEEN ident_krav.gyldig_fra_dato AND ident_krav.gyldig_til_dato
    LEFT JOIN dt_person.ident_off_id_til_fk_person1 ident_mottaker
      ON p.FNR_MOTTAKER = ident_mottaker.off_id
     AND p.VEDTAKSTIDSPUNKT BETWEEN ident_mottaker.gyldig_fra_dato AND ident_mottaker.gyldig_til_dato
    LEFT JOIN dt_person.ident_off_id_til_fk_person1 ident_skyldner
      ON p.skyldner = ident_skyldner.off_id
     AND p.VEDTAKSTIDSPUNKT BETWEEN ident_skyldner.gyldig_fra_dato AND ident_skyldner.gyldig_til_dato
)

SELECT 
    dvh_fam_bb.DVH_FAMBB_KAFKA.nextval AS pk_bb_fagsak,
    VEDTAKS_ID,
    type,
    kafka_offset,
    VEDTAKSTIDSPUNKT,
    saksnr,
    fk_person1_kravhaver,
    fk_person1_mottaker,
    fk_person1_skyldner,
    CASE WHEN fk_person1_kravhaver = -1 THEN FNR_KRAVHAVER ELSE NULL END AS FNR_KRAVHAVER,
    CASE WHEN fk_person1_mottaker = -1 THEN FNR_MOTTAKER ELSE NULL END AS FNR_MOTTAKER,
    CASE WHEN fk_person1_skyldner = -1 THEN skyldner ELSE NULL END AS skyldner,
    CASE WHEN historisk_vedtak = 'true' THEN 1 ELSE 0 END AS historisk_vedtak,
    fk_bb_meta_data,
    localtimestamp AS lastet_dato
FROM final