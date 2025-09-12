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
          vedtaks_id          varchar2(255) path '$.vedtaksid'
          ,vedtakstidspunkt   timestamp(9)  path '$.vedtakstidspunkt'
          ,behandlings_type   varchar2(255) path '$.type'
          ,saksnr             varchar2(255) path '$.saksnr'
          ,fnr_skyldner       varchar2(255) path '$.skyldner'
          ,fnr_kravhaver      varchar2(255) path '$.kravhaver'
          ,fnr_mottaker       varchar2(255) path '$.mottaker'
          ,historisk_vedtak   varchar2(255) path '$.historiskVedtak'
          ,innkreving_flagg   varchar2(255) path '$.innkreving'
          )
        ) j 
),

final as (
    select distinct 
        p.vedtaks_id,
        p.behandlings_type,
        p.fnr_skyldner,
        p.saksnr,
        p.fnr_kravhaver,
        p.fnr_mottaker,
        p.innkreving_flagg,
        p.pk_bb_meta_data as fk_bb_meta_data,
        p.vedtakstidspunkt,
        p.historisk_vedtak,
        nvl(ident_skyldner.fk_person1, -1) as fk_person1_skyldner,
        nvl(ident_krav.fk_person1, -1) as fk_person1_kravhaver,
        nvl(ident_mottaker.fk_person1, -1) as fk_person1_mottaker,
        p.kafka_offset
    from pre_final p
    left join dt_person.ident_off_id_til_fk_person1 ident_krav
      on p.fnr_kravhaver = ident_krav.off_id
     and p.vedtakstidspunkt between ident_krav.gyldig_fra_dato and ident_krav.gyldig_til_dato
    left join dt_person.ident_off_id_til_fk_person1 ident_mottaker
      on p.fnr_mottaker = ident_mottaker.off_id
     and p.vedtakstidspunkt between ident_mottaker.gyldig_fra_dato and ident_mottaker.gyldig_til_dato
    left join dt_person.ident_off_id_til_fk_person1 ident_skyldner
      on p.fnr_skyldner = ident_skyldner.off_id
     and p.vedtakstidspunkt between ident_skyldner.gyldig_fra_dato and ident_skyldner.gyldig_til_dato
)

select 
    dvh_fam_bb.dvh_fambb_kafka.nextval as pk_bb_fagsak,
    vedtaks_id,
    behandlings_type,
    kafka_offset,
    vedtakstidspunkt,
    saksnr,
    innkreving_flagg,
    fk_person1_kravhaver,
    fk_person1_mottaker,
    fk_person1_skyldner,
    case when fk_person1_kravhaver = -1 then fnr_kravhaver else null end as fnr_kravhaver,
    case when fk_person1_mottaker = -1 then fnr_mottaker else null end as fnr_mottaker,
    case when fk_person1_skyldner = -1 then fnr_skyldner else null end as fnr_skyldner,
    case when historisk_vedtak = 'true' then 1 else 0 end as historisk_vedtak,
    fk_bb_meta_data,
    localtimestamp as lastet_dato
from final