{{ config(
    materialized='incremental',
    unique_key = ['aar_maaned', 'gyldig_flagg'],
    incremental_strategy='delete+insert'
    ) 
}}

with tid as (

  select aar_maaned, siste_dato_i_perioden, aar, pk_dim_tid
  from {{ source ('kode_verk', 'dim_tid') }}
  where gyldig_flagg = 1
  and dim_nivaa = 3

  and aar_maaned between '{{ var ("periode_fom") }}' and '{{ var ("periode_tom") }}' 
),

fagsak as (
  select
    fagsak.pk_bb_fagsak,
    fagsak.vedtaks_id,
    fagsak.behandlings_type, --
    fagsak.vedtakstidspunkt,
    fagsak.saksnr,
    fagsak.fk_person1_kravhaver,
    fagsak.fk_person1_mottaker,
    fagsak.fk_person1_skyldner, --

    periode.pk_bb_bidrags_periode, --
    periode.periode_fra,
    periode.periode_til,
    periode.belop,
    periode.resultat,
    periode.bidragsevne, --
    periode.underholdskostnad, --
    periode.samvaersfradrag, --
    periode.netto_barnetillegg_bp, --
    periode.netto_barnetillegg_bm, --
    periode.samvaersklasse, --
    periode.bps_andel_underholdskostnad, --
    periode.bpbor_med_andre_voksne, --

    tid.aar_maaned,
    tid.siste_dato_i_perioden,
    tid.aar,
    tid.pk_dim_tid as fk_dim_tid_mnd,
    row_number() over (partition by tid.aar_maaned, fagsak.fk_person1_kravhaver ,fagsak.saksnr
            order by fagsak.vedtakstidspunkt desc, periode.belop desc
        ) nr
  from {{ source ('fam_bb', 'fam_bb_fagsak_ord') }} fagsak
 
  join {{ source ('fam_bb', 'fam_bb_bidrags_periode_ord') }} periode
  on fagsak.pk_bb_fagsak = periode.fk_bb_fagsak
  --and periode.belop >= 0

  join tid
  on periode.periode_fra <= to_date(tid.aar_maaned||'01', 'yyyymmdd')
  and nvl(periode.periode_til, tid.siste_dato_i_perioden) >= tid.siste_dato_i_perioden

  where fagsak.behandlings_type not in ('ENDRING_MOTTAKER', 'OPPHØR', 'ALDERSOPPHØR')
  and trunc(fagsak.vedtakstidspunkt, 'dd') <= to_date('{{ var ("max_vedtaksdato") }}', 'yyyymmdd')
),


siste as (

  select *
  from fagsak
  where nr = 1
),

opphor_fra as (

      select fagsak.fk_person1_kravhaver, fagsak.saksnr, fagsak.vedtakstidspunkt
      ,min(periode.periode_fra) periode_fra_opphor
      from {{ source ('fam_bb', 'fam_bb_fagsak_ord') }} fagsak 

      join {{ source ('fam_bb', 'fam_bb_bidrags_periode_ord') }} periode
      on fagsak.pk_bb_fagsak = periode.fk_bb_fagsak
      and periode.belop is null

      where fagsak.behandlings_type not in ('ENDRING_MOTTAKER')
      and trunc(fagsak.vedtakstidspunkt, 'dd') <= to_date('{{ var ("max_vedtaksdato") }}', 'yyyymmdd')--Begrense max_vedtaksdato på dag nivå

      group by fagsak.fk_person1_kravhaver, fagsak.saksnr, fagsak.vedtakstidspunkt
),

siste_opphør as (
    select siste.*
        ,opphor_fra.periode_fra_opphor
    from siste
    left join opphor_fra
    on opphor_fra.fk_person1_kravhaver = siste.fk_person1_kravhaver
    and opphor_fra.saksnr = siste.saksnr

    and opphor_fra.vedtakstidspunkt > siste.vedtakstidspunkt
),

opphor_hvis_finnes as
(
  select aar_maaned, siste_dato_i_perioden, aar, pk_bb_fagsak,fk_dim_tid_mnd,
        vedtaks_id, behandlings_type, vedtakstidspunkt, saksnr, fk_person1_kravhaver, fk_person1_mottaker,
        fk_person1_skyldner, pk_bb_bidrags_periode, periode_fra, periode_til, belop,
        resultat, bidragsevne, underholdskostnad, samvaersfradrag, netto_barnetillegg_bp, netto_barnetillegg_bm, 
        samvaersklasse, bps_andel_underholdskostnad, bpbor_med_andre_voksne,
        min(periode_fra_opphor) periode_fra_opphor
  from siste_opphør
  where siste_dato_i_perioden < nvl(periode_fra_opphor, siste_dato_i_perioden+1)

  group by aar_maaned, siste_dato_i_perioden, aar, pk_bb_fagsak,fk_dim_tid_mnd,
        vedtaks_id, behandlings_type, vedtakstidspunkt, saksnr, fk_person1_kravhaver, fk_person1_mottaker,
        fk_person1_skyldner, pk_bb_bidrags_periode, periode_fra, periode_til, belop,
        resultat, bidragsevne, underholdskostnad, samvaersfradrag, netto_barnetillegg_bp, netto_barnetillegg_bm, 
        samvaersklasse, bps_andel_underholdskostnad, bpbor_med_andre_voksne
),

inntekt as (
    select  
        fk_bb_bidrags_periode,
        type_inntekt,
        belop,
        flagg,
        row_number() over (partition by fk_bb_bidrags_periode, flagg order by type_inntekt) as nr
    from fam_bb_inntekts_liste_ord
),

inntekts_typer as (
select
        fk_bb_bidrags_periode,
        max(case when nr = 1 and flagg = 'P' then type_inntekt end) as p_type_inntekt_1,
        max(case when nr = 1 and flagg = 'P' then belop end) as p_inntekt_1,
        max(case when nr = 2 and flagg = 'P' then type_inntekt end) as p_type_inntekt_2,
        max(case when nr = 2 and flagg = 'P' then belop end) as p_inntekt_2,
        max(case when nr = 3 and flagg = 'P' then type_inntekt end) as p_type_inntekt_3,
        max(case when nr = 3 and flagg = 'P' then belop end) as p_inntekt_3,
        
        max(case when nr = 1 and flagg = 'M' then type_inntekt end) as m_type_inntekt_1,
        max(case when nr = 1 and flagg = 'M' then belop end) as m_inntekt_1,
        max(case when nr = 2 and flagg = 'M' then type_inntekt end) as m_type_inntekt_2,
        max(case when nr = 2 and flagg = 'M' then belop end) as m_inntekt_2,
        max(case when nr = 3 and flagg = 'M' then type_inntekt end) as m_type_inntekt_3,
        max(case when nr = 3 and flagg = 'M' then belop end) as m_inntekt_3,
        max(case when nr = 4 and flagg = 'M' then type_inntekt end) as m_type_inntekt_4,
        max(case when nr = 4 and flagg = 'M' then belop end) as m_inntekt_4,
        max(case when nr = 5 and flagg = 'M' then type_inntekt end) as m_type_inntekt_5,
        max(case when nr = 5 and flagg = 'M' then belop end) as m_inntekt_5,

        sum(case when flagg = 'P' then belop else 0 end) as p_inntekt_total,
        max(case when flagg = 'P' then nr else 0 end) as p_antall_typer,

        sum(case when flagg = 'M' then belop else 0 end) as m_inntekt_total,
        max(case when flagg = 'M' then nr else 0 end) as m_antall_typer

    from inntekt
    group by fk_bb_bidrags_periode
),

periode_uten_opphort as (
 
  select aar_maaned, fk_person1_kravhaver, fk_person1_mottaker, fk_person1_skyldner, vedtakstidspunkt
        ,pk_bb_fagsak as fk_bb_fagsak, saksnr
        ,vedtaks_id, behandlings_type, pk_bb_bidrags_periode as fk_bb_bidrags_periode
        ,periode_fra, periode_til, belop, resultat
        ,periode_fra_opphor, aar
        ,bidragsevne, underholdskostnad, samvaersfradrag, netto_barnetillegg_bp, netto_barnetillegg_bm,
        samvaersklasse, bps_andel_underholdskostnad, bpbor_med_andre_voksne
        --,TO_DATE(TO_CHAR(LAST_DAY(SYSDATE), 'YYYYMMDD'), 'YYYYMMDD') MAX_VEDTAKSDATO --Input max_vedtaksdato
        ,to_date('{{ var ("max_vedtaksdato") }}', 'yyyymmdd') max_vedtaksdato
        ,fk_dim_tid_mnd
        ,'{{ var ("periode_type") }}' periode_type --Input periode_type
        ,dim_kravhaver.pk_dim_person as fk_dim_person_kravhaver
        ,floor(months_between(vedtak.siste_dato_i_perioden, dim_kravhaver.fodt_dato)/12) alder_kravhaver
        ,dim_kravhaver.kjonn_nr kjonn_kravhaver
        
        ,dim_mottaker.pk_dim_person as fk_dim_person_mottaker
        ,dim_mottaker.bosted_kommune_nr as bosted_kommune_nr_mottaker
        ,dim_mottaker.fk_dim_land_statsborgerskap as fk_dim_land_statsborgerskap_mottaker
        ,dim_mottaker.fk_dim_geografi_bosted as fk_dim_geografi_bosted_mottaker
        ,floor(months_between(vedtak.siste_dato_i_perioden, dim_mottaker.fodt_dato)/12) alder_mottaker
        
        ,dim_skyldner.pk_dim_person as fk_dim_person_skyldner
        ,dim_skyldner.bosted_kommune_nr as bosted_kommune_nr_skyldner
        ,dim_skyldner.fk_dim_land_statsborgerskap as fk_dim_land_statsborgerskap_skyldner
        ,dim_skyldner.fk_dim_geografi_bosted as fk_dim_geografi_bosted_skyldner
        ,floor(months_between(vedtak.siste_dato_i_perioden, dim_skyldner.fodt_dato)/12) alder_skyldner
        
        ,inntekts_typer.p_type_inntekt_1,inntekts_typer.p_inntekt_1
        ,inntekts_typer.p_type_inntekt_2,inntekts_typer.p_inntekt_2,inntekts_typer.p_type_inntekt_3,inntekts_typer.p_inntekt_3
        ,inntekts_typer.p_inntekt_total, inntekts_typer.p_antall_typer
        
        ,inntekts_typer.m_type_inntekt_1
        ,inntekts_typer.m_inntekt_1, inntekts_typer.m_type_inntekt_2, inntekts_typer.m_inntekt_2
        ,inntekts_typer.m_type_inntekt_3, inntekts_typer.m_inntekt_3, inntekts_typer.m_type_inntekt_4
        ,inntekts_typer.m_inntekt_4,inntekts_typer.m_type_inntekt_5, inntekts_typer.m_inntekt_5
        ,inntekts_typer.m_inntekt_total, inntekts_typer.m_antall_typer
        ,'{{ var ("gyldig_flagg") }}' as gyldig_flagg --Input gyldig_flagg
        ,localtimestamp as lastet_dato
  from opphor_hvis_finnes vedtak
 
  left join dt_person.dim_person dim_kravhaver
  on dim_kravhaver.fk_person1 = vedtak.fk_person1_kravhaver
  and vedtak.fk_person1_kravhaver != -1
  and vedtak.siste_dato_i_perioden between dim_kravhaver.gyldig_fra_dato and dim_kravhaver.gyldig_til_dato
 
  left join dt_person.dim_person dim_mottaker
  on dim_mottaker.fk_person1 = vedtak.fk_person1_mottaker
  and vedtak.fk_person1_mottaker != -1
  and vedtak.siste_dato_i_perioden between dim_mottaker.gyldig_fra_dato and dim_mottaker.gyldig_til_dato
  
  left join dt_person.dim_person dim_skyldner
  on dim_skyldner.fk_person1 = vedtak.fk_person1_skyldner
  and vedtak.fk_person1_skyldner != -1
  and vedtak.siste_dato_i_perioden between dim_skyldner.gyldig_fra_dato and dim_skyldner.gyldig_til_dato

  left join inntekts_typer
  on vedtak.pk_bb_bidrags_periode = inntekts_typer.fk_bb_bidrags_periode
)

select 
    * 
from periode_uten_opphort