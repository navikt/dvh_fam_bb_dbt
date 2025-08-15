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
    periode.BIDRAGSEVNE, --
    periode.UNDERHOLDSKOSTNAD, --
    periode.SAMVAERSFRADRAG, --
    periode.NETTO_BARNETILLEGG_BP, --
    periode.NETTO_BARNETILLEGG_BM, --
    periode.SAMVAERSKLASSE, --
    periode.BPS_ANDEL_UNDERHOLDSKOSTNAD, --
    periode.BPBOR_MED_ANDRE_VOKSNE, --

    tid.aar_maaned,
    tid.siste_dato_i_perioden,
    tid.aar,
    tid.pk_dim_tid as fk_dim_tid_mnd,
    row_number() over (partition by tid.aar_maaned, fagsak.fk_person1_kravhaver ,fagsak.saksnr
            order by fagsak.vedtakstidspunkt desc
        ) nr
  from {{ source ('fam_bb', 'fam_bb_fagsak_ord') }} fagsak
 
  join {{ source ('fam_bb', 'fam_bb_bidrags_periode_ord') }} periode
  on fagsak.pk_bb_fagsak = periode.fk_bb_fagsak
  --and periode.belop >= 0

  join tid
  on periode.periode_fra <= to_date(tid.aar_maaned||'01', 'yyyymmdd')
  and nvl(periode.periode_til, tid.siste_dato_i_perioden) >= tid.siste_dato_i_perioden

  where fagsak.behandlings_type not in ('ENDRING_MOTTAKER', 'OPPHØR', 'ALDERSOPPHØR')
  and trunc(fagsak.vedtakstidspunkt, 'dd') <= TO_DATE('{{ var ("max_vedtaksdato") }}', 'yyyymmdd')
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
      and trunc(fagsak.vedtakstidspunkt, 'dd') <= TO_DATE('{{ var ("max_vedtaksdato") }}', 'yyyymmdd')--Begrense max_vedtaksdato på dag nivå

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
        resultat, BIDRAGSEVNE, UNDERHOLDSKOSTNAD, SAMVAERSFRADRAG, NETTO_BARNETILLEGG_BP, NETTO_BARNETILLEGG_BM, 
        SAMVAERSKLASSE, BPS_ANDEL_UNDERHOLDSKOSTNAD, BPBOR_MED_ANDRE_VOKSNE,
        min(periode_fra_opphor) periode_fra_opphor
  from siste_opphør

  group by aar_maaned, siste_dato_i_perioden, aar, pk_bb_fagsak,fk_dim_tid_mnd,
        vedtaks_id, behandlings_type, vedtakstidspunkt, saksnr, fk_person1_kravhaver, fk_person1_mottaker,
        fk_person1_skyldner, pk_bb_bidrags_periode, periode_fra, periode_til, belop,
        resultat, BIDRAGSEVNE, UNDERHOLDSKOSTNAD, SAMVAERSFRADRAG, NETTO_BARNETILLEGG_BP, NETTO_BARNETILLEGG_BM, 
        SAMVAERSKLASSE, BPS_ANDEL_UNDERHOLDSKOSTNAD, BPBOR_MED_ANDRE_VOKSNE
),

inntekt AS (
    SELECT  
        FK_BB_BIDRAGS_PERIODE,
        TYPE_INNTEKT,
        BELOP,
        flagg,
        ROW_NUMBER() OVER (PARTITION BY FK_BB_BIDRAGS_PERIODE, flagg ORDER BY TYPE_INNTEKT) AS NR
    FROM FAM_BB_INNTEKTS_LISTE_ORD
),

inntekts_typer as (
SELECT
        FK_BB_BIDRAGS_PERIODE,
        MAX(CASE WHEN NR = 1 AND flagg = 'P' THEN TYPE_INNTEKT END) AS P_TYPE_INNTEKT_1,
        MAX(CASE WHEN NR = 1 AND flagg = 'P' THEN BELOP END) AS P_INNTEKT_1,
        MAX(CASE WHEN NR = 2 AND flagg = 'P' THEN TYPE_INNTEKT END) AS P_TYPE_INNTEKT_2,
        MAX(CASE WHEN NR = 2 AND flagg = 'P' THEN BELOP END) AS P_INNTEKT_2,
        MAX(CASE WHEN NR = 3 AND flagg = 'P' THEN TYPE_INNTEKT END) AS P_TYPE_INNTEKT_3,
        MAX(CASE WHEN NR = 3 AND flagg = 'P' THEN BELOP END) AS P_INNTEKT_3,
        
        MAX(CASE WHEN NR = 1 AND flagg = 'M' THEN TYPE_INNTEKT END) AS M_TYPE_INNTEKT_1,
        MAX(CASE WHEN NR = 1 AND flagg = 'M' THEN BELOP END) AS M_INNTEKT_1,
        MAX(CASE WHEN NR = 2 AND flagg = 'M' THEN TYPE_INNTEKT END) AS M_TYPE_INNTEKT_2,
        MAX(CASE WHEN NR = 2 AND flagg = 'M' THEN BELOP END) AS M_INNTEKT_2,
        MAX(CASE WHEN NR = 3 AND flagg = 'M' THEN TYPE_INNTEKT END) AS M_TYPE_INNTEKT_3,
        MAX(CASE WHEN NR = 3 AND flagg = 'M' THEN BELOP END) AS M_INNTEKT_3,
        MAX(CASE WHEN NR = 4 AND flagg = 'M' THEN TYPE_INNTEKT END) AS M_TYPE_INNTEKT_4,
        MAX(CASE WHEN NR = 4 AND flagg = 'M' THEN BELOP END) AS M_INNTEKT_4,
        MAX(CASE WHEN NR = 5 AND flagg = 'M' THEN TYPE_INNTEKT END) AS M_TYPE_INNTEKT_5,
        MAX(CASE WHEN NR = 5 AND flagg = 'M' THEN BELOP END) AS M_INNTEKT_5,

        SUM(CASE WHEN flagg = 'P' THEN BELOP ELSE 0 END) AS P_INNTEKT_TOTAL,
        MAX(CASE WHEN flagg = 'P' THEN NR ELSE 0 END) AS P_ANTALL_TYPER,

        SUM(CASE WHEN flagg = 'M' THEN BELOP ELSE 0 END) AS M_INNTEKT_TOTAL,
        MAX(CASE WHEN flagg = 'M' THEN NR ELSE 0 END) AS M_ANTALL_TYPER

    FROM INNTEKT
    GROUP BY FK_BB_BIDRAGS_PERIODE
),

periode_uten_opphort as (
 
  select aar_maaned, fk_person1_kravhaver, fk_person1_mottaker, fk_person1_skyldner, vedtakstidspunkt
        ,pk_bb_fagsak as fk_bb_fagsak, saksnr
        ,vedtaks_id, behandlings_type, pk_bb_bidrags_periode as fk_bb_bidrags_periode
        ,periode_fra, periode_til, belop, resultat
        ,periode_fra_opphor, aar
        ,BIDRAGSEVNE, UNDERHOLDSKOSTNAD, SAMVAERSFRADRAG, NETTO_BARNETILLEGG_BP, NETTO_BARNETILLEGG_BM,
        SAMVAERSKLASSE, BPS_ANDEL_UNDERHOLDSKOSTNAD, BPBOR_MED_ANDRE_VOKSNE
        --,TO_DATE(TO_CHAR(LAST_DAY(SYSDATE), 'YYYYMMDD'), 'YYYYMMDD') MAX_VEDTAKSDATO --Input max_vedtaksdato
        ,TO_DATE('{{ var ("max_vedtaksdato") }}', 'yyyymmdd') MAX_VEDTAKSDATO
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
        
        ,inntekts_typer.P_TYPE_INNTEKT_1,inntekts_typer.P_inntekt_1
        ,inntekts_typer.P_TYPE_INNTEKT_2,inntekts_typer.P_inntekt_2,inntekts_typer.P_TYPE_INNTEKT_3,inntekts_typer.P_inntekt_3
        ,inntekts_typer.P_INNTEKT_TOTAL, inntekts_typer.P_ANTALL_TYPER
        
        ,inntekts_typer.M_TYPE_INNTEKT_1
        ,inntekts_typer.M_inntekt_1, inntekts_typer.M_TYPE_INNTEKT_2, inntekts_typer.M_inntekt_2
        ,inntekts_typer.M_type_inntekt_3, inntekts_typer.M_inntekt_3, inntekts_typer.M_type_inntekt_4
        ,inntekts_typer.M_inntekt_4,inntekts_typer.M_type_inntekt_5, inntekts_typer.M_inntekt_5
        ,inntekts_typer.M_INNTEKT_TOTAL, inntekts_typer.M_ANTALL_TYPER
        ,'{{ var ("gyldig_flagg") }}' as gyldig_flagg --Input gyldig_flagg
        ,localtimestamp AS lastet_dato
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

  on vedtak.pK_BB_BIDRAGS_PERIODE = inntekts_typer.FK_BB_BIDRAGS_PERIODE
 
  where siste_dato_i_perioden < nvl(periode_fra_opphor, siste_dato_i_perioden+1)
)

select 
    * 
from periode_uten_opphort
