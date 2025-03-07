{{
    config(
        materialized='incremental'
    )
}}

with tid as
(
  select aar_maaned, siste_dato_i_perioden, aar, pk_dim_tid
  from dt_kodeverk.dim_tid
  where gyldig_flagg = 1
  and dim_nivaa = 3
  and aar_maaned between 202401 and 202412--Begrense hvilken måned statitstikk gjelder
  --and aar_maaned between 200801 and 200812
)
,
fagsak as
(
  select fagsak.pk_bb_fagsak, fagsak.fk_person1_kravhaver, fagsak.vedtaks_id, fagsak.saksnr, fagsak.behandlings_type
        ,fagsak.vedtakstidspunkt, fagsak.fk_person1_mottaker
        ,periode.pk_bb_forskudds_periode, periode.periode_fra, periode.periode_til, periode.belop
        ,periode.resultat, periode.barnets_alders_gruppe
        ,tid.aar_maaned, tid.siste_dato_i_perioden, tid.aar, tid.pk_dim_tid as fk_dim_tid_mnd
        --,to_date(tid.aar_maaned||'01','yyyymmdd') dato_utbet_fom, dato_utbet.siste_dato_i_perioden dato_utbet_tom
        ,row_number() over (partition by tid.aar_maaned, fagsak.fk_person1_kravhaver order by fagsak.vedtakstidspunkt desc) nr
  from fam_bb_fagsak fagsak

  join fam_bb_forskudds_periode periode
  on fagsak.pk_bb_fagsak = periode.fk_bb_fagsak
  and periode.belop > 0

  join tid
  on periode.periode_fra <= to_date(tid.aar_maaned||'01', 'yyyymmdd')
  and nvl(periode.periode_til, tid.siste_dato_i_perioden) >= tid.siste_dato_i_perioden

  where fagsak.behandlings_type not in ('ENDRING_MOTTAKER', 'OPPHØR', 'ALDERSOPPHØR')
  and trunc(fagsak.vedtakstidspunkt, 'dd') <= to_date('20250228','yyyymmdd')--Begrense max_vedtaksdato på dag nivå
  --and fagsak.vedtakstidspunkt <= '22.09.2008 07.34.04,000000000'
)
--select * from fagsak where fk_person1_kravhaver = 156222618; order by dato_utbet_fom;
,
siste as
(
  select *
  from fagsak
  where nr = 1
)
--select * from siste where fk_person1_kravhaver = 1236727771;
,
--Finne ut første opphørt periode hvis finnes
opphor_hvis_finnes as
(
  select aar_maaned, siste_dato_i_perioden, aar, fk_dim_tid_mnd
        ,fk_person1_kravhaver, fk_person1_mottaker
        ,vedtakstidspunkt, pk_bb_fagsak, saksnr, vedtaks_id, behandlings_type
        ,pk_bb_forskudds_periode, periode_fra, periode_til, belop
        ,resultat, barnets_alders_gruppe
        ,min(periode_fra_opphor) periode_fra_opphor
  from
  (
    select siste.*
          ,opphor_fra.periode_fra_opphor
    from siste

    left join
    (
      select fagsak.fk_person1_kravhaver, fagsak.saksnr, fagsak.vedtakstidspunkt
            ,min(periode.periode_fra) periode_fra_opphor
      from fam_bb_fagsak fagsak

      join fam_bb_forskudds_periode periode
      on fagsak.pk_bb_fagsak = periode.fk_bb_fagsak
      and periode.belop is null --Opphørt versjon

      where fagsak.behandlings_type not in ('ENDRING_MOTTAKER')
      and trunc(fagsak.vedtakstidspunkt, 'dd') <= to_date('20250228','yyyymmdd')--Begrense max_vedtaksdato på dag nivå
      --and fagsak.vedtakstidspunkt <= '22.09.2008 07.34.04,000000000'

      group by fagsak.fk_person1_kravhaver, fagsak.saksnr, fagsak.vedtakstidspunkt
    ) opphor_fra
    on opphor_fra.fk_person1_kravhaver = siste.fk_person1_kravhaver
    and opphor_fra.saksnr = siste.saksnr
    and opphor_fra.vedtakstidspunkt > siste.vedtakstidspunkt
  )
  group by aar_maaned, siste_dato_i_perioden, aar, fk_dim_tid_mnd
          ,fk_person1_kravhaver, fk_person1_mottaker
          ,vedtakstidspunkt, pk_bb_fagsak, saksnr, vedtaks_id, behandlings_type
          ,pk_bb_forskudds_periode, periode_fra, periode_til, belop
          ,resultat, barnets_alders_gruppe
)
--select * from opphor_hvis_finnes where fk_person1_kravhaver = 1883921514 order by aar_maaned;
,
--Ta med perioder før opphørt hvis det finnes
periode_uten_opphort as
(
  select /*+ parallel(64) */ aar_maaned, fk_person1_kravhaver, fk_person1_mottaker, vedtakstidspunkt
        ,pk_bb_fagsak as fk_bb_fagsak, saksnr
        ,vedtaks_id, behandlings_type, pk_bb_forskudds_periode as fk_bb_forskudds_periode
        ,periode_fra, periode_til, belop, resultat
        ,barnets_alders_gruppe, periode_fra_opphor, aar
        ,to_date('20250228','yyyymmdd') max_vedtaksdato --Input max_vedtaksdato
        ,fk_dim_tid_mnd
        ,'M' periode_type --Input periode_type
        ,dim_kravhaver.pk_dim_person as fk_dim_person_kravhaver
        ,floor(months_between(vedtak.siste_dato_i_perioden, dim_kravhaver.fodt_dato)/12) alder_kravhaver
        ,dim_kravhaver.kjonn_nr kjonn_kravhaver
        ,dim_mottaker.pk_dim_person as fk_dim_person_mottaker
        ,dim_mottaker.bosted_kommune_nr as bosted_kommune_nr_mottaker
        ,dim_mottaker.fk_dim_land_statsborgerskap as fk_dim_land_statsborgerskap_mottaker
        ,dim_mottaker.fk_dim_geografi_bosted as fk_dim_geografi_bosted_mottaker
        ,floor(months_between(vedtak.siste_dato_i_perioden, dim_mottaker.fodt_dato)/12) alder_mottaker
        ,inntekt.inntekt_total, inntekt.antall_inntekts_typer
        ,1 as gyldig_flagg --Input gyldig_flagg
  from opphor_hvis_finnes vedtak

  left join dt_person.dim_person dim_kravhaver
  on dim_kravhaver.fk_person1 = vedtak.fk_person1_kravhaver
  and vedtak.fk_person1_kravhaver != -1
  and vedtak.siste_dato_i_perioden between dim_kravhaver.gyldig_fra_dato and dim_kravhaver.gyldig_til_dato

  left join dt_person.dim_person dim_mottaker
  on dim_mottaker.fk_person1 = vedtak.fk_person1_mottaker
  and vedtak.fk_person1_mottaker != -1
  and vedtak.siste_dato_i_perioden between dim_mottaker.gyldig_fra_dato and dim_mottaker.gyldig_til_dato

  left join
  (
    select fk_bb_forskudds_periode
          ,sum(belop) inntekt_total
          ,count(distinct type_inntekt) antall_inntekts_typer
    from fam_bb_inntekt
    group by fk_bb_forskudds_periode
  ) inntekt
  on vedtak.pk_bb_forskudds_periode = inntekt.fk_bb_forskudds_periode

  where siste_dato_i_perioden < nvl(periode_fra_opphor, siste_dato_i_perioden+1)
)
select * from periode_uten_opphort

--Last opp kun ny periode siden siste periode fra tabellen
{% if is_incremental() %}

where aar_maaned > (select coalesce(max(aar_maaned), 190000) from {{ this }})

{% endif %}