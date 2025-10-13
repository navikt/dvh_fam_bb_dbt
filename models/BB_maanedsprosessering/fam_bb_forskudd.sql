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
        fagsak.fk_person1_kravhaver, 
        fagsak.vedtaks_id, 
        fagsak.saksnr, 
        fagsak.behandlings_type,
        fagsak.vedtakstidspunkt, 
        fagsak.fk_person1_mottaker,
        periode.pk_bb_forskudds_periode, 
        periode.periode_fra, 
        periode.periode_til, 
        periode.belop,
        periode.resultat, 
        periode.barnets_alders_gruppe,
        tid.aar_maaned, 
        tid.siste_dato_i_perioden, 
        tid.aar, 
        tid.pk_dim_tid as fk_dim_tid_mnd,
        row_number() over (partition by tid.aar_maaned, fagsak.fk_person1_kravhaver ,fagsak.saksnr 
                           order by fagsak.vedtakstidspunkt desc
                          ) nr,
        min(fagsak.vedtakstidspunkt) over (partition by tid.aar_maaned, fagsak.fk_person1_kravhaver ,fagsak.saksnr) forste_vedtakstidspunkt           
    from {{ source ('fam_bb_forskudd_maaned', 'fam_bb_fagsak') }} fagsak
 
    join {{ source ('fam_bb_forskudd_maaned', 'fam_bb_forskudds_periode') }} periode
    on fagsak.pk_bb_fagsak = periode.fk_bb_fagsak
    and periode.belop > 0
 
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
    from {{ source ('fam_bb_forskudd_maaned', 'fam_bb_fagsak') }} fagsak 

    join {{ source ('fam_bb_forskudd_maaned', 'fam_bb_forskudds_periode') }} periode
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
    select aar_maaned, siste_dato_i_perioden, aar, fk_dim_tid_mnd
          ,fk_person1_kravhaver, fk_person1_mottaker
          ,vedtakstidspunkt, pk_bb_fagsak, saksnr, vedtaks_id, behandlings_type
          ,pk_bb_forskudds_periode, periode_fra, periode_til, belop
          ,resultat, barnets_alders_gruppe, antall_barn_i_egen_husstand, sivilstand, barn_bor_med_bm
          ,min(periode_fra_opphor) periode_fra_opphor
          ,forste_vedtakstidspunkt
    from siste_opphør

    group by aar_maaned, siste_dato_i_perioden, aar, fk_dim_tid_mnd
            ,fk_person1_kravhaver, fk_person1_mottaker
            ,vedtakstidspunkt, pk_bb_fagsak, saksnr, vedtaks_id, behandlings_type
            ,pk_bb_forskudds_periode, periode_fra, periode_til, belop
            ,resultat, barnets_alders_gruppe, antall_barn_i_egen_husstand, sivilstand, barn_bor_med_bm
            ,forste_vedtakstidspunkt
),
--Hent ut inntektsliste fra siste versjon som har den. inntekt_versjon=1
--Dette kan være en eldre versjon enn siste, og derfor blir fagsak koblet opp, ikke siste. Fagsak har alle versjoner.
siste_inntekt as (
    select fagsak.aar_maaned, fagsak.fk_person1_kravhaver ,fagsak.saksnr
          ,max(inntekt.fk_bb_forskudds_periode) keep (dense_rank first order by fagsak.vedtakstidspunkt desc) inntekt_fk_bb_forskudds_periode
          ,max(fagsak.resultat) keep (dense_rank first order by fagsak.vedtakstidspunkt desc) siste_inntekt_resultat
          ,max(fagsak.barnets_alders_gruppe) keep (dense_rank first order by fagsak.vedtakstidspunkt desc) siste_inntekt_barnets_alders_gruppe
          ,max(fagsak.antall_barn_i_egen_husstand) keep (dense_rank first order by fagsak.vedtakstidspunkt desc) siste_inntekt_antall_barn_i_egen_husstand
          ,max(fagsak.sivilstand) keep (dense_rank first order by fagsak.vedtakstidspunkt desc) siste_inntekt_sivilstand
          ,max(fagsak.barn_bor_med_bm) keep (dense_rank first order by fagsak.vedtakstidspunkt desc) siste_inntekt_barn_bor_med_bm
          ,max(fagsak.vedtakstidspunkt) siste_inntekt_vedtakstidspunkt
    from fagsak
    join {{ source ('fam_bb_forskudd_maaned', 'fam_bb_inntekt') }} inntekt
    on fagsak.pk_bb_forskudds_periode = inntekt.fk_bb_forskudds_periode
    group by fagsak.aar_maaned, fagsak.fk_person1_kravhaver ,fagsak.saksnr
)
,
inntekt as (
    select siste_inntekt.*
          ,inntekt.type_inntekt, inntekt.belop
          ,row_number() over (partition by inntekt.fk_bb_forskudds_periode order by inntekt.type_inntekt desc) nr --Inntektstype-sortering fra en bestemt versjon, og UTVIDET_BARNETRYGD0 har høy prioritering
    from siste_inntekt
    join fam_bb_inntekt inntekt
    on siste_inntekt.inntekt_fk_bb_forskudds_periode = inntekt.fk_bb_forskudds_periode
)
,
--Pivotere ut inntekter til en linje per aar_maaned, fk_person1_kravhaver ,saksnr
inntekts_typer as (
    select  
        aar_maaned, fk_person1_kravhaver, saksnr, siste_inntekt_vedtakstidspunkt
       ,siste_inntekt_resultat, siste_inntekt_barnets_alders_gruppe
       ,siste_inntekt_antall_barn_i_egen_husstand
       ,siste_inntekt_sivilstand, siste_inntekt_barn_bor_med_bm,
        max(case when nr=1 then type_inntekt end) type_inntekt_1,
        max(case when nr=1 then belop end) inntekt_1,
        max(case when nr=2 then type_inntekt end) type_inntekt_2,
        max(case when nr=2 then belop end) inntekt_2,
        max(case when nr=3 then type_inntekt end) type_inntekt_3,
        max(case when nr=3 then belop end) inntekt_3,
        max(case when nr=4 then type_inntekt end) type_inntekt_4,
        max(case when nr=4 then belop end) inntekt_4,
        sum(belop) inntekt_total,
        count(distinct type_inntekt) antall_inntekts_typer
    from inntekt
    group by aar_maaned, fk_person1_kravhaver, saksnr, siste_inntekt_vedtakstidspunkt
            ,siste_inntekt_resultat, siste_inntekt_barnets_alders_gruppe
            ,siste_inntekt_antall_barn_i_egen_husstand
            ,siste_inntekt_sivilstand, siste_inntekt_barn_bor_med_bm
)
,

periode_uten_opphort as (
 
    select aar_maaned, fk_person1_kravhaver, fk_person1_mottaker, vedtakstidspunkt
          ,pk_bb_fagsak as fk_bb_fagsak, saksnr
          ,vedtaks_id, behandlings_type, pk_bb_forskudds_periode as fk_bb_forskudds_periode
          ,periode_fra, periode_til, belop
          --Hent ut resultat, barnets_alders_gruppe, antall_barn_i_egen_husstand, sivilstand, barn_bor_med_bm
          --fra siste versjon av inntekt. Hvis inntekt ikke finnes, returneres disse feltene fra siste versjon av vedtaket
          ,inntekts_typer.siste_inntekt_vedtakstidspunkt
          ,nvl(inntekts_typer.siste_inntekt_resultat, vedtak.resultat) as resultat
          ,nvl(inntekts_typer.siste_inntekt_barnets_alders_gruppe, vedtak.barnets_alders_gruppe) as barnets_alders_gruppe
          ,nvl(inntekts_typer.siste_inntekt_antall_barn_i_egen_husstand, vedtak.antall_barn_i_egen_husstand) as antall_barn_i_egen_husstand
          ,nvl(inntekts_typer.siste_inntekt_sivilstand, vedtak.sivilstand) as sivilstand
          ,nvl(inntekts_typer.siste_inntekt_barn_bor_med_bm, vedtak.barn_bor_med_bm) as barn_bor_med_bm
          --
          ,periode_fra_opphor, aar
          --,TO_DATE(TO_CHAR(LAST_DAY(SYSDATE), 'YYYYMMDD'), 'YYYYMMDD') MAX_VEDTAKSDATO --Input max_vedtaksdato
          ,to_date('{{ var ("max_vedtaksdato") }}', 'yyyymmdd') max_vedtaksdato
          ,fk_dim_tid_mnd
          ,'{{ var ("periode_type") }}' periode_type --Input periode_type
          ,dim_kravhaver.pk_dim_person as fk_dim_person_kravhaver
          ,floor(months_between(vedtak.siste_dato_i_perioden, dim_kravhaver.fodt_dato)/12) alder_kravhaver
          ,case 
              when dim_kravhaver.kjonn_nr = 1 then 'M'
              when dim_kravhaver.kjonn_nr = 0 then 'K'
              else 'U'
          end kjonn_kravhaver 
          ,dim_mottaker.pk_dim_person as fk_dim_person_mottaker
          ,dim_mottaker.bosted_kommune_nr as bosted_kommune_nr_mottaker
          ,dim_mottaker.fk_dim_land_statsborgerskap as fk_dim_land_statsborgerskap_mottaker
          ,dim_mottaker.fk_dim_geografi_bosted as fk_dim_geografi_bosted_mottaker
          ,floor(months_between(vedtak.siste_dato_i_perioden, dim_mottaker.fodt_dato)/12) alder_mottaker
          ,inntekts_typer.inntekt_total, inntekts_typer.antall_inttekts_typer, inntekts_typer.type_inntekt_1
          ,inntekts_typer.inntekt_1, inntekts_typer.type_inntekt_2, inntekts_typer.inntekt_2
          ,inntekts_typer.type_inntekt_3, inntekts_typer.inntekt_3, inntekts_typer.type_inntekt_4
          ,inntekts_typer.inntekt_4
          ,'{{ var ("gyldig_flagg") }}' as gyldig_flagg --Input gyldig_flagg
          ,localtimestamp as lastet_dato
          ,forste_vedtakstidspunkt
    from opphor_hvis_finnes vedtak
   
    left join {{ source ('dt_person', 'dim_person') }} dim_kravhaver
    on dim_kravhaver.fk_person1 = vedtak.fk_person1_kravhaver
    and vedtak.fk_person1_kravhaver != -1
    and vedtak.siste_dato_i_perioden between dim_kravhaver.gyldig_fra_dato and dim_kravhaver.gyldig_til_dato
   
    left join {{ source ('dt_person', 'dim_person') }} dim_mottaker
    on dim_mottaker.fk_person1 = vedtak.fk_person1_mottaker
    and vedtak.fk_person1_mottaker != -1
    and vedtak.siste_dato_i_perioden between dim_mottaker.gyldig_fra_dato and dim_mottaker.gyldig_til_dato
  
    left join inntekts_typer  
    on vedtak.aar_maaned = inntekts_typer.aar_maaned
    and vedtak.fk_person1_kravhaver = inntekts_typer.fk_person1_kravhaver
    and vedtak.saksnr = inntekts_typer.saksnr
   
    where siste_dato_i_perioden < nvl(periode_fra_opphor, siste_dato_i_perioden+1)
)

select 
    aar_maaned, fk_person1_kravhaver, fk_person1_mottaker, vedtakstidspunkt, fk_bb_fagsak, vedtaks_id
   ,fk_bb_forskudds_periode, periode_fra, periode_til, belop, periode_fra_opphor, aar, max_vedtaksdato
   ,fk_dim_tid_mnd, periode_type, fk_dim_person_kravhaver, alder_kravhaver, fk_dim_person_mottaker
   ,bosted_kommune_nr_mottaker, fk_dim_land_statsborgerskap_mottaker, fk_dim_geografi_bosted_mottaker
   ,alder_mottaker, inntekt_total, antall_inntekts_typer, gyldig_flagg, lastet_dato, inntekt_1, inntekt_2, inntekt_3, inntekt_4
   ,saksnr, behandlings_type, resultat, barnets_alders_gruppe, type_inntekt_1, type_inntekt_2, type_inntekt_3, type_inntekt_4
   ,kjonn_kravhaver, antall_barn_i_egen_husstand, sivilstand, barn_bor_med_bm
   ,siste_inntekt_vedtakstidspunkt, forste_vedtakstidspunkt
from periode_uten_opphort