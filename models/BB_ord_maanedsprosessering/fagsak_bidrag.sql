{{ config(
    materialized='table'
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
    fagsak.behandlings_type,
    fagsak.vedtakstidspunkt,
    fagsak.saksnr,
    fagsak.innkreving_flagg,
    fagsak.STONADSTYPE,
    fagsak.fk_person1_kravhaver,
    fagsak.fk_person1_mottaker,
    fagsak.fk_person1_skyldner, 

    periode.pk_bb_bidrags_periode, 
    periode.periode_fra,
    periode.periode_til,
    periode.belop,
    periode.netto_tilsynsutgift,
    periode.faktisk_tilsynsutgift,
    periode.resultat,
    periode.BIDRAGSEVNE, 
    periode.UNDERHOLDSKOSTNAD, 
    periode.SAMVAERSFRADRAG, 
    periode.NETTO_BARNETILLEGG_BP, 
    periode.NETTO_BARNETILLEGG_BM, 
    periode.SAMVAERSKLASSE, 
    periode.BPS_ANDEL_UNDERHOLDSKOSTNAD, 
    periode.BPBOR_MED_ANDRE_VOKSNE, 
    periode.valutakode,

    resultat.RESULTAT_TIL resultat_tekst,

    tid.aar_maaned,
    tid.siste_dato_i_perioden,
    tid.aar,
    tid.pk_dim_tid as fk_dim_tid_mnd,
    row_number() over (partition by tid.aar_maaned, decode(fagsak.fk_person1_kravhaver,-1,ident_krav.fk_person1,fagsak.fk_person1_kravhaver) ,fagsak.saksnr, fagsak.stonadstype
            order by fagsak.vedtakstidspunkt desc, periode.belop desc) nr,
    min(fagsak.vedtakstidspunkt) over (partition by tid.aar_maaned, fagsak.fk_person1_kravhaver ,fagsak.saksnr) forste_vedtakstidspunkt--,
   -- last_value(innkreving_flagg) over (partition by tid.aar_maaned, decode(fagsak.fk_person1_kravhaver,-1,ident_krav.fk_person1,fagsak.fk_person1_kravhaver) ,fagsak.saksnr, fagsak.stonadstype
    --        order by fagsak.vedtakstidspunkt desc, periode.belop desc) t
    

  from {{ source ('fam_bb', 'fam_bb_fagsak_ord') }} fagsak
 
  join {{ source ('fam_bb', 'fam_bb_bidrags_periode_ord') }} periode
  on fagsak.pk_bb_fagsak = periode.fk_bb_fagsak
  --and periode.belop >= 0

  join tid
  on periode.periode_fra <= to_date(tid.aar_maaned||'01', 'yyyymmdd')
  and nvl(periode.periode_til, tid.siste_dato_i_perioden) >= tid.siste_dato_i_perioden

  left join {{ source ('fam_bb', 'FAM_BB_BIDRAG_RESULTAT_MAPPING') }}  resultat
  on periode.resultat = resultat.resultat_fra

  left join {{ source ('dt_person_arena', 'ident_off_id_til_fk_person1') }} ident_krav
  on fagsak.fnr_kravhaver = ident_krav.off_id
  and fagsak.fk_person1_kravhaver = -1
  and tid.siste_dato_i_perioden between ident_krav.gyldig_fra_dato and ident_krav.gyldig_til_dato

  where fagsak.behandlings_type not in ('ENDRING_MOTTAKER', 'OPPHØR', 'ALDERSOPPHØR')
  and trunc(fagsak.vedtakstidspunkt, 'dd') <= TO_DATE('{{ var ("max_vedtaksdato") }}', 'yyyymmdd')
  and fagsak.INNKREVING_FLAGG = '1'
),

fagsak_fylt_verdier as (
  select
    f.pk_bb_fagsak, f.behandlings_type, f.saksnr, f.innkreving_flagg, f.stonadstype,
    f.fk_person1_kravhaver, f.fk_person1_mottaker, f.fk_person1_skyldner, f.pk_bb_bidrags_periode,
    f.periode_fra, f.periode_til, f.belop, f.valutakode, f.aar_maaned, f.siste_dato_i_perioden,
    f.aar, f.fk_dim_tid_mnd, f.nr, f.vedtakstidspunkt, f.vedtaks_id,forste_vedtakstidspunkt,

            COALESCE(f.netto_tilsynsutgift, LAST_VALUE(f.netto_tilsynsutgift IGNORE NULLS) OVER ( PARTITION BY f.fk_person1_kravhaver, f.saksnr, f.stonadstype ORDER BY f.vedtakstidspunkt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) ) AS netto_tilsynsutgift,
            COALESCE(f.faktisk_tilsynsutgift, LAST_VALUE(f.faktisk_tilsynsutgift IGNORE NULLS) OVER ( PARTITION BY f.fk_person1_kravhaver, f.saksnr, f.stonadstype ORDER BY f.vedtakstidspunkt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) ) AS faktisk_tilsynsutgift,
            COALESCE(f.resultat, LAST_VALUE(f.resultat IGNORE NULLS) OVER ( PARTITION BY f.fk_person1_kravhaver, f.saksnr, f.stonadstype ORDER BY f.vedtakstidspunkt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) ) AS resultat,
            COALESCE(f.resultat_tekst, f.resultat) resultat_tekst,
            COALESCE(f.BIDRAGSEVNE, LAST_VALUE(f.BIDRAGSEVNE IGNORE NULLS) OVER ( PARTITION BY f.fk_person1_kravhaver, f.saksnr, f.stonadstype ORDER BY f.vedtakstidspunkt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) ) AS BIDRAGSEVNE,
            COALESCE(f.UNDERHOLDSKOSTNAD, LAST_VALUE(f.UNDERHOLDSKOSTNAD IGNORE NULLS) OVER ( PARTITION BY f.fk_person1_kravhaver, f.saksnr, f.stonadstype ORDER BY f.vedtakstidspunkt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) ) AS UNDERHOLDSKOSTNAD,
            COALESCE(f.SAMVAERSFRADRAG, LAST_VALUE(f.SAMVAERSFRADRAG IGNORE NULLS) OVER ( PARTITION BY f.fk_person1_kravhaver, f.saksnr, f.stonadstype ORDER BY f.vedtakstidspunkt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) ) AS SAMVAERSFRADRAG,
            COALESCE(f.NETTO_BARNETILLEGG_BP, LAST_VALUE(f.NETTO_BARNETILLEGG_BP IGNORE NULLS) OVER ( PARTITION BY f.fk_person1_kravhaver, f.saksnr, f.stonadstype ORDER BY f.vedtakstidspunkt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) ) AS NETTO_BARNETILLEGG_BP,
            COALESCE(f.NETTO_BARNETILLEGG_BM, LAST_VALUE(f.NETTO_BARNETILLEGG_BM IGNORE NULLS) OVER ( PARTITION BY f.fk_person1_kravhaver, f.saksnr, f.stonadstype ORDER BY f.vedtakstidspunkt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )) AS NETTO_BARNETILLEGG_BM,
            COALESCE(f.SAMVAERSKLASSE, LAST_VALUE(f.SAMVAERSKLASSE IGNORE NULLS) OVER ( PARTITION BY f.fk_person1_kravhaver, f.saksnr, f.stonadstype ORDER BY f.vedtakstidspunkt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )) AS SAMVAERSKLASSE,
            COALESCE(f.BPS_ANDEL_UNDERHOLDSKOSTNAD, LAST_VALUE(f.BPS_ANDEL_UNDERHOLDSKOSTNAD IGNORE NULLS) OVER ( PARTITION BY f.fk_person1_kravhaver, f.saksnr, f.stonadstype ORDER BY f.vedtakstidspunkt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS BPS_ANDEL_UNDERHOLDSKOSTNAD,
            COALESCE(f.BPBOR_MED_ANDRE_VOKSNE, LAST_VALUE(f.BPBOR_MED_ANDRE_VOKSNE IGNORE NULLS) OVER (PARTITION BY f.fk_person1_kravhaver, f.saksnr, f.stonadstype ORDER BY f.vedtakstidspunkt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS BPBOR_MED_ANDRE_VOKSNE,     
            LAST_VALUE(CASE WHEN f.underholdskostnad IS NOT NULL THEN f.vedtaks_id END IGNORE NULLS) OVER (PARTITION BY f.fk_person1_kravhaver, f.saksnr, f.stonadstype ORDER BY f.vedtakstidspunkt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS SISTE_KOMPLETT_VEDTAK,
            LAST_VALUE(CASE WHEN f.underholdskostnad IS NOT NULL THEN f.vedtakstidspunkt END IGNORE NULLS) OVER (PARTITION BY f.fk_person1_kravhaver, f.saksnr, f.stonadstype ORDER BY f.vedtakstidspunkt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS SISTE_KOMPLETT_VEDTAKSTIDSPUNKT

  from fagsak f
)

select * from fagsak_fylt_verdier