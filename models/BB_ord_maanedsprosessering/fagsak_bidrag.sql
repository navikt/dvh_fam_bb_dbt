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
    fagsak.behandlings_type, --
    fagsak.vedtakstidspunkt,
    fagsak.saksnr,
    fagsak.innkreving_flagg,
    fagsak.STONADSTYPE,
    fagsak.fk_person1_kravhaver,
    fagsak.fk_person1_mottaker,
    fagsak.fk_person1_skyldner, --

    periode.pk_bb_bidrags_periode, --
    periode.periode_fra,
    periode.periode_til,
    periode.belop,
    periode.netto_tilsynsutgift,
    periode.faktisk_tilsynsutgift,
    periode.resultat,
    periode.BIDRAGSEVNE, --
    periode.UNDERHOLDSKOSTNAD, --
    periode.SAMVAERSFRADRAG, --
    periode.NETTO_BARNETILLEGG_BP, --
    periode.NETTO_BARNETILLEGG_BM, --
    periode.SAMVAERSKLASSE, --
    periode.BPS_ANDEL_UNDERHOLDSKOSTNAD, --
    periode.BPBOR_MED_ANDRE_VOKSNE, --
    periode.valutakode,

    tid.aar_maaned,
    tid.siste_dato_i_perioden,
    tid.aar,
    tid.pk_dim_tid as fk_dim_tid_mnd,
    row_number() over (partition by tid.aar_maaned, decode(fagsak.fk_person1_kravhaver,-1,ident_krav.fk_person1,fagsak.fk_person1_kravhaver) ,fagsak.saksnr, fagsak.stonadstype
            order by fagsak.vedtakstidspunkt desc, periode.belop desc) nr,
    last_value(innkreving_flagg) over (partition by tid.aar_maaned, decode(fagsak.fk_person1_kravhaver,-1,ident_krav.fk_person1,fagsak.fk_person1_kravhaver) ,fagsak.saksnr, fagsak.stonadstype
            order by fagsak.vedtakstidspunkt desc, periode.belop desc) t

  from {{ source ('fam_bb', 'fam_bb_fagsak_ord') }} fagsak
 
  join {{ source ('fam_bb', 'fam_bb_bidrags_periode_ord') }} periode
  on fagsak.pk_bb_fagsak = periode.fk_bb_fagsak
  --and periode.belop >= 0

  join tid
  on periode.periode_fra <= to_date(tid.aar_maaned||'01', 'yyyymmdd')
  and nvl(periode.periode_til, tid.siste_dato_i_perioden) >= tid.siste_dato_i_perioden

  left join dt_person.ident_off_id_til_fk_person1 ident_krav
  on fagsak.fnr_kravhaver = ident_krav.off_id
  and fagsak.fk_person1_kravhaver = -1
  and tid.siste_dato_i_perioden between ident_krav.gyldig_fra_dato and ident_krav.gyldig_til_dato

  where fagsak.behandlings_type not in ('ENDRING_MOTTAKER', 'OPPHØR', 'ALDERSOPPHØR')
  and trunc(fagsak.vedtakstidspunkt, 'dd') <= TO_DATE('{{ var ("max_vedtaksdato") }}', 'yyyymmdd')
  --and fagsak.INNKREVING_FLAGG = '1'
)

select * from fagsak