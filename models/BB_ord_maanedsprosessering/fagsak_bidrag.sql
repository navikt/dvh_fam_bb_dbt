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
    row_number() over (partition by tid.aar_maaned, fagsak.fk_person1_kravhaver ,fagsak.saksnr, fagsak.fk_person1_mottaker
            order by fagsak.vedtakstidspunkt desc, periode.belop desc) nr
  from {{ source ('fam_bb', 'fam_bb_fagsak_ord') }} fagsak
 
  join {{ source ('fam_bb', 'fam_bb_bidrags_periode_ord') }} periode
  on fagsak.pk_bb_fagsak = periode.fk_bb_fagsak
  --and periode.belop >= 0

  join tid
  on periode.periode_fra <= to_date(tid.aar_maaned||'01', 'yyyymmdd')
  and nvl(periode.periode_til, tid.siste_dato_i_perioden) >= tid.siste_dato_i_perioden

  where fagsak.behandlings_type not in ('ENDRING_MOTTAKER', 'OPPHØR', 'ALDERSOPPHØR')
  and trunc(fagsak.vedtakstidspunkt, 'dd') <= to_date('{{ var ("max_vedtaksdato") }}', 'yyyymmdd')
)

select * from fagsak

