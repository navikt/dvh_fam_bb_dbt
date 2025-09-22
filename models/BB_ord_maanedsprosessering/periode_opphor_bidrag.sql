with opphor_fra as (

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
    from ref('fagsak_bidrag') siste
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
)

select * 
from opphor_hvis_finnes