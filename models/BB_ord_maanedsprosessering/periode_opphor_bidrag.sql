with opphor_fra as (

      select fagsak.fk_person1_kravhaver, fagsak.saksnr, fagsak.vedtakstidspunkt
      ,fagsak.stonadstype
      ,min(periode.periode_fra) periode_fra_opphor
      from {{ source ('fam_bb', 'fam_bb_fagsak_ord') }} fagsak 

      join {{ source ('fam_bb', 'fam_bb_bidrags_periode_ord') }} periode
      on fagsak.pk_bb_fagsak = periode.fk_bb_fagsak
      and (periode.belop is null OR periode.belop = 0)

      where fagsak.behandlings_type not in ('ENDRING_MOTTAKER')
      and trunc(fagsak.vedtakstidspunkt, 'dd') <= TO_DATE('{{ var ("max_vedtaksdato") }}', 'yyyymmdd')--Begrense max_vedtaksdato på dag nivå

      group by fagsak.fk_person1_kravhaver, fagsak.saksnr, fagsak.vedtakstidspunkt, fagsak.stonadstype
),

siste as (
      select *
      from {{ ref('fagsak_bidrag') }}
      where nr = 1
      --and INNKREVING_FLAGG = '1'
),

siste_opphør as (
    select siste.*
        ,opphor_fra.periode_fra_opphor
    from siste
    left join opphor_fra
    on opphor_fra.fk_person1_kravhaver = siste.fk_person1_kravhaver
    and opphor_fra.saksnr = siste.saksnr

    and opphor_fra.vedtakstidspunkt > siste.vedtakstidspunkt
    and opphor_fra.stonadstype = siste.stonadstype
),

opphor_hvis_finnes as
(
  select aar_maaned, siste_dato_i_perioden, aar, pk_bb_fagsak,fk_dim_tid_mnd,innkreving_flagg,stonadstype,
        vedtaks_id, behandlings_type, vedtakstidspunkt, saksnr, fk_person1_kravhaver, fk_person1_mottaker,
        fk_person1_skyldner, pk_bb_bidrags_periode, periode_fra, periode_til, belop,netto_tilsynsutgift,faktisk_tilsynsutgift,
        resultat,resultat_tekst, BIDRAGSEVNE, UNDERHOLDSKOSTNAD, SAMVAERSFRADRAG, NETTO_BARNETILLEGG_BP, NETTO_BARNETILLEGG_BM, 
        SAMVAERSKLASSE, BPS_ANDEL_UNDERHOLDSKOSTNAD, BPBOR_MED_ANDRE_VOKSNE,valutakode,forste_vedtakstidspunkt,
        SISTE_KOMPLETT_VEDTAK,SISTE_KOMPLETT_VEDTAKSTIDSPUNKT,
        min(periode_fra_opphor) periode_fra_opphor
  from siste_opphør
																				

  group by aar_maaned, siste_dato_i_perioden, aar, pk_bb_fagsak,fk_dim_tid_mnd,
        vedtaks_id, behandlings_type, vedtakstidspunkt, saksnr, fk_person1_kravhaver, fk_person1_mottaker,
        fk_person1_skyldner, pk_bb_bidrags_periode, periode_fra, periode_til, belop,
        resultat,resultat_tekst, BIDRAGSEVNE, UNDERHOLDSKOSTNAD, SAMVAERSFRADRAG, NETTO_BARNETILLEGG_BP, NETTO_BARNETILLEGG_BM, 
        SAMVAERSKLASSE, BPS_ANDEL_UNDERHOLDSKOSTNAD, BPBOR_MED_ANDRE_VOKSNE,innkreving_flagg,stonadstype,
        netto_tilsynsutgift,faktisk_tilsynsutgift,valutakode,forste_vedtakstidspunkt,SISTE_KOMPLETT_VEDTAK,SISTE_KOMPLETT_VEDTAKSTIDSPUNKT
)

select * 
from opphor_hvis_finnes