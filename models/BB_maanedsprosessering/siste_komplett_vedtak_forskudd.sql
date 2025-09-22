with lopende_sak as (
  select 
    aar_maaned, siste_dato_i_perioden, aar, pk_bb_fagsak,fk_dim_tid_mnd,
    fk_person1_mottaker, fk_person1_skyldner, pk_bb_bidrags_periode, periode_fra, periode_til,
    belop, resultat, siste_opphor.periode_fra_opphor, behandlings_type, vedtakstidspunkt,
    nyeste_record.vedtaks_id, nyeste_record.saksnr, nyeste_record.fk_person1_kravhaver

  from {{ ref('nyeste_record_forskudd') }}  nyeste_record
  join {{ ref('opphor_forskudd') }}  siste_opphor
  on nyeste_record.saksnr = siste_opphor.saksnr
  and nyeste_record.vedtaks_id = siste_opphør.vedtaks_id
  and nyeste_record.fk_person1_kravhaver = siste_opphor.fk_person1_kravhaver
  and nyeste_record.siste_dato_i_perioden < nvl( siste_opphor.periode_fra_opphor, nyeste_record.siste_dato_i_perioden + 1)
 
  group by aar_maaned, siste_dato_i_perioden, aar, pk_bb_fagsak,fk_dim_tid_mnd,
           nyeste_record.vedtaks_id, behandlings_type, vedtakstidspunkt, nyeste_record.saksnr, 
           nyeste_record.fk_person1_kravhaver, fk_person1_mottaker,fk_person1_skyldner, 
           pk_bb_bidrags_periode, periode_fra, periode_til, belop, resultat, siste_opphor.periode_fra_opphor
)
,
 
siste_komplett_info as (
  select lopende_sak.vedtaks_id, lopende_sak.saksnr, lopende_sak.fk_person1_kravhaver
        ,max(fagsak.pk_bb_bidrags_periode) keep (dense_rank first order by fagsak.vedtakstidspunkt desc) siste_komplett_pk_periode
        ,max(fagsak.vedtaks_id) keep (dense_rank first order by fagsak.vedtakstidspunkt desc) siste_komplett_vedtak
  
  from lopende_sak
  join {{ ref('fagsak_forskudd') }} fagsak
  on lopende_sak.saksnr = fagsak.saksnr
  and lopende_sak.fk_person1_kravhaver = fagsak.fk_person1_kravhaver
  and lopende_sak.vedtakstidspunkt >= fagsak.vedtakstidspunkt
  and fagsak.underholdskostnad > 0
 
  group by lopende_sak.vedtaks_id, lopende_sak.saksnr, lopende_sak.fk_person1_kravhaver
)

select * 
from siste_komplett_info