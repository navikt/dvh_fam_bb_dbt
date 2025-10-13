with opphor_fra as (
  select 
    fagsak.fk_person1_kravhaver, fagsak.saksnr, fagsak.vedtakstidspunkt, fagsak.vedtaks_id
    ,min(periode.periode_fra) periode_fra_opphor
  from {{ source ('fam_bb_forskudd_maaned', 'fam_bb_fagsak') }} fagsak   
  join {{ source ('fam_bb_forskudd_maaned', 'fam_bb_forskudds_periode') }} periode
  on fagsak.pk_bb_fagsak = periode.fk_bb_fagsak
  and periode.belop is null --Opphørt versjon
  
  where fagsak.behandlings_type not in ('ENDRING_MOTTAKER')
  and trunc(fagsak.vedtakstidspunkt, 'dd') <= to_date('20250731', 'yyyymmdd')--Begrense max_vedtaksdato

  group by fagsak.fk_person1_kravhaver, fagsak.saksnr, fagsak.vedtakstidspunkt, fagsak.vedtaks_id
),

siste_opphor as (
  select 
    nyeste_record.saksnr, nyeste_record.vedtaks_id, nyeste_record.fk_person1_kravhaver
    ,min(opphor_fra.periode_fra_opphor) as periode_fra_opphor
  from {{ref('nyeste_record_forskudd')}} nyeste_record
  left join opphor_fra
  on opphor_fra.fk_person1_kravhaver = nyeste_record.fk_person1_kravhaver
  and opphor_fra.saksnr = nyeste_record.saksnr
  and opphor_fra.vedtakstidspunkt >= nyeste_record.vedtakstidspunkt
 
  group by nyeste_record.saksnr, nyeste_record.vedtaks_id, nyeste_record.fk_person1_kravhaver
)

select * 
from siste_opphor