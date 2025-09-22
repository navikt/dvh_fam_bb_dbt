with tid as (
 
  select aar_maaned, siste_dato_i_perioden, aar, pk_dim_tid
  from {{ source ('kode_verk', 'dim_tid') }}
  where gyldig_flagg = 1
  and dim_nivaa = 3

  and aar_maaned between '{{ var ("periode_fom") }}' and '{{ var ("periode_tom") }}' 
),

fagsak as (
  select 
    fagsak.pk_bb_fagsak, fagsak.fk_person1_kravhaver, fagsak.vedtaks_id, fagsak.saksnr, 
    fagsak.behandlings_type,fagsak.vedtakstidspunkt, fagsak.fk_person1_mottaker,

    periode.pk_bb_forskudds_periode, periode.periode_fra, periode.periode_til, 
    periode.belop, periode.resultat, periode.barnets_alders_gruppe,

    tid.aar_maaned, tid.siste_dato_i_perioden, tid.aar, tid.pk_dim_tid as fk_dim_tid_mnd,

    row_number() over (partition by tid.aar_maaned, fagsak.fk_person1_kravhaver ,fagsak.saksnr 
	    order by fagsak.vedtakstidspunkt desc) nr

  from {{ source ('fam_bb', 'fam_bb_fagsak') }} fagsak
 
  join {{ source ('fam_bb', 'fam_bb_forskudds_periode') }} periode
  on fagsak.pk_bb_fagsak = periode.fk_bb_fagsak
  and periode.belop > 0
 
  join tid
  on periode.periode_fra <= to_date(tid.aar_maaned||'01', 'yyyymmdd')
  and nvl(periode.periode_til, tid.siste_dato_i_perioden) >= tid.siste_dato_i_perioden
 
  where fagsak.behandlings_type not in ('ENDRING_MOTTAKER', 'OPPHØR', 'ALDERSOPPHØR')
  and trunc(fagsak.vedtakstidspunkt, 'dd') <= TO_DATE('{{ var ("max_vedtaksdato") }}', 'yyyymmdd')
)

-- henter den nyeste/siste recorden
select *      
from fagsak
where nr = 1