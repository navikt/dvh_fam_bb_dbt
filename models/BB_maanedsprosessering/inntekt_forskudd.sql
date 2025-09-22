with siste_inntekt as (
    select
        fagsak.saksnr, fagsak.fk_person1_kravhaver, fagsak.fk_person1_mottaker,
        max(inntekt.fk_bb_bidrags_periode) keep (dense_rank first order by fagsak.vedtakstidspunkt desc) fk_bb_bidrags_periode_inntekt       
    from {{ ref('fagsak_forskudd') }} fagsak
    join {{ source ('fam_bb', 'fam_bb_inntekts_liste_ord') }} inntekt
    on fagsak.pk_bb_bidrags_periode = inntekt.fk_bb_bidrags_periode     
    group by fagsak.saksnr, fagsak.fk_person1_kravhaver, fagsak.fk_person1_mottaker
),

inntekt as (
  select siste_inntekt.saksnr, siste_inntekt.fk_person1_mottaker, siste_inntekt.fk_person1_kravhaver,      
    inntekt.type_inntekt, inntekt.belop as belop_inntekt,inntekt.flagg,
    row_number() over (partition by inntekt.fk_bb_bidrags_periode, inntekt.flagg order by inntekt.type_inntekt) as nr
  from siste_inntekt
  join {{ source ('fam_bb', 'fam_bb_inntekts_liste_ord') }} inntekt
  on siste_inntekt.fk_bb_bidrags_periode_inntekt = inntekt.fk_bb_bidrags_periode
)
,
 
--Definere 3 typer inntekter for pliktig og 6 typer for mottaker.
inntekts_typer as (
  select
    --pk_bb_bidrags_periode, max(saksnr) saksnr, max(fk_person1_kravhaver) fk_person1_kravhaver,
    saksnr, fk_person1_kravhaver, fk_person1_mottaker,
       
    max(case when nr = 1 and flagg = 'P' then type_inntekt end) as p_type_inntekt_1,
    max(case when nr = 1 and flagg = 'P' then belop_inntekt end) as p_inntekt_1,
    max(case when nr = 2 and flagg = 'P' then type_inntekt end) as p_type_inntekt_2,
    max(case when nr = 2 and flagg = 'P' then belop_inntekt end) as p_inntekt_2,
    max(case when nr = 3 and flagg = 'P' then type_inntekt end) as p_type_inntekt_3,
    max(case when nr = 3 and flagg = 'P' then belop_inntekt end) as p_inntekt_3,
    
    max(case when nr = 1 and flagg = 'M' then type_inntekt end) as m_type_inntekt_1,
    max(case when nr = 1 and flagg = 'M' then belop_inntekt end) as m_inntekt_1,
    max(case when nr = 2 and flagg = 'M' then type_inntekt end) as m_type_inntekt_2,
    max(case when nr = 2 and flagg = 'M' then belop_inntekt end) as m_inntekt_2,
    max(case when nr = 3 and flagg = 'M' then type_inntekt end) as m_type_inntekt_3,
    max(case when nr = 3 and flagg = 'M' then belop_inntekt end) as m_inntekt_3,
    max(case when nr = 4 and flagg = 'M' then type_inntekt end) as m_type_inntekt_4,
    max(case when nr = 4 and flagg = 'M' then belop_inntekt end) as m_inntekt_4,
    max(case when nr = 5 and flagg = 'M' then type_inntekt end) as m_type_inntekt_5,
    max(case when nr = 5 and flagg = 'M' then belop_inntekt end) as m_inntekt_5,
    max(case when nr = 5 and flagg = 'M' then type_inntekt end) as m_type_inntekt_6,
    max(case when nr = 5 and flagg = 'M' then belop_inntekt end) as m_inntekt_6,
 
    sum(case when flagg = 'P' then belop_inntekt else 0 end) as p_inntekt_total,
    max(case when flagg = 'P' then nr else 0 end) as p_antall_typer,
 
    sum(case when flagg = 'M' then belop_inntekt else 0 end) as m_inntekt_total,
    max(case when flagg = 'M' then nr else 0 end) as m_antall_typer
 
  from inntekt
  group by saksnr, fk_person1_kravhaver, fk_person1_mottaker
)