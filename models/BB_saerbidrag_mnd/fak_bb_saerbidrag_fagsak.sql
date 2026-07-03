{{
    config(
        materialized='table'
    )
}}

with fag as (
    select * from {{ref ('fam_bb_saerbidrag_fagsak')}}
    where HISTORISK_FLAGG = 0 
and resultat = 'SÆRBIDRAG_INNVILGET'
and INNKREVING_FLAGG = 1
),


omgjoring as (
    select t1.vedtaksid
,case when aarmnd_original = aarmnd_omgjort then 0 else 1 end as gyldig
,aarmnd_omgjort as aarmnd_omgjort_belopsendring
,case when aarmnd_original < aarmnd_omgjort then belop * (-1) else 0 end as belop_endring
from (
select vedtaksid
,TO_CHAR(VEDTAKSTIDSPUNKT, 'yyyymm') as aarmnd_original
,belop
FROM fag
) t1
inner join (
select OMGJOR_VEDTAK_ID
,TO_CHAR(VEDTAKSTIDSPUNKT, 'yyyymm') as aarmnd_omgjort
FROM fag
where OMGJOR_VEDTAK_ID is not null) t2
on t1.vedtaksid = t2.OMGJOR_VEDTAK_ID
),


sammenstilling as (
SELECT pk_bb_saerbidrag_fagsak
,TO_CHAR(VEDTAKSTIDSPUNKT, 'yyyymm') as aarmnd
,case when OMGJOR_VEDTAK_ID is null then concat(concat(to_char(t1.VEDTAKSID),'-' ), to_char(FK_PERSON1_KRAVHAVER)) 
else concat(concat(to_char(OMGJOR_VEDTAK_ID),'-' ), to_char(FK_PERSON1_KRAVHAVER)) end as sammenhengende_vedtak
,case when t2.gyldig is null then 1 else t2.gyldig end as gyldig_flagg
    ,t1.vedtaksid
    ,vedtakstidspunkt
    ,bidragstype
    ,kategori
    ,saksnr
    ,fk_person1_skyldner
    ,fk_person1_kravhaver
    ,fk_person1_mottaker
    ,belop
    ,valutakode
    ,resultat
    --,innkreving_flagg
    --,omgjor_vedtak_id
    --,historisk_flagg
    ,kravbelop
    ,godkjent_belop
    ,betalt_belop
    ,lastet_dato as mart_lastet_dato

from fag t1
left join (select vedtaksid, gyldig 
from omgjoring) t2
on t1.vedtaksid = t2.vedtaksid

UNION ALL

select 
NULL as pk_bb_saerbidrag_fagsak
,aarmnd_omgjort_belopsendring as aarmnd
,NULL as sammenhengende_vedtak
    ,1 as gyldig_flagg
    ,NULL as vedtaksid
    ,NULL as vedtakstidspunkt
    ,NULL as bidragstype
    ,NULL as kategori
    ,NULL as saksnr
    ,NULL as fk_person1_skyldner
    ,NULL as fk_person1_kravhaver
    ,NULL as fk_person1_mottaker
    ,belop
    ,NULL as valutakode   -- må håndteres tidligere
    ,NULL as resultat
    --,NULL as innkreving_flagg
   -- ,NULL as omgjor_vedtak_id
   -- ,NULL as historisk_flagg
    ,NULL as kravbelop
    ,NULL as godkjent_belop
    ,NULL as betalt_belop
    ,NULL as mart_lastet_dato
from (select aarmnd_omgjort_belopsendring, sum(belop_endring) as belop
from omgjoring
group by aarmnd_omgjort_belopsendring
) t3
where belop <> 0
)


select sammenstilling.*
,localtimestamp as lastet_dato  from sammenstilling
