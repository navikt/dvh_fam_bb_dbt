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

inntekt as (
    SELECT * 
    FROM ( 
        SELECT
            FK_BB_SAERBIDRAG_FAGSAK,
            TYPE_INNTEKT,
            INNTEKT_FOR,
            BELOP
        FROM {{ ref('fam_bb_saerbidrag_inntekt') }}
    )
    PIVOT ( 
        SUM(BELOP) as totalt,
        COUNT(DISTINCT TYPE_INNTEKT) AS antall_typer  
        FOR INNTEKT_FOR IN ( 
            'm' AS inntekt_mottaker,
            'p' AS inntekt_pliktig
        ) 
    ) piv
),


omgjoring as (
    select t1.vedtaks_id
,case when aarmnd_original = aarmnd_omgjort then 0 else 1 end as gyldig
,aarmnd_omgjort as aarmnd_omgjort_belopsendring
,case when aarmnd_original < aarmnd_omgjort then belop * (-1) else 0 end as belop_endring
from (
select vedtaks_id
,TO_CHAR(VEDTAKS_TIDSPUNKT, 'yyyymm') as aarmnd_original
,belop
FROM fag
) t1
inner join (
select OMGJOR_VEDTAKS_ID
,TO_CHAR(VEDTAKS_TIDSPUNKT, 'yyyymm') as aarmnd_omgjort
FROM fag
where OMGJOR_VEDTAKS_ID is not null) t2
on t1.vedtaks_id = t2.OMGJOR_VEDTAKS_ID
),


sammenstilling as (
SELECT pk_bb_saerbidrag_fagsak
,concat(TO_CHAR(VEDTAKS_TIDSPUNKT, 'yyyymm'),to_char('03')) as fk_dim_tid
,TO_CHAR(VEDTAKS_TIDSPUNKT, 'yyyymm') as aar_mnd
,case when OMGJOR_VEDTAKS_ID is null then concat(concat(to_char(t1.VEDTAKS_ID),'-' ), to_char(FK_PERSON1_KRAVHAVER)) 
else concat(concat(to_char(OMGJOR_VEDTAKS_ID),'-' ), to_char(FK_PERSON1_KRAVHAVER)) end as sammenhengende_vedtak
,case when t2.gyldig is null then 1 else t2.gyldig end as gyldig_flagg
    ,t1.vedtaks_id
    ,vedtaks_tidspunkt
    ,bidragstype
    ,kategori
    ,saksnr
    ,fk_person1_skyldner
    ,fk_person1_kravhaver
    ,fk_person1_mottaker
    ,belop
    ,valuta_kode
    ,resultat
    --,innkreving_flagg
    --,omgjor_vedtak_id
    --,historisk_flagg
    ,krav_belop
    ,godkjent_belop
    ,betalt_belop
    ,lastet_dato as mart_lastet_dato

from fag t1
left join (select vedtaks_id, gyldig 
from omgjoring) t2
on t1.vedtaks_id = t2.vedtaks_id

UNION ALL

select 
NULL as pk_bb_saerbidrag_fagsak
,concat(TO_CHAR(aarmnd_omgjort_belopsendring),to_char('03')) as fk_dim_tid
,aarmnd_omgjort_belopsendring as aarmnd
,NULL as sammenhengende_vedtak
    ,1 as gyldig_flagg
    ,NULL as vedtaks_id
    ,NULL as vedtaks_tidspunkt
    ,NULL as bidragstype
    ,NULL as kategori
    ,NULL as saksnr
    ,NULL as fk_person1_skyldner
    ,NULL as fk_person1_kravhaver
    ,NULL as fk_person1_mottaker
    ,belop
    ,NULL as valuta_kode   -- må håndteres tidligere
    ,NULL as resultat
    --,NULL as innkreving_flagg
   -- ,NULL as omgjor_vedtak_id
   -- ,NULL as historisk_flagg
    ,NULL as krav_belop
    ,NULL as godkjent_belop
    ,NULL as betalt_belop
    ,NULL as mart_lastet_dato
from (select aarmnd_omgjort_belopsendring, sum(belop_endring) as belop
from omgjoring
group by aarmnd_omgjort_belopsendring
) t3
where belop <> 0
),

final as (
    select t1.*
    ,t2.inntekt_mottaker_totalt
    ,t2.inntekt_mottaker_antall_typer
    ,t2.inntekt_pliktig_totalt
    ,t2.inntekt_pliktig_antall_typer
    from sammenstilling t1
    left join inntekt t2
    on t1.pk_bb_saerbidrag_fagsak = t2.fk_bb_saerbidrag_fagsak
)


select final.*
,localtimestamp as lastet_dato  
from final
