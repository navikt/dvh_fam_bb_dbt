{{
    config(
        materialized='table'
    )
}}

with fag as (
    select * from {{ref ('fam_bb_saerbidrag_fagsak')}}
    where belop is not null 
    or omgjor_vedtaks_id is not null
    and INNKREVING_FLAGG = 1
    and fk_person1_skyldner <> -5
    and fk_person1_kravhaver <> -5
    and fk_person1_mottaker <> -5
),

org_vedtak as (
    SELECT
    CONNECT_BY_ROOT vedtaks_id AS original_vedtaks_id,
    vedtaks_id                 AS vedtaks_id
    FROM {{ref ('fam_bb_saerbidrag_fagsak')}}
    START WITH omgjor_vedtaks_id IS NULL
    CONNECT BY NOCYCLE PRIOR vedtaks_id = omgjor_vedtaks_id
),

inntekt as (
    SELECT * 
    FROM ( 
        SELECT
            FK_BB_SAERBIDRAG_FAGSAK,
            TYPE_INNTEKT,
            INNTEKT_FOR,
            inntekt_belop
        FROM {{ ref('fam_bb_saerbidrag_inntekt') }}
    )
    PIVOT ( 
        SUM(inntekt_belop) as totalt,
        COUNT(DISTINCT TYPE_INNTEKT) AS antall_typer  
        FOR INNTEKT_FOR IN ( 
            'm' AS inntekt_mottaker,
            'p' AS inntekt_pliktig
        ) 
    ) piv
),


omgjoring as (
    select t1.vedtaks_id
        ,case when aarmnd_original = aarmnd_omgjort then 0 else 1 end as aktuell
        ,aarmnd_omgjort as aarmnd_omgjort_belopsendring
        ,case when aarmnd_original < aarmnd_omgjort then belop * (-1) else 0 end as belop_endring
    from (
        select vedtaks_id
            ,TO_CHAR(vedtaks_tid, 'yyyymm') as aarmnd_original
            ,case when belop is null then 0 else belop end as belop
        FROM fag
    ) t1
    inner join (
        select OMGJOR_VEDTAKS_ID
            ,TO_CHAR(vedtaks_tid, 'yyyymm') as aarmnd_omgjort
        FROM fag
        where OMGJOR_VEDTAKS_ID is not null) t2
        on t1.vedtaks_id = t2.OMGJOR_VEDTAKS_ID
),

vedtak as (
    SELECT pk_bb_saerbidrag_fagsak
        ,concat(TO_CHAR(vedtaks_tid, 'yyyymm'),to_char('03')) as fk_dim_tid
        ,TO_CHAR(vedtaks_tid, 'yyyymm') as aar_mnd
        ,concat(concat(to_char(t3.original_vedtaks_id),'-' ), to_char(FK_PERSON1_KRAVHAVER)) as sammenhengende_vedtak
        ,case when t2.aktuell is null then 1 else t2.aktuell end as aktuell_flagg
        ,t1.vedtaks_id
        ,vedtaks_tid
        ,behandlings_type
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
        ,historisk_flagg
        ,krav_belop
        ,godkjent_belop
        ,betalt_belop
        ,lastet_dato as mart_lastet_dato
    from fag t1
    left join (select vedtaks_id, aktuell 
        from omgjoring) t2
    on t1.vedtaks_id = t2.vedtaks_id
    left join org_vedtak t3
    on t1.vedtaks_id = t3.vedtaks_id
),

omgjorings_vedtak as (
    select 
        NULL as pk_bb_saerbidrag_fagsak
        ,concat(TO_CHAR(t2.aarmnd_omgjort_belopsendring),to_char('03')) as fk_dim_tid
        ,t2.aarmnd_omgjort_belopsendring as aarmnd
        ,t1.sammenhengende_vedtak as sammenhengende_vedtak
        ,1 as aktuell_flagg
        ,t2.vedtaks_id
        ,t1.vedtaks_tid
        ,NULL as behandlings_type
        ,NULL as kategori
        ,t1.saksnr
        ,t1.fk_person1_skyldner
        ,t1.fk_person1_kravhaver
        ,t1.fk_person1_mottaker
        ,t2.belop
        ,t1.valuta_kode   -- må håndteres tidligere
        ,t1.resultat
        --,NULL as innkreving_flagg
        -- ,NULL as omgjor_vedtak_id
        ,NULL as historisk_flagg
        ,NULL as krav_belop
        ,NULL as godkjent_belop
        ,NULL as betalt_belop
        ,NULL as mart_lastet_dato
        from vedtak t1
        inner join  
            (select aarmnd_omgjort_belopsendring,vedtaks_id, sum(belop_endring) as belop
            from omgjoring
            group by aarmnd_omgjort_belopsendring, vedtaks_id
            ) t2
        on t1.vedtaks_id = t2.vedtaks_id
        where t2.belop <> 0
),


sammenstilling as (
SELECT * from vedtak

UNION ALL

select * from omgjorings_vedtak
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
,'{{ var("gyldig_flagg") }}'  as gyldig_flagg
,localtimestamp as lastet_dato  
from final
