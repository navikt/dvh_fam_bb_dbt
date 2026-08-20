{{
    config(
        materialized='table'
    )
}}

with fag as (
    select t1.* from {{ref ('fam_bb_saerbidrag_fagsak')}} t1
    left join (select omgjor.vedtaks_id
       ,1 as forrige_belop_null
        from {{ref ('fam_bb_saerbidrag_fagsak')}}  omgjor
            left join {{ref ('fam_bb_saerbidrag_fagsak')}}  ved
                on omgjor.omgjor_vedtaks_id = ved.vedtaks_id
                where omgjor.omgjor_vedtaks_id is not null
                and ved.belop is null) t2
    on t1.vedtaks_id = t2.vedtaks_id
    where (t1.belop is not null 
    or t1.omgjor_vedtaks_id is not null)
    and t1.INNKREVING_FLAGG = 1
    and t2.forrige_belop_null is null
    and t1.fk_person1_skyldner <> -5
    and t1.fk_person1_kravhaver <> -5
    and t1.fk_person1_mottaker <> -5
),


inntekt as (
    SELECT * 
    FROM ( 
        SELECT
            FK_BB_SAERBIDRAG_FAGSAK,
            inntekt_kategori,
            INNTEKT_FOR,
            inntekt_belop
        FROM {{ ref('fam_bb_saerbidrag_inntekt') }}
    )
    PIVOT ( 
        SUM(inntekt_belop) as totalt,
        COUNT(DISTINCT inntekt_kategori) AS antall_typer  
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
            ,TO_CHAR(vedtakstidspunkt, 'yyyymm') as aarmnd_original
            ,case when belop is null then 0 else belop end as belop
        FROM fag
    ) t1
    inner join (
        select OMGJOR_VEDTAKS_ID
            ,TO_CHAR(vedtakstidspunkt, 'yyyymm') as aarmnd_omgjort
        FROM fag
        where OMGJOR_VEDTAKS_ID is not null) t2
        on t1.vedtaks_id = t2.OMGJOR_VEDTAKS_ID
),

vedtak as (
    SELECT pk_bb_saerbidrag_fagsak
        ,concat(TO_CHAR(vedtakstidspunkt, 'yyyymm'),'003') as fk_dim_tid
        ,TO_CHAR(vedtakstidspunkt, 'yyyymm') as aar_maaned
        ,referanse
        ,case when t2.aktuell is null then 1 else t2.aktuell end as aktuell_flagg
        ,t1.vedtaks_id
        ,vedtakstidspunkt
        ,behandlings_type
        ,kategori
        ,saksnr
        ,fk_person1_skyldner
        ,fk_person1_kravhaver
        ,fk_person1_mottaker
        ,case when belop is null then 0 else belop end as belop
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
),


omgjorings_vedtak as (
    select 
        NULL as pk_bb_saerbidrag_fagsak
        ,concat(TO_CHAR(t2.aarmnd_omgjort_belopsendring),'003') as fk_dim_tid
        ,t2.aarmnd_omgjort_belopsendring as aar_maaned
        ,t1.referanse as referanse
        ,1 as aktuell_flagg
        ,t2.vedtaks_id
        ,t1.vedtakstidspunkt
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
            (select aarmnd_omgjort_belopsendring
                ,vedtaks_id
                ,sum(belop_endring) as belop
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

pre_final as (
    select t1.*
    ,t2.inntekt_mottaker_totalt
    ,t2.inntekt_mottaker_antall_typer
    ,t2.inntekt_pliktig_totalt
    ,t2.inntekt_pliktig_antall_typer
    ,{{ dbt_utils.star(from=ref('dim_person_felter'), relation_alias='t3', prefix='SKYLDNER_', except=["FK_PERSON1","gyldig_fra_dato", "gyldig_til_dato" ]) }}
    ,trunc(months_between(to_date(aar_maaned, 'yyyymm'), to_date(t6.fodt_aar_maaned, 'yyyymm')) / 12) AS skyldner_alder    
    ,{{ dbt_utils.star(from=ref('dim_person_felter'), relation_alias='t4', prefix='MOTTAKER_', except=["FK_PERSON1","gyldig_fra_dato", "gyldig_til_dato" ]) }}
    ,trunc(months_between(to_date(aar_maaned, 'yyyymm'), to_date(t7.fodt_aar_maaned, 'yyyymm')) / 12) AS mottaker_alder    
    ,{{ dbt_utils.star(from=ref('dim_person_felter'), relation_alias='t5', prefix='KRAVHAVER_', except=["FK_PERSON1","gyldig_fra_dato", "gyldig_til_dato" ]) }}
    ,trunc(months_between(to_date(aar_maaned, 'yyyymm'), to_date(t8.fodt_aar_maaned, 'yyyymm')) / 12) AS kravhaver_alder
    from sammenstilling t1
    left join inntekt t2
    on t1.pk_bb_saerbidrag_fagsak = t2.fk_bb_saerbidrag_fagsak
    left join  {{ ref('dim_person_felter') }} t3
    on t1.fk_person1_skyldner = t3.fk_person1
        and t3.gyldig_fra_dato <= t1.vedtakstidspunkt
        and t3.gyldig_til_dato >= t1.vedtakstidspunkt
    left join  {{ ref('dim_person_felter') }} t4
    on t1.fk_person1_mottaker = t4.fk_person1
        and t4.gyldig_fra_dato <= t1.vedtakstidspunkt
        and t4.gyldig_til_dato >= t1.vedtakstidspunkt
    left join  {{ ref('dim_person_felter') }} t5
    on t1.fk_person1_kravhaver = t5.fk_person1
        and t5.gyldig_fra_dato <= t1.vedtakstidspunkt
        and t5.gyldig_til_dato >= t1.vedtakstidspunkt
-- alder
    left join  {{ ref('dim_person_fodt') }} t6
    on t1.fk_person1_skyldner = t6.fk_person1
    left join  {{ ref('dim_person_fodt') }} t7
    on t1.fk_person1_mottaker = t6.fk_person1
    left join  {{ ref('dim_person_fodt') }} t8
    on t1.fk_person1_kravhaver = t6.fk_person1
),

final as (
    select t1.*
    ,t2.alder_gruppe5_besk as skyldner_alder_gruppe5
    ,t3.alder_gruppe5_besk as mottaker_alder_gruppe5
    ,t4.alder_gruppe5_besk as kravhvaer_alder_gruppe5
    from prefinal t1
    left join  {{ ref('dim_alder') }} t2
    on t1.skyldner_alder = t2.alder
        left join  {{ ref('dim_alder') }} t3
    on t1.mottaker_alder = t3.alder
    left join  {{ ref('dim_alder') }} t4
    on t1.kravhaver_alder = t4.alder
)

select final.*
,'{{ var("gyldig_flagg") }}'  as gyldig_flagg
,localtimestamp as lastet_dato  
from final
