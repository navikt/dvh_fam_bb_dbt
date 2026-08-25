{{
    config(
        materialized='table'
    )
}}

with fag as (
    select t1.* from {{ref ('fam_bb_saerbidrag_fagsak')}} t1
    left join (select omgjor.vedtaks_id
    ,omgjor.fk_person1_kravhaver
    ,omgjor.saksnr
       ,1 as forrige_belop_null
        from {{ref ('fam_bb_saerbidrag_fagsak')}}  omgjor
            left join {{ref ('fam_bb_saerbidrag_fagsak')}}  ved
                on omgjor.omgjor_vedtaks_id = ved.vedtaks_id
                and omgjor.fk_person1_kravhaver = ved.fk_person1_kravhaver
                and omgjor.saksnr = ved.saksnr
                where omgjor.omgjor_vedtaks_id is not null
                and ved.belop is null) t2
    on t1.vedtaks_id = t2.vedtaks_id
    and t1.fk_person1_kravhaver = t2.fk_person1_kravhaver
    and t1.saksnr = t2.saksnr
    where (t1.belop is not null 
    or t1.omgjor_vedtaks_id is not null)
    and t1.INNKREVING_FLAGG = 1
    and t2.forrige_belop_null is null
    and t1.fk_person1_skyldner <> -5
    and t1.fk_person1_kravhaver <> -5
    and t1.fk_person1_mottaker <> -5
),

/* 
Finn total inntekt for personer, og tell antall kategorier inntektene kommer fra
*/
inntekt as (
    SELECT * 
    FROM ( 
        SELECT
            FK_BB_SAERBIDRAG_FAGSAK as key_fak_bb_saerbidrag,
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
            'p' AS inntekt_skyldner
        ) 
    ) piv
),

/* 
1 vedtak kan ha flere omgjøringsvedtak pekende på seg. Siste omgjøringsvedtak
blir gjeldene.
*/
siste_omgjoring as ( select t1.OMGJOR_VEDTAKS_ID
            ,t1.fk_person1_kravhaver
            ,t1.saksnr
            ,TO_CHAR(t1.vedtakstidspunkt, 'yyyymm') as aarmnd_omgjort
        FROM fag t1
       
        INNER JOIN (
            SELECT OMGJOR_VEDTAKS_ID
                ,fk_person1_kravhaver
                ,saksnr
                ,MAX(vedtakstidspunkt) AS max_date 
            FROM fag  
            where OMGJOR_VEDTAKS_ID is not null
            GROUP BY OMGJOR_VEDTAKS_ID
                ,fk_person1_kravhaver
                ,saksnr) t2
        on  t1.OMGJOR_VEDTAKS_ID = t2.OMGJOR_VEDTAKS_ID
        and  t1.fk_person1_kravhaver = t2.fk_person1_kravhaver
        and t1.saksnr = t2.saksnr
        and t1.vedtakstidspunkt = t2.max_date
),

omgjoring as (
    select t1.vedtaks_id
        ,t1.fk_person1_kravhaver
        ,t1.saksnr
        ,case when aarmnd_original = aarmnd_omgjort then 0 else 1 end as aktuell
        ,aarmnd_omgjort as aarmnd_omgjort_belopsendring
        ,case when aarmnd_original < aarmnd_omgjort then belop * (-1) else 0 end as belop_endring
    from (
        select vedtaks_id
            ,fk_person1_kravhaver
            ,saksnr
            ,to_char(vedtakstidspunkt, 'yyyymm') as aarmnd_original
            ,case when belop is null then 0 else belop end as belop
        FROM fag
    ) t1
    inner join siste_omgjoring t2 
    on t1.vedtaks_id = t2.OMGJOR_VEDTAKS_ID
    and  t1.fk_person1_kravhaver = t2.fk_person1_kravhaver
    and t1.saksnr = t2.saksnr

),

vedtak as (
    SELECT RAWTOHEX(pk_bb_saerbidrag_fagsak) as key_fak_bb_saerbidrag -- settes til string slik at omgjoringsvedtak kan få unik key som aldri vil knyttes mot inntekt
        ,concat(TO_CHAR(vedtakstidspunkt, 'yyyymm'),'003') as fk_dim_tid
        ,TO_CHAR(vedtakstidspunkt, 'yyyymm') as aar_maaned
        ,referanse
        ,case when t2.aktuell is null and t1.omgjor_vedtaks_id is null then 1 
        when t2.aktuell is null and t1.omgjor_vedtaks_id is not null then 0
        else t2.aktuell end as aktuell_flagg
        ,t1.vedtaks_id
        ,vedtakstidspunkt
        ,behandlings_type
        ,kategori
        ,t1.saksnr
        ,fk_person1_skyldner
        ,t1.fk_person1_kravhaver
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
    left join omgjoring t2
    on t1.vedtaks_id = t2.vedtaks_id
    and  t1.fk_person1_kravhaver = t2.fk_person1_kravhaver
    and t1.saksnr = t2.saksnr
),


omgjorings_vedtak as (
    select 
        'omgjøring' || '-' || t1.referanse || '-' || t2.vedtaks_id || '-' || t1.fk_person1_kravhaver || '-' || t1.saksnr as key_fak_bb_saerbidrag
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
                ,fk_person1_kravhaver
                ,saksnr
                ,sum(belop_endring) as belop
            from omgjoring
            group by aarmnd_omgjort_belopsendring
                ,vedtaks_id
                ,fk_person1_kravhaver
                ,saksnr
            ) t2
        on t1.vedtaks_id = t2.vedtaks_id
        and  t1.fk_person1_kravhaver = t2.fk_person1_kravhaver
        and t1.saksnr = t2.saksnr
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
    ,t2.inntekt_skyldner_totalt
    ,t2.inntekt_skyldner_antall_typer
    ,{{ ephemeral_star(model_name='dim_person_felter', relation_alias='t3', prefix='SKYLDNER_', except=["fk_person1","gyldig_fra_dato", "gyldig_til_dato" ]) }}
    ,trunc(months_between(to_date(aar_maaned, 'yyyymm'), to_date(t6.fodt_aar_maaned, 'yyyymm')) / 12) AS skyldner_alder    
    ,{{ ephemeral_star(model_name='dim_person_felter', relation_alias='t4', prefix='MOTTAKER_', except=["fk_person1","gyldig_fra_dato", "gyldig_til_dato" ]) }}
    ,trunc(months_between(to_date(aar_maaned, 'yyyymm'), to_date(t7.fodt_aar_maaned, 'yyyymm')) / 12) AS mottaker_alder    
    ,{{ ephemeral_star(model_name='dim_person_felter', relation_alias='t5', prefix='KRAVHAVER_', except=["fk_person1","gyldig_fra_dato", "gyldig_til_dato" ]) }}
    ,trunc(months_between(to_date(aar_maaned, 'yyyymm'), to_date(t8.fodt_aar_maaned, 'yyyymm')) / 12) AS kravhaver_alder
    from sammenstilling t1
    left join inntekt t2
    on t1.key_fak_bb_saerbidrag = t2.key_fak_bb_saerbidrag
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
    on t1.fk_person1_mottaker = t7.fk_person1
    left join  {{ ref('dim_person_fodt') }} t8
    on t1.fk_person1_kravhaver = t8.fk_person1
),

final as (
    select t1.*
    ,{{ ephemeral_star(model_name='dim_alder', relation_alias='t2', prefix='SKYLDNER_', except=["alder"]) }}
    ,{{ ephemeral_star(model_name='dim_alder', relation_alias='t3', prefix='MOTTAKER_', except=["alder"]) }}
    ,{{ ephemeral_star(model_name='dim_alder', relation_alias='t4', prefix='KRAVHAVER_', except=["alder"]) }}    
    from pre_final t1
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
