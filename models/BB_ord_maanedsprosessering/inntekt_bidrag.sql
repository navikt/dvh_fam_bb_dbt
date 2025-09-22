inntekt as (
    select  
        fk_bb_bidrags_periode,
        type_inntekt,
        belop,
        flagg,
        row_number() over (partition by fk_bb_bidrags_periode, flagg order by type_inntekt) as nr
    from {{ source ('fam_bb', 'fam_bb_inntekts_liste_ord') }} 
),

inntekts_typer as (
select
        fk_bb_bidrags_periode,
        max(case when nr = 1 and flagg = 'P' then type_inntekt end) as p_type_inntekt_1,
        max(case when nr = 1 and flagg = 'P' then belop end) as p_inntekt_1,
        max(case when nr = 2 and flagg = 'P' then type_inntekt end) as p_type_inntekt_2,
        max(case when nr = 2 and flagg = 'P' then belop end) as p_inntekt_2,
        max(case when nr = 3 and flagg = 'P' then type_inntekt end) as p_type_inntekt_3,
        max(case when nr = 3 and flagg = 'P' then belop end) as p_inntekt_3,
        sum(case when flagg = 'P' then belop else 0 end) as p_inntekt_total,
        max(case when flagg = 'P' then nr else 0 end) as p_antall_typer,
        
        max(case when nr = 1 and flagg = 'M' then type_inntekt end) as m_type_inntekt_1,
        max(case when nr = 1 and flagg = 'M' then belop end) as m_inntekt_1,
        max(case when nr = 2 and flagg = 'M' then type_inntekt end) as m_type_inntekt_2,
        max(case when nr = 2 and flagg = 'M' then belop end) as m_inntekt_2,
        max(case when nr = 3 and flagg = 'M' then type_inntekt end) as m_type_inntekt_3,
        max(case when nr = 3 and flagg = 'M' then belop end) as m_inntekt_3,
        max(case when nr = 4 and flagg = 'M' then type_inntekt end) as m_type_inntekt_4,
        max(case when nr = 4 and flagg = 'M' then belop end) as m_inntekt_4,
        max(case when nr = 5 and flagg = 'M' then type_inntekt end) as m_type_inntekt_5,
        max(case when nr = 5 and flagg = 'M' then belop end) as m_inntekt_5,
        max(case when nr = 5 and flagg = 'M' then type_inntekt end) as m_type_inntekt_6,
        max(case when nr = 5 and flagg = 'M' then belop end) as m_inntekt_6,3
        sum(case when flagg = 'M' then belop else 0 end) as m_inntekt_total,
        max(case when flagg = 'M' then nr else 0 end) as m_antall_typer

    from inntekt
    group by fk_bb_bidrags_periode
)

select * 
from inntekts_typer