with fak as (
    select * 
    from {{ ref('test_fam_bb_saerbidrag_inntekt_mnd') }}
   -- where gyldig_flagg = 1
),

final as (
    select 
        fak.*,
        'SÆRBIDRAG' as stonads_type
    from fak
    -- UNION med andre inntekter
)

select * from final