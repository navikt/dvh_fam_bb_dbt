with fak as (
    select * from {{ref ('test_fam_bb_saerbidrag_mnd')}}
    where gyldig_flagg = 1
),

final as (
select * 
from fak
)

select * from final