{{
    config(
        materialized='table',
        post_hook="{{ sync_multi_source_comments([ ['kode_verk', 'dim_tid'], ['kode_verk', 'dim_geografi']]) }}"
    )
}}

with saer as (
    select fk_dim_tid
    ,mottaker_fk_dim_geografi
    ,mottaker_fk_dim_alder
    ,mottaker_fk_dim_kjonn
    ,count(distinct fk_person1_mottaker) as saerbidrag_antall_mottakere
    from test_fam_bb_saerbidrag_mnd
    group by fk_dim_tid
    ,mottaker_fk_dim_geografi
    ,mottaker_fk_dim_alder
    ,mottaker_fk_dim_kjonn
),

final as ( 
    select 
    'Barnebidrag' as kilde_omraade,
    {{ ephemeral_star(model_name='dim_bredt_aggregat_mnd', relation_alias='t1') }},
    t2.saerbidrag_antall_mottakere
    from saer t2
    right join {{ ref('dim_bredt_aggregat_mnd') }} t1
    on t1.fk_dim_tid = t2.fk_dim_tid
    and t1.fk_dim_geografi = t2.mottaker_fk_dim_geografi
    and t1.fk_dim_alder = t2.mottaker_fk_dim_alder
    and t1.fk_dim_kjonn = t2.mottaker_fk_dim_kjonn
)

select final.*
,'{{ var("gyldig_flagg") }}'  as gyldig_flagg
,localtimestamp as lastet_dato  
from final