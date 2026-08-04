with katalog as (
select *
from {{ref ('test_objekter')}}
),

final as (
    select concat( concat(lower(SCHEMA_NAME), '-'),lower(OBJECT_NAME)) as key_fam_dvh_katalog
    ,REGEXP_SUBSTR(OBJECT_NAME, '[^_]+', 1, 1) AS kategori
    ,katalog.* 
from katalog

)

select * from final