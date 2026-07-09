with katalog as (
select skjema
,table_name as navn
,'TABLE' as format
,has_identity as id_flagg
,num_rows as antall_rader
,sample_size as storrelse
,last_analyzed as sist_analysert
from {{ref ('test_tabeller')}}

union all 

select skjema
,view_name as navn
,'VIEW' as format
,NULL as id_flagg
,NULL as antall_rader
,NULL as storrelse
,NULL as sist_analysert
from  {{ref ('test_views')}}
),

final as (
    select concat( concat(lower(skjema), '-'),lower(navn)) as key_fam_dvh_katalog
    ,REGEXP_SUBSTR(navn, '[^_]+', 1, 1) AS kategori
    ,katalog.* 
from katalog

)

select * from final