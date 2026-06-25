with tilganger as (
select *
from {{ref ('test_tilganger_bb')}}

--union all
-- sett opp i alle skjema slik at infoen kan hentes og sammenstilles

),

final as ( select concat( concat(lower(owner), '-'),lower(table_name)) as key_fam_dvh_katalog
,grantee as hvem
,case when  REGEXP_LIKE(grantee, '.*[0-9].*') then 'bruker' else 'gruppe' end as kategori
,grantor as giver
,privilege
,grantable
,type as format
,inherited as arvet

from tilganger

)

select * from final

