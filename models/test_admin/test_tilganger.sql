with tilganger as (
select *
from {{ref ('test_tilganger_bb')}}

--union all
-- sett opp i alle skjema slik at infoen kan hentes og sammenstilles

),

bruker as (
select username
,user_category as bruker_gruppe
,user_sub_category as bruker_undergruppe
,profile as bruker_profil
,active as aktiv_bruker
,enhet as enhet_bruker
from {{ref ('test_brukere')}}
),

final as ( 
select concat( concat(lower(owner), '-'),lower(table_name)) as key_fam_dvh_katalog
,grantee as hvem
,grantor as giver
,privilege
,grantable
,type as format
,inherited as arvet
,t2.bruker_gruppe
,t2.bruker_undergruppe
,t2.bruker_profil
,t2.aktiv_bruker
,t2.enhet_bruker

from tilganger t1
left join bruker t2
on t1.grantee = t2.username

)

select * from final

