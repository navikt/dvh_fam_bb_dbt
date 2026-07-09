with tilganger as (
select *
from {{ref ('test_tilganger')}}

union

select
t2.grantee
,t1.skjema as owner
,t1.navn as table_name
,'SELECT ANY TABLE' as grantor 
,'SELECT' as privilege
,NULL as grantable
,NULL as hierarchy 
,null as common
,t1.format as type
,t2.inherited
from {{ref ('test_fam_dvh_katalog')}} t1
cross join (select GRANTEE, inherited from {{ref ('test_tilgang_til_alt')}} 
where PRIVILEGE like 'SELECT ANY TABLE') t2

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

ad as (
    select * from {{ref ('test_ad')}}
),

pre_final as ( 
select concat( concat(lower(owner), '-'),lower(table_name)) as key_fam_dvh_katalog
,grantee as hvem
,case when t2.bruker_gruppe = 'PERSONLIG_BRUKER' then grantee else t3.final_user end as bruker_navn
,grantor as giver
,privilege
,grantable
,type as format
,inherited as arvet
,t2.bruker_gruppe as overordnet_tilgangsgruppe
from tilganger t1
left join bruker t2
on t1.grantee = t2.username
left join ad t3
on t1.grantee = t3.initial_access_grant
),


final as (
    select key_fam_dvh_katalog
    ,hvem
    ,bruker_navn
    ,giver
    ,privilege
,grantable
,format
,arvet
,overordnet_tilgangsgruppe
    ,t4.bruker_gruppe
,t4.bruker_undergruppe
,t4.bruker_profil
,t4.aktiv_bruker
,t4.enhet_bruker
from pre_final t1
left join bruker t4
on t1.bruker_navn = t4.username
)

select * from final

