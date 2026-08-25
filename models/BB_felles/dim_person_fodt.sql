{{ config(materialized='ephemeral') }}

select t1.fk_person1
,t1.fodt_dato
,to_char(t1.fodt_dato, 'yyyymm') as fodt_aar_maaned
from {{ source ('person', 'dim_person') }} t1
inner join (select  fk_person1
                ,max(gyldig_fra_dato) as max_gyldig
            from {{ source ('person', 'dim_person') }} 
            group by fk_person1) t2
on t1.fk_person1 = t2.fk_person1 
and t1.gyldig_fra_dato = t2.max_gyldig
where t1.utfaset = 0 