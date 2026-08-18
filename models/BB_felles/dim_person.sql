select t1.fk_person1
    ,t1.pk_dim_person as fk_dim_person
    ,t1.gyldig_fra_dato
    ,t1.gyldig_til_dato
    ,t1.statsborgerskap
    ,t1.bosted_land
    ,t2.fylke_nr
    ,t2.fylke_navn
    ,t2.fylke_gruppe_nr
    ,t2.fylke_gruppe_besk
    ,t2.kommune_nr
    ,t2.kommune_navn
    ,t2.kommune_gruppe_nr
    ,t2.kommune_gruppe_besk
    ,t2.bydel_nr
    ,t2.bydel_navn
    ,t3.kjonn_kode
    ,t3.kjonn_flertall_besk
from {{ source ('person', 'dim_person') }} t1
left join {{ source ('kode_verk', 'dim_geografi') }} t2
    on t1.FK_dim_geografi_bosted = t2.pk_dim_geografi
left join {{ source ('kode_verk', 'dim_kjonn') }} t3
    on t1.fk_dim_kjonn = t3.pk_dim_kjonn
    where t1.utfaset = 0 
    and t2.gyldig_flagg = 1
