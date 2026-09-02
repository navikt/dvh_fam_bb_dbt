{{ config(materialized='ephemeral') }}

select 
    --,statsborgerskap
    --,bosted_land
    t1.pk_dim_tid as fk_dim_tid
    ,t1.aar
    ,t1.maaned
    ,t1.aar_maaned
    ,t1.aar_maaned_besk_kort
    ,t1.aar_maaned_besk
    ,t1.kvartal_besk
    ,t1.aar_kvartal_besk_kort
    ,t1.aar_kvartal_besk
    ,t1.aar_tertial_besk_kort
    ,t1.aar_tertial_besk
    ,t1.aar_halvaar_besk_kort
    ,t1.aar_halvaar_besk
    ,t2.pk_dim_geografi as fk_dim_geografi
    ,t2.fylke_nr
    ,t2.fylke_navn
    ,t2.fylke_nr_navn
    ,t2.fylke_gruppe_nr
    ,t2.fylke_gruppe_besk
    ,t2.kommune_nr
    ,t2.kommune_navn
    ,t2.kommune_nr_navn
    ,t2.kommune_gruppe_nr
    ,t2.kommune_gruppe_besk
    ,t2.bydel_nr
    ,t2.bydel_navn
    ,t2.bydel_nr_navn
    ,t3.pk_dim_kjonn as fk_dim_kjonn
    ,t3.kjonn_kode
    ,t3.kjonn_flertall_besk
    ,t4.pk_dim_alder as fk_dim_alder
    ,t4.alder
    ,t4.ti_aar_gruppe_besk
    ,t4.alder_gruppe5_besk
    ,t4.alder_gruppe7_besk
from {{ source ('kode_verk', 'dim_tid') }} t1
CROSS JOIN  {{ source ('kode_verk', 'dim_geografi') }} t2
CROSS JOIN {{ source ('kode_verk', 'dim_kjonn') }} t3
CROSS JOIN {{ source ('kode_verk', 'dim_alder') }} t4
where t1.gyldig_flagg = 1
and t2.gyldig_flagg = 1
and t3.gyldig_flagg = 1
and t4.gyldig_flagg = 1
and  t1.dim_nivaa = 3 
and t1.aar = '2026' 