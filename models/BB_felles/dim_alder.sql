{{ config(materialized='ephemeral') }}

select alder
,ti_aar_gruppe_besk
,alder_gruppe5_besk
,alder_gruppe7_besk
from {{ source ('kode_verk', 'dim_alder') }}
where gyldig_flagg = 1