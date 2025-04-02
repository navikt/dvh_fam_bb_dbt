{{
    config(
        materialized = 'incremental'
    )
}}

with final as (
    select 
    nvl(b.fk_person1, -1) fk_person1
    ,b.skjermet_kode
    ,PERIODE
    ,SAKSNR
    ,REC_TYPE
    ,TKNR_BOST
    ,FNR
    ,ALDER
    ,KJONN
    ,BOLAND
    ,STONAD_STAT
    ,SAKSTYP
    ,EKBIDR_G
    ,OBJNR
    ,ANTBARNB
    ,EKBIDR
    ,BIDRSUM
    ,LOPSAK
    ,UTLVAL
    ,LAND
    ,LASTET_DATO
from {{ source ('fam_bb', 'STG_FAM_BB_MOTTAKER_BIS') }} 
left outer join {{ source ('dt_person_arena', 'ident_off_id_til_fk_person1') }} b 
ON FNR=b.off_id
    and b.gyldig_fra_dato<=to_date(periode|| '01','yyyymmdd')
    and b.gyldig_til_dato>=to_date(periode|| '01','yyyymmdd')
    and b.skjermet_kode=0
where periode =  (select TO_CHAR (ADD_MONTHS (SYSDATE, -1), 'YYYYMM') from dual)
)

select 
f.PERIODE
,f.REC_TYPE
,case when f.skjermet_kode = 6 or f.skjermet_kode = 7 then  -1
        else f.fk_person1
   end fk_person1
,case when f.skjermet_kode = 6 or f.skjermet_kode = 7 then  null
      when f.fk_person1 = -1 then f.FNR
      else null
   end FNR
,f.skjermet_kode
,f.SAKSNR
,f.TKNR_BOST
,f.ALDER
,f.KJONN
,f.BOLAND
,f.STONAD_STAT
,f.SAKSTYP
,f.EKBIDR_G
,f.OBJNR
,f.ANTBARNB
,f.EKBIDR
,f.BIDRSUM
,f.LOPSAK
,f.UTLVAL
,f.LAND
,f.LASTET_DATO
,p.pk_dim_person fk_dim_person
from final f
left join {{ source ('dt_person_arena', 'dim_person') }} p
on f.fk_person1 = p.fk_person1
    and p.gyldig_fra_dato<=to_date(f.periode, 'yyyymm')
    and p.gyldig_til_dato>=to_date(f.periode, 'yyyymm')
    and p.K67_FLAGG=0
    and f.fk_person1 <> -1