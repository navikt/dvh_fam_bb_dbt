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
    ,INNB_TOTALT
    ,INNB_UFORDELT
    ,INNB_11_21
    ,INNB_32
    ,INNB_42
    ,INNB_31_41
    ,INNB_12_22
    ,F_PAALOP
    ,SUM_PAALOP
    ,SUM_GJELD
    ,GJELD_11_21
    ,GJELD_32
    ,GJELD_42
    ,GJELD_31_41
    ,GJELD_12_22
    ,LOPSAK
    ,SAKSTYP
    ,OBJNR
    ,ANTBARNB
    ,BIDRSUM
    ,UTLVAL
    ,LAND
    ,LASTET_DATO
from {{ source ('fam_bb', 'STG_FAM_BB_PLIKTIGE_BIS') }} 
left outer join {{ source ('dt_person_arena', 'ident_off_id_til_fk_person1') }} b 
on FNR=b.off_id
    and b.gyldig_fra_dato<=to_date(PERIODE|| '01','yyyymmdd')
    and b.gyldig_til_dato>=to_date(PERIODE|| '01','yyyymmdd')
    and b.skjermet_kode=0
where periode =  (select TO_CHAR (ADD_MONTHS (SYSDATE, -1), 'YYYYMM') from dual)
)

select 
f.PERIODE 
,f.SAKSNR
,f.REC_TYPE
,case when f.skjermet_kode = 6 or f.skjermet_kode = 7 then  -1
        else f.fk_person1
   end fk_person1
,case when f.skjermet_kode = 6 or f.skjermet_kode = 7 then  null
      when f.fk_person1 = -1 then f.FNR
      else null
   end FNR
,f.skjermet_kode
,f.TKNR_BOST
,f.ALDER
,f.KJONN
,f.BOLAND
,f.INNB_TOTALT
,f.INNB_UFORDELT
,f.INNB_11_21
,f.INNB_32
,f.INNB_42
,f.INNB_31_41
,f.INNB_12_22
,f.F_PAALOP
,f.SUM_PAALOP
,f.SUM_GJELD
,f.GJELD_11_21
,f.GJELD_32
,f.GJELD_42
,f.GJELD_31_41
,f.GJELD_12_22
,f.LOPSAK
,f.SAKSTYP
,f.OBJNR
,f.ANTBARNB
,f.BIDRSUM
,f.UTLVAL
,f.LAND
,f.LASTET_DATO
,p.pk_dim_person fk_dim_person
from final f
left join {{ source ('dt_person_arena', 'dim_person') }} p
on f.fk_person1 = p.fk_person1
    and p.gyldig_fra_dato<=to_date(f.PERIODE, 'yyyymm')
    and p.gyldig_til_dato>=to_date(f.PERIODE, 'yyyymm')
    and p.K67_FLAGG=0
    and f.fk_person1 <> -1