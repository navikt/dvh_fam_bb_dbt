{{
    config(
        materialized = 'incremental'
    )
}}

with final as (
select 
    PERIODE
    ,SAKSNR
    ,REC_TYPE
    ,FNR
    ,nvl(b.fk_person1, -1) fk_person1
    ,b.skjermet_kode
    ,VEDTDATO
    ,SKATTFOR
    ,INNTTYP1
    ,INNTTYP2
    ,INNTTYP3
    ,INNTTYP4
    ,INNTTYP5
    ,KONTST
    ,INNTBEL1
    ,INNTBEL2
    ,INNTBEL3
    ,INNTBEL4
    ,INNTBEL5
    ,KONTSBEL
    ,BTILRED
    ,BARNETILBEL
    ,BARNETILFORS
    ,LASTET_DATO
from {{ source ('fam_bb', 'STG_FAM_BB_MOTTAKER_BERM') }} 
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
    ,f.VEDTDATO
    ,f.SKATTFOR
    ,f.INNTTYP1
    ,f.INNTTYP2
    ,f.INNTTYP3
    ,f.INNTTYP4
    ,f.INNTTYP5
    ,f.KONTST
    ,f.INNTBEL1
    ,f.INNTBEL2
    ,f.INNTBEL3
    ,f.INNTBEL4
    ,f.INNTBEL5
    ,f.KONTSBEL
    ,f.BTILRED
    ,f.BARNETILBEL
    ,f.BARNETILFORS
    ,f.LASTET_DATO
    ,p.pk_dim_person fk_dim_person
from final f
left join {{ source ('dt_person_arena', 'dim_person') }} p
on f.fk_person1 = p.fk_person1
    and p.gyldig_fra_dato<=to_date(f.PERIODE, 'yyyymm')
    and p.gyldig_til_dato>=to_date(f.PERIODE, 'yyyymm')
    and p.K67_FLAGG=0
    and f.fk_person1 <> -1