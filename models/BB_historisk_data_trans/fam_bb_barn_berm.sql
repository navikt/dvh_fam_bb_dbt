{{
    config(
        materialized = 'incremental'
    )
}}

with final as (
select 
    bb_barn.PERIODE
    ,bb_barn.SAKSNR
    ,bb_barn.REC_TYPE
    ,bb_barn.OBJNR
    ,bb_barn.FNR
    ,nvl(b.fk_person1, -1) fk_person1
    ,b.skjermet_kode
    ,bb_barn.VEDTDATO
    ,bb_barn.BIDRBE
    ,bb_barn.BIDRBEL
    ,bb_barn.BIDRTIL
    ,bb_barn.BIDRES
    ,bb_barn.HGBERM
    ,bb_barn.UGBERM
    ,bb_barn.AARSAK
    ,bb_barn.SAMV
    ,bb_barn.UNDERH
    ,bb_barn.INNTTYP1
    ,bb_barn.INNTTYP2
    ,bb_barn.INNTTYP3
    ,bb_barn.INNTTYP4
    ,bb_barn.INNTTYP5
    ,bb_barn.KONTST
    ,bb_barn.INNTBEL1
    ,bb_barn.INNTBEL2
    ,bb_barn.INNTBEL3
    ,bb_barn.INNTBEL4
    ,bb_barn.INNTBEL5
    ,bb_barn.KONTSBEL
    ,bb_barn.BTILRED
    ,bb_barn.HD
    ,bb_barn.STDEKN
    ,bb_barn.BTBEL
    ,bb_barn.BTFAK
    ,bb_barn.FORPL
    ,bb_barn.TELLER
    ,bb_barn.NEVNER
    ,bb_barn.BPDELU
    ,bb_barn.VIRKDATOBB
    ,bb_barn.VEDTDATOFO
    ,bb_barn.VIRKDATOFO
    ,bb_barn.BPPROS
    ,bb_barn.BTSKODE
    ,bb_barn.LASTET_DATO
from {{ source ('fam_bb', 'STG_FAM_BB_BARN_BERM') }} bb_barn
left outer join {{ source ('dt_person_arena', 'ident_off_id_til_fk_person1') }} b 
on bb_barn.fnr=b.off_id
    and b.gyldig_fra_dato<=to_date(bb_barn.periode|| '01','yyyymmdd')
    and b.gyldig_til_dato>=to_date(bb_barn.periode|| '01','yyyymmdd')
    and b.skjermet_kode=0
where periode =  (select TO_CHAR (ADD_MONTHS (SYSDATE, -1), 'YYYYMM') from dual)
)

select 
f.PERIODE
,f.SAKSNR
,f.REC_TYPE
,f.OBJNR
,case when f.skjermet_kode = 6 or f.skjermet_kode = 7 then  -1
        else f.fk_person1
   end fk_person1
,case when f.skjermet_kode = 6 or f.skjermet_kode = 7 then  null
      when f.fk_person1 = -1 then f.fnr
      else null
   end fnr
,f.skjermet_kode
,f.VEDTDATO
,f.BIDRBE
,f.BIDRBEL
,f.BIDRTIL
,f.BIDRES
,f.HGBERM
,f.UGBERM
,f.AARSAK
,f.SAMV
,f.UNDERH
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
,f.HD
,f.STDEKN
,f.BTBEL
,f.BTFAK
,f.FORPL
,f.TELLER
,f.NEVNER
,f.BPDELU
,f.VIRKDATOBB
,f.VEDTDATOFO
,f.VIRKDATOFO
,f.BPPROS
,f.BTSKODE
,f.LASTET_DATO
,p.pk_dim_person fk_dim_person
from final f
left join {{ source ('dt_person_arena', 'dim_person') }} p
on f.fk_person1 = p.fk_person1
    and p.gyldig_fra_dato<=to_date(f.periode, 'yyyymm')
    and p.gyldig_til_dato>=to_date(f.periode, 'yyyymm')
    and p.K67_FLAGG=0
    and f.fk_person1 <> -1