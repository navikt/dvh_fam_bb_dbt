{{
    config(
        materialized = 'incremental'
    )
}}

with pre_final as (
select 
    PERIODE
    ,SAKSNR
    ,REC_TYPE
    ,nvl(b.fk_person1, -1) fk_person1_barn
    ,b.skjermet_kode
    ,OBJNR
    ,TKNR_BOST
    ,FNR
    ,ALDER
    ,KJONN
    ,INSTITUSJON
    ,BIDRAG_G
    ,FORSK
    ,SBBEL
    ,SUM_P_KRAV
    ,F_FORSKUDD
    ,ETTERG_BEL
    ,ANT_ETTERG
    ,MOID
    ,MOTYPE
    ,SAKSTYP
    ,SAKSDATO
    ,BOLAND
    ,BPFNR
    ,BMFNR
    ,FORSKUDD
    ,HGBOST
    ,UGBOST
    ,RESKODE
    ,SBBELHI
    ,BIDRAG
    ,LOPSAK
    ,UTLVAL
    ,VEDTDATOBB
    ,VIRKDATOBB
    ,VEDTDATOFO
    ,VIRKDATOFO
    ,LASTET_DATO
from {{ source ('fam_bb', 'STG_FAM_BB_BARN_BIS') }} 
left outer join {{ source ('dt_person_arena', 'ident_off_id_til_fk_person1') }} b 
on fnr=b.off_id
    and b.gyldig_fra_dato<=to_date(periode|| '01','yyyymmdd')
    and b.gyldig_til_dato>=to_date(periode|| '01','yyyymmdd')
    and b.skjermet_kode=0   
where periode =  (select TO_CHAR (ADD_MONTHS (SYSDATE, -1), 'YYYYMM') from dual)
),

mottaker_fk as (
    select pre.*
    ,nvl(m.fk_person1, -1) fk_person1_mottaker
    ,m.skjermet_kode skjermet_kode_mottaker
    from pre_final pre
    left outer join {{ source ('dt_person_arena', 'ident_off_id_til_fk_person1') }} m on
        BMFNR=m.off_id
        and m.gyldig_fra_dato<=to_date(periode|| '01','yyyymmdd')
        and m.gyldig_til_dato>=to_date(periode|| '01','yyyymmdd')
        and m.skjermet_kode=0
),
pliktig_fk as (
    select m.*
    ,nvl(p.fk_person1, -1) fk_person1_pliktig
    ,p.skjermet_kode skjermet_kode_pliktig
    from mottaker_fk m
  left outer join {{ source ('dt_person_arena', 'ident_off_id_til_fk_person1') }} p 
  on BPFNR=p.off_id
    and p.gyldig_fra_dato<=to_date(periode|| '01','yyyymmdd')
    and p.gyldig_til_dato>=to_date(periode|| '01','yyyymmdd')
    and p.skjermet_kode=0
)

select 
    f.PERIODE
    ,f.SAKSNR
    ,f.REC_TYPE
    ,case when f.skjermet_kode = 6 or f.skjermet_kode = 7 then  -1
            else f.fk_person1_barn
       end fk_person1_barn
    ,case when f.skjermet_kode = 6 or f.skjermet_kode = 7 then  null
      when f.fk_person1_barn = -1 then f.fnr
      else null
    end fnr
    ,f.skjermet_kode
    ,f.OBJNR
   ,f.TKNR_BOST
   ,f.ALDER
   ,f.KJONN
   ,f.INSTITUSJON
   ,f.BIDRAG_G
   ,f.FORSK
   ,f.SBBEL
   ,f.SUM_P_KRAV
   ,f.F_FORSKUDD
   ,f.ETTERG_BEL
   ,f.ANT_ETTERG
   ,f.MOID
   ,f.MOTYPE
   ,f.SAKSTYP
   ,f.SAKSDATO
   ,f.BOLAND
    ,case when f.skjermet_kode_mottaker = 6 or f.skjermet_kode_mottaker = 7 then  -1
            else f.fk_person1_mottaker
       end fk_person1_mottaker
    ,case when f.skjermet_kode_mottaker = 6 or f.skjermet_kode_mottaker = 7 then  null
      when f.fk_person1_mottaker = -1 then f.BMFNR
      else null
    end BMFNR
    ,case when f.skjermet_kode_pliktig = 6 or f.skjermet_kode_pliktig = 7 then  -1
            else f.fk_person1_pliktig
       end fk_person1_pliktig
    ,case when f.skjermet_kode_pliktig = 6 or f.skjermet_kode_pliktig = 7 then  null
      when f.fk_person1_pliktig = -1 then f.BPFNR
      else null
    end BPFNR
   ,f.FORSKUDD
   ,f.HGBOST
   ,f.UGBOST
   ,f.RESKODE
   ,f.SBBELHI
   ,f.BIDRAG
   ,f.LOPSAK
   ,f.UTLVAL
   ,f.VEDTDATOBB
   ,f.VIRKDATOBB
   ,f.VEDTDATOFO
   ,f.VIRKDATOFO
   ,f.LASTET_DATO
   ,b.pk_dim_person fk_dim_person_barn
   ,m.pk_dim_person fk_dim_person_mottaker
   ,p.pk_dim_person fk_dim_person_pliktig
from pliktig_fk f
left join {{ source ('dt_person_arena', 'dim_person') }} b
on f.fk_person1_barn = b.fk_person1
    and b.gyldig_fra_dato<=to_date(f.periode, 'yyyymm')
    and b.gyldig_til_dato>=to_date(f.periode, 'yyyymm')
    and b.K67_FLAGG=0
    and f.fk_person1_barn <> -1

left join {{ source ('dt_person_arena', 'dim_person') }} m
on f.fk_person1_mottaker = m.fk_person1
    and m.gyldig_fra_dato<=to_date(f.periode, 'yyyymm')
    and m.gyldig_til_dato>=to_date(f.periode, 'yyyymm')
    and m.K67_FLAGG=0
    and f.fk_person1_mottaker <> -1
    
left join {{ source ('dt_person_arena', 'dim_person') }} p
on f.fk_person1_pliktig = p.fk_person1
    and p.gyldig_fra_dato<=to_date(f.periode, 'yyyymm')
    and p.gyldig_til_dato>=to_date(f.periode, 'yyyymm')
    and p.K67_FLAGG=0
    and f.fk_person1_pliktig <> -1