{{ config(
    materialized='incremental',
    unique_key = ['aar_maaned', 'gyldig_flagg'],
    incremental_strategy='delete+insert'
    ) 
}}

with periode_uten_opphort as (
 
  select aar_maaned, fk_person1_kravhaver, fk_person1_mottaker, fk_person1_skyldner, vedtakstidspunkt
        ,pk_bb_fagsak as fk_bb_fagsak, saksnr
        ,vedtaks_id, behandlings_type, pk_bb_bidrags_periode as fk_bb_bidrags_periode
        ,periode_fra, periode_til, belop as belop_vedtak
        ,resultat
        ,periode_fra_opphor, aar, STONADSTYPE, NETTO_TILSYNSUTGIFT, FAKTISK_TILSYNSUTGIFT, INNKREVING_FLAGG
        ,BIDRAGSEVNE, UNDERHOLDSKOSTNAD, SAMVAERSFRADRAG, NETTO_BARNETILLEGG_BP, NETTO_BARNETILLEGG_BM,
        SAMVAERSKLASSE, BPS_ANDEL_UNDERHOLDSKOSTNAD, BPBOR_MED_ANDRE_VOKSNE, valutakode
        --,TO_DATE(TO_CHAR(LAST_DAY(SYSDATE), 'YYYYMMDD'), 'YYYYMMDD') MAX_VEDTAKSDATO --Input max_vedtaksdato
        ,TO_DATE('{{ var ("max_vedtaksdato") }}', 'yyyymmdd') MAX_VEDTAKSDATO
        ,fk_dim_tid_mnd
        ,'{{ var ("periode_type") }}' periode_type --Input periode_type
        ,dim_kravhaver.pk_dim_person as fk_dim_person_kravhaver
        ,floor(months_between(vedtak.siste_dato_i_perioden, dim_kravhaver.fodt_dato)/12) alder_kravhaver
        
        ,case 
            when dim_kravhaver.kjonn_nr = 1 then 'M'
            when dim_kravhaver.kjonn_nr = 0 then 'K'			
        end kjonn_kravhaver   
		
        ,case 
            when vedtak.valutakode != 'NOK' then 
                belop * (
                    select valutakurser
                    from norges_bank_valuta nb
                    where nb.base_cur = vedtak.valutakode
                      and nb.periode <= vedtak.aar_maaned
                    order by nb.periode desc fetch first 1 row only
            )
            else belop
        end belop
		
        ,1234 SISTE_KOMPLETT_VEDTAK
        ,TO_TIMESTAMP('30.06.2025 07:03:43.360199', 'DD.MM.YYYY HH24:MI:SS.FF6') SISTE_KOMPLETT_VEDTAKSTIDSPUNKT
        
        ,dim_mottaker.pk_dim_person as fk_dim_person_mottaker
        ,case 
            when dim_mottaker.kjonn_nr = 1 then 'M'
            when dim_mottaker.kjonn_nr = 0 then 'K'
        end kjonn_mottaker  
        ,dim_mottaker.bosted_kommune_nr as bosted_kommune_nr_mottaker
        ,dim_mottaker.fk_dim_land_statsborgerskap as fk_dim_land_statsborgerskap_mottaker
        ,dim_mottaker.fk_dim_geografi_bosted as fk_dim_geografi_bosted_mottaker
        ,floor(months_between(vedtak.siste_dato_i_perioden, dim_mottaker.fodt_dato)/12) alder_mottaker
        
        ,dim_skyldner.pk_dim_person as fk_dim_person_skyldner
        ,dim_skyldner.bosted_kommune_nr as bosted_kommune_nr_skyldner
        ,dim_skyldner.fk_dim_land_statsborgerskap as fk_dim_land_statsborgerskap_skyldner
        ,dim_skyldner.fk_dim_geografi_bosted as fk_dim_geografi_bosted_skyldner
        ,floor(months_between(vedtak.siste_dato_i_perioden, dim_skyldner.fodt_dato)/12) alder_skyldner
        
        ,inntekts_typer.P_TYPE_INNTEKT_1,inntekts_typer.P_inntekt_1
        ,inntekts_typer.P_TYPE_INNTEKT_2,inntekts_typer.P_inntekt_2,inntekts_typer.P_TYPE_INNTEKT_3,inntekts_typer.P_inntekt_3
        ,inntekts_typer.P_INNTEKT_TOTAL, inntekts_typer.P_ANTALL_TYPER
        
        ,inntekts_typer.M_TYPE_INNTEKT_1
        ,inntekts_typer.M_inntekt_1, inntekts_typer.M_TYPE_INNTEKT_2, inntekts_typer.M_inntekt_2
        ,inntekts_typer.M_type_inntekt_3, inntekts_typer.M_inntekt_3, inntekts_typer.M_type_inntekt_4
        ,inntekts_typer.M_inntekt_4,inntekts_typer.M_type_inntekt_5, inntekts_typer.M_inntekt_5
        ,inntekts_typer.M_INNTEKT_TOTAL, inntekts_typer.M_ANTALL_TYPER
																	  
        ,'{{ var ("gyldig_flagg") }}' as gyldig_flagg --Input gyldig_flagg
        ,localtimestamp AS lastet_dato
  from {{ ref('periode_opphor_bidrag') }} vedtak

  left join dt_person.dim_person dim_kravhaver
  on dim_kravhaver.fk_person1 = vedtak.fk_person1_kravhaver
  and vedtak.fk_person1_kravhaver != -1
  and vedtak.siste_dato_i_perioden between dim_kravhaver.gyldig_fra_dato and dim_kravhaver.gyldig_til_dato
 
  left join dt_person.dim_person dim_mottaker
  on dim_mottaker.fk_person1 = vedtak.fk_person1_mottaker
  and vedtak.fk_person1_mottaker != -1
  and vedtak.siste_dato_i_perioden between dim_mottaker.gyldig_fra_dato and dim_mottaker.gyldig_til_dato
  
  left join dt_person.dim_person dim_skyldner
  on dim_skyldner.fk_person1 = vedtak.fk_person1_skyldner
  and vedtak.fk_person1_skyldner != -1
  and vedtak.siste_dato_i_perioden between dim_skyldner.gyldig_fra_dato and dim_skyldner.gyldig_til_dato

  left join {{ ref("inntekt_bidrag") }} inntekts_typer

  on vedtak.pK_BB_BIDRAGS_PERIODE = inntekts_typer.FK_BB_BIDRAGS_PERIODE
 
  where siste_dato_i_perioden < nvl(periode_fra_opphor, siste_dato_i_perioden+1)
)

select 
    * 
from periode_uten_opphort