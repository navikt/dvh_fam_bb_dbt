periode_uten_opphort as (
 
  select aar_maaned, fk_person1_kravhaver, fk_person1_mottaker, fk_person1_skyldner, vedtakstidspunkt
        ,pk_bb_fagsak as fk_bb_fagsak, saksnr
        ,vedtaks_id, behandlings_type, pk_bb_bidrags_periode as fk_bb_bidrags_periode
        ,periode_fra, periode_til, belop, resultat
        ,periode_fra_opphor, aar
        ,bidragsevne, underholdskostnad, samvaersfradrag, netto_barnetillegg_bp, netto_barnetillegg_bm,
        samvaersklasse, bps_andel_underholdskostnad, bpbor_med_andre_voksne
        --,TO_DATE(TO_CHAR(LAST_DAY(SYSDATE), 'YYYYMMDD'), 'YYYYMMDD') MAX_VEDTAKSDATO --Input max_vedtaksdato
        ,to_date('{{ var ("max_vedtaksdato") }}', 'yyyymmdd') max_vedtaksdato
        ,fk_dim_tid_mnd
        ,'{{ var ("periode_type") }}' periode_type --Input periode_type
        ,dim_kravhaver.pk_dim_person as fk_dim_person_kravhaver
        ,floor(months_between(vedtak.siste_dato_i_perioden, dim_kravhaver.fodt_dato)/12) alder_kravhaver
        ,case 
            when dim_kravhaver.kjonn_nr = 1 then 'M'
            when dim_kravhaver.kjonn_nr = 0 then 'K'
            else 'U'
        end kjonn_kravhaver        
        
        ,dim_mottaker.pk_dim_person as fk_dim_person_mottaker
        ,dim_mottaker.bosted_kommune_nr as bosted_kommune_nr_mottaker
        ,dim_mottaker.fk_dim_land_statsborgerskap as fk_dim_land_statsborgerskap_mottaker
        ,dim_mottaker.fk_dim_geografi_bosted as fk_dim_geografi_bosted_mottaker
        ,floor(months_between(vedtak.siste_dato_i_perioden, dim_mottaker.fodt_dato)/12) alder_mottaker
        
        ,dim_skyldner.pk_dim_person as fk_dim_person_skyldner
        ,dim_skyldner.bosted_kommune_nr as bosted_kommune_nr_skyldner
        ,dim_skyldner.fk_dim_land_statsborgerskap as fk_dim_land_statsborgerskap_skyldner
        ,dim_skyldner.fk_dim_geografi_bosted as fk_dim_geografi_bosted_skyldner
        ,floor(months_between(vedtak.siste_dato_i_perioden, dim_skyldner.fodt_dato)/12) alder_skyldner
        
        ,inntekts_typer.p_type_inntekt_1,inntekts_typer.p_inntekt_1
        ,inntekts_typer.p_type_inntekt_2,inntekts_typer.p_inntekt_2,inntekts_typer.p_type_inntekt_3,inntekts_typer.p_inntekt_3
        ,inntekts_typer.p_inntekt_total, inntekts_typer.p_antall_typer
        
        ,inntekts_typer.m_type_inntekt_1
        ,inntekts_typer.m_inntekt_1, inntekts_typer.m_type_inntekt_2, inntekts_typer.m_inntekt_2
        ,inntekts_typer.m_type_inntekt_3, inntekts_typer.m_inntekt_3, inntekts_typer.m_type_inntekt_4
        ,inntekts_typer.m_inntekt_4,inntekts_typer.m_type_inntekt_5, inntekts_typer.m_inntekt_5
        ,inntekts_typer.m_type_inntekt_6,inntekts_typer.m_inntekt_6
        ,inntekts_typer.m_inntekt_total, inntekts_typer.m_antall_typer
        ,'{{ var ("gyldig_flagg") }}' as gyldig_flagg --Input gyldig_flagg
        ,localtimestamp as lastet_dato
  from ref('periode_opphor_bidrag') opphor_hvis_finnes vedtak
 
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

  left join ref('inntekt_bidrag')
  on vedtak.pk_bb_bidrags_periode = inntekts_typer.fk_bb_bidrags_periode
)

select 
    * 
from periode_uten_opphort