{% set start_date = 202001 %}
{% set end_date = 202508 %}
{% set periods = [] %}

{{ config(
    materialized='incremental',
    unique_key = ['aar_maaned', 'gyldig_flagg'],
    incremental_strategy='delete+insert',
    vars={}
) }}

{% for year_month in range(start_date, end_date + 1) %}
    {% set year = year_month // 100 %}
    {% set month = year_month % 100 %}

    {% if month >= 1 and month <= 12 %}
        {% set formatted_month = "{:02d}".format(month) %}
        {% set max_vedtaksdato = regner_max_vedtaksdato(year, month) %}
        {{ log("Running for periode_fom: " ~ year_month ~ ", max_vedtaksdato: " ~ max_vedtaksdato, info=True) }}

        {% set periode_fom = year_month %}
        {% set periode_tom = year_month %}
        {% set max_vedtaksdato_var = max_vedtaksdato %}
        {% set periode_type = 'M' %}
        {% set gyldig_flagg = 1 %}

        {% do periods.append({
            'periode_fom': periode_fom,
            'periode_tom': periode_tom,
            'max_vedtaksdato': max_vedtaksdato_var,
            'periode_type': periode_type,
            'gyldig_flagg': gyldig_flagg
        }) %}
    {% endif %}
{% endfor %}

{% for period in periods %}
    {% if not loop.first %} UNION ALL {% endif %}
    SELECT /*+ parallel(64) */
        aar_maaned,
        fk_person1_kravhaver,
        fk_person1_mottaker,
        fk_person1_skyldner,
        vedtakstidspunkt,
        pk_bb_fagsak AS fk_bb_fagsak,
        saksnr,
        vedtaks_id,
        behandlings_type,
        pk_bb_bidrags_periode AS fk_bb_bidrags_periode,
        periode_fra,
        periode_til,
        belop,
        resultat,
        periode_fra_opphor,
        aar,
        STONADSTYPE, 
        NETTO_TILSYNSUTGIFT,
        FAKTISK_TILSYNSUTGIFT, 
        INNKREVING_FLAGG,
        BIDRAGSEVNE,
        UNDERHOLDSKOSTNAD,
        SAMVAERSFRADRAG,
        NETTO_BARNETILLEGG_BP,
        NETTO_BARNETILLEGG_BM,
        SAMVAERSKLASSE,
        BPS_ANDEL_UNDERHOLDSKOSTNAD,
        BPBOR_MED_ANDRE_VOKSNE,
        valutakode,
        TO_DATE('{{ period['max_vedtaksdato'] }}', 'YYYYMMDD') MAX_VEDTAKSDATO,
        fk_dim_tid_mnd,
        '{{ period['periode_type'] }}' periode_type,
        dim_kravhaver.pk_dim_person AS fk_dim_person_kravhaver,
        FLOOR(MONTHS_BETWEEN(siste_dato_i_perioden, dim_kravhaver.fodt_dato)/12) alder_kravhaver,
        
        case 
            when dim_kravhaver.kjonn_nr = 1 then 'M'
            when dim_kravhaver.kjonn_nr = 0 then 'K'
        end kjonn_kravhaver, 

        1234 SISTE_KOMPLETT_VEDTAK,
        TO_TIMESTAMP('30.06.2025 07:03:43.360199', 'DD.MM.YYYY HH24:MI:SS.FF6') SISTE_KOMPLETT_VEDTAKSTIDSPUNKT,
        

        dim_mottaker.pk_dim_person AS fk_dim_person_mottaker,
        case 
            when dim_mottaker.kjonn_nr = 1 then 'M'
            when dim_mottaker.kjonn_nr = 0 then 'K'
        end kjonn_mottaker,
        dim_mottaker.bosted_kommune_nr AS bosted_kommune_nr_mottaker,
        dim_mottaker.fk_dim_land_statsborgerskap AS fk_dim_land_statsborgerskap_mottaker,
        dim_mottaker.fk_dim_geografi_bosted AS fk_dim_geografi_bosted_mottaker,
        FLOOR(MONTHS_BETWEEN(siste_dato_i_perioden, dim_mottaker.fodt_dato)/12) alder_mottaker,
        
        dim_skyldner.pk_dim_person AS fk_dim_person_skyldner,
        dim_skyldner.bosted_kommune_nr AS bosted_kommune_nr_skyldner,
        dim_skyldner.fk_dim_land_statsborgerskap AS fk_dim_land_statsborgerskap_skyldner,
        dim_skyldner.fk_dim_geografi_bosted AS fk_dim_geografi_bosted_skyldner,
        FLOOR(MONTHS_BETWEEN(siste_dato_i_perioden, dim_skyldner.fodt_dato)/12) alder_skyldner,
        
        P_TYPE_INNTEKT_1,P_inntekt_1,P_TYPE_INNTEKT_2,P_inntekt_2,
        P_TYPE_INNTEKT_3,P_inntekt_3,P_INNTEKT_TOTAL,P_ANTALL_TYPER,
        M_TYPE_INNTEKT_1,M_inntekt_1,M_TYPE_INNTEKT_2,M_inntekt_2,
        M_type_inntekt_3,M_inntekt_3,M_type_inntekt_4,M_inntekt_4,
        M_type_inntekt_5,M_inntekt_5,M_INNTEKT_TOTAL,M_ANTALL_TYPER,
        '{{ period['gyldig_flagg'] }}' AS gyldig_flagg,
        LOCALTIMESTAMP AS lastet_dato
    FROM (
        WITH tid AS (
            SELECT aar_maaned, siste_dato_i_perioden, aar, pk_dim_tid
            FROM {{ source('kode_verk', 'dim_tid') }}
            WHERE gyldig_flagg = 1
            AND dim_nivaa = 3
            AND aar_maaned BETWEEN '{{ period['periode_fom'] }}' AND '{{ period['periode_tom'] }}'
        ),

        fagsak AS (
            SELECT
                fagsak.pk_bb_fagsak,
                fagsak.vedtaks_id,
                fagsak.behandlings_type,
                fagsak.vedtakstidspunkt,
                fagsak.saksnr,
                fagsak.innkreving_flagg,
                fagsak.STONADSTYPE,
                fagsak.fk_person1_kravhaver,
                fagsak.fk_person1_mottaker,
                fagsak.fk_person1_skyldner,


                periode.pk_bb_bidrags_periode,
                periode.periode_fra,
                periode.periode_til,
                periode.belop,
                periode.resultat,
                periode.BIDRAGSEVNE,
                periode.UNDERHOLDSKOSTNAD,
                periode.netto_tilsynsutgift,
                periode.faktisk_tilsynsutgift,
                periode.SAMVAERSFRADRAG,
                periode.NETTO_BARNETILLEGG_BP,
                periode.NETTO_BARNETILLEGG_BM,
                periode.SAMVAERSKLASSE,
                periode.BPS_ANDEL_UNDERHOLDSKOSTNAD,
                periode.BPBOR_MED_ANDRE_VOKSNE,
                periode.valutakode,

                tid.aar_maaned,
                tid.siste_dato_i_perioden,
                tid.aar,
                tid.pk_dim_tid AS fk_dim_tid_mnd,
                row_number() over (partition by tid.aar_maaned, decode(fagsak.fk_person1_kravhaver,-1,ident_krav.fk_person1,fagsak.fk_person1_kravhaver) ,fagsak.saksnr, fagsak.stonadstype
                    order by fagsak.vedtakstidspunkt desc, periode.belop desc) nr
            
            FROM {{ source('fam_bb', 'fam_bb_fagsak_ord') }} fagsak
            JOIN {{ source('fam_bb', 'fam_bb_bidrags_periode_ord') }} periode
            ON fagsak.pk_bb_fagsak = periode.fk_bb_fagsak
            
            JOIN tid
            ON periode.periode_fra <= TO_DATE(tid.aar_maaned || '01', 'YYYYMMDD')
            AND NVL(periode.periode_til, tid.siste_dato_i_perioden) >= tid.siste_dato_i_perioden
            
            left join dt_person.ident_off_id_til_fk_person1 ident_krav
            on fagsak.fnr_kravhaver = ident_krav.off_id
            and fagsak.fk_person1_kravhaver = -1
            and tid.siste_dato_i_perioden between ident_krav.gyldig_fra_dato and ident_krav.gyldig_til_dato

            
            WHERE fagsak.behandlings_type NOT IN ('ENDRING_MOTTAKER', 'OPPHØR', 'ALDERSOPPHØR')
            AND TRUNC(fagsak.vedtakstidspunkt, 'DD') <= TO_DATE('{{ period['max_vedtaksdato'] }}', 'YYYYMMDD')
            and fagsak.INNKREVING_FLAGG = '1'
        
        ),

        siste AS (
            SELECT *
            FROM fagsak
            WHERE nr = 1
        ),

        opphor_fra AS (
            SELECT
                fagsak.fk_person1_kravhaver,
                fagsak.saksnr,
                fagsak.vedtakstidspunkt,
                fagsak.stonadstype,
                MIN(periode.periode_fra) periode_fra_opphor
            FROM {{ source('fam_bb', 'fam_bb_fagsak_ord') }} fagsak
            
            JOIN {{ source('fam_bb', 'fam_bb_bidrags_periode_ord') }} periode
                ON fagsak.pk_bb_fagsak = periode.fk_bb_fagsak
                AND periode.belop IS NULL

            WHERE fagsak.behandlings_type NOT IN ('ENDRING_MOTTAKER')
                AND TRUNC(fagsak.vedtakstidspunkt, 'DD') <= TO_DATE('{{ period['max_vedtaksdato'] }}', 'YYYYMMDD')
            GROUP BY fagsak.fk_person1_kravhaver, fagsak.saksnr, fagsak.vedtakstidspunkt, fagsak.stonadstype
        ),

        siste_opphør AS (
            SELECT
                siste.*,
                opphor_fra.periode_fra_opphor
            FROM siste
            LEFT JOIN opphor_fra
                ON opphor_fra.fk_person1_kravhaver = siste.fk_person1_kravhaver
                AND opphor_fra.saksnr = siste.saksnr
                AND opphor_fra.vedtakstidspunkt > siste.vedtakstidspunkt
                and opphor_fra.stonadstype = siste.stonadstype
        ),

        opphor_hvis_finnes AS (
            select aar_maaned, siste_dato_i_perioden, aar, pk_bb_fagsak,fk_dim_tid_mnd,innkreving_flagg,stonadstype,
                    vedtaks_id, behandlings_type, vedtakstidspunkt, saksnr, fk_person1_kravhaver, fk_person1_mottaker,
                    fk_person1_skyldner, pk_bb_bidrags_periode, periode_fra, periode_til, belop,netto_tilsynsutgift,faktisk_tilsynsutgift,
                    resultat, BIDRAGSEVNE, UNDERHOLDSKOSTNAD, SAMVAERSFRADRAG, NETTO_BARNETILLEGG_BP, NETTO_BARNETILLEGG_BM, 
                    SAMVAERSKLASSE, BPS_ANDEL_UNDERHOLDSKOSTNAD, BPBOR_MED_ANDRE_VOKSNE,valutakode,
                    min(periode_fra_opphor) periode_fra_opphor
            from siste_opphør

            group by aar_maaned, siste_dato_i_perioden, aar, pk_bb_fagsak,fk_dim_tid_mnd,
                    vedtaks_id, behandlings_type, vedtakstidspunkt, saksnr, fk_person1_kravhaver, fk_person1_mottaker,
                    fk_person1_skyldner, pk_bb_bidrags_periode, periode_fra, periode_til, belop,
                    resultat, BIDRAGSEVNE, UNDERHOLDSKOSTNAD, SAMVAERSFRADRAG, NETTO_BARNETILLEGG_BP, NETTO_BARNETILLEGG_BM, 
                    SAMVAERSKLASSE, BPS_ANDEL_UNDERHOLDSKOSTNAD, BPBOR_MED_ANDRE_VOKSNE,innkreving_flagg,stonadstype,
                    netto_tilsynsutgift,faktisk_tilsynsutgift,valutakode
        ),

        inntekt AS (
            SELECT  
                FK_BB_BIDRAGS_PERIODE,
                TYPE_INNTEKT,
                BELOP,
                flagg,
                ROW_NUMBER() OVER (PARTITION BY FK_BB_BIDRAGS_PERIODE, flagg ORDER BY TYPE_INNTEKT) AS NR
            FROM FAM_BB_INNTEKTS_LISTE_ORD
        ),

        inntekts_typer AS (
            SELECT
                FK_BB_BIDRAGS_PERIODE,
                MAX(CASE WHEN NR = 1 AND flagg = 'P' THEN TYPE_INNTEKT END) AS P_TYPE_INNTEKT_1,
                MAX(CASE WHEN NR = 1 AND flagg = 'P' THEN BELOP END) AS P_INNTEKT_1,
                MAX(CASE WHEN NR = 2 AND flagg = 'P' THEN TYPE_INNTEKT END) AS P_TYPE_INNTEKT_2,
                MAX(CASE WHEN NR = 2 AND flagg = 'P' THEN BELOP END) AS P_INNTEKT_2,
                MAX(CASE WHEN NR = 3 AND flagg = 'P' THEN TYPE_INNTEKT END) AS P_TYPE_INNTEKT_3,
                MAX(CASE WHEN NR = 3 AND flagg = 'P' THEN BELOP END) AS P_INNTEKT_3,
                MAX(CASE WHEN NR = 1 AND flagg = 'M' THEN TYPE_INNTEKT END) AS M_TYPE_INNTEKT_1,
                MAX(CASE WHEN NR = 1 AND flagg = 'M' THEN BELOP END) AS M_INNTEKT_1,
                MAX(CASE WHEN NR = 2 AND flagg = 'M' THEN TYPE_INNTEKT END) AS M_TYPE_INNTEKT_2,
                MAX(CASE WHEN NR = 2 AND flagg = 'M' THEN BELOP END) AS M_INNTEKT_2,
                MAX(CASE WHEN NR = 3 AND flagg = 'M' THEN TYPE_INNTEKT END) AS M_TYPE_INNTEKT_3,
                MAX(CASE WHEN NR = 3 AND flagg = 'M' THEN BELOP END) AS M_INNTEKT_3,
                MAX(CASE WHEN NR = 4 AND flagg = 'M' THEN TYPE_INNTEKT END) AS M_TYPE_INNTEKT_4,
                MAX(CASE WHEN NR = 4 AND flagg = 'M' THEN BELOP END) AS M_INNTEKT_4,
                MAX(CASE WHEN NR = 5 AND flagg = 'M' THEN TYPE_INNTEKT END) AS M_TYPE_INNTEKT_5,
                MAX(CASE WHEN NR = 5 AND flagg = 'M' THEN BELOP END) AS M_INNTEKT_5,
                SUM(CASE WHEN flagg = 'P' THEN BELOP ELSE 0 END) AS P_INNTEKT_TOTAL,
                MAX(CASE WHEN flagg = 'P' THEN NR ELSE 0 END) AS P_ANTALL_TYPER,
                SUM(CASE WHEN flagg = 'M' THEN BELOP ELSE 0 END) AS M_INNTEKT_TOTAL,
                MAX(CASE WHEN flagg = 'M' THEN NR ELSE 0 END) AS M_ANTALL_TYPER
            FROM inntekt
            GROUP BY FK_BB_BIDRAGS_PERIODE
        )

        SELECT
            vedtak.aar_maaned,
            vedtak.fk_person1_kravhaver,
            vedtak.fk_person1_mottaker,
            vedtak.fk_person1_skyldner,
            vedtak.vedtakstidspunkt,
            vedtak.pk_bb_fagsak,
            vedtak.saksnr,
            vedtak.vedtaks_id,
            vedtak.behandlings_type,
            vedtak.pk_bb_bidrags_periode,
            vedtak.periode_fra,
            vedtak.periode_til,
            vedtak.belop,
            vedtak.resultat,
            vedtak.BIDRAGSEVNE,
            vedtak.UNDERHOLDSKOSTNAD,
            vedtak.SAMVAERSFRADRAG,
            vedtak.NETTO_BARNETILLEGG_BP,
            vedtak.NETTO_BARNETILLEGG_BM,
            vedtak.SAMVAERSKLASSE,
            vedtak.BPS_ANDEL_UNDERHOLDSKOSTNAD,
            vedtak.BPBOR_MED_ANDRE_VOKSNE,
            vedtak.periode_fra_opphor,
            vedtak.aar,
            vedtak.valutakode,
            vedtak.STONADSTYPE, 
            vedtak.NETTO_TILSYNSUTGIFT,
            vedtak.FAKTISK_TILSYNSUTGIFT, 
            vedtak.INNKREVING_FLAGG,
            vedtak.fk_dim_tid_mnd,
            vedtak.siste_dato_i_perioden,
            inntekts_typer.P_TYPE_INNTEKT_1,
            inntekts_typer.P_inntekt_1,
            inntekts_typer.P_TYPE_INNTEKT_2,
            inntekts_typer.P_inntekt_2,
            inntekts_typer.P_TYPE_INNTEKT_3,
            inntekts_typer.P_inntekt_3,
            inntekts_typer.P_INNTEKT_TOTAL,
            inntekts_typer.P_ANTALL_TYPER,
            inntekts_typer.M_TYPE_INNTEKT_1,
            inntekts_typer.M_inntekt_1,
            inntekts_typer.M_TYPE_INNTEKT_2,
            inntekts_typer.M_inntekt_2,
            inntekts_typer.M_type_inntekt_3,
            inntekts_typer.M_inntekt_3,
            inntekts_typer.M_type_inntekt_4,
            inntekts_typer.M_inntekt_4,
            inntekts_typer.M_type_inntekt_5,
            inntekts_typer.M_inntekt_5,
            inntekts_typer.M_INNTEKT_TOTAL,
            inntekts_typer.M_ANTALL_TYPER
        FROM opphor_hvis_finnes vedtak
        LEFT JOIN inntekts_typer
            ON vedtak.pk_bb_bidrags_periode = inntekts_typer.FK_BB_BIDRAGS_PERIODE
        WHERE vedtak.siste_dato_i_perioden < NVL(vedtak.periode_fra_opphor, vedtak.siste_dato_i_perioden + 1)
    ) base
    LEFT JOIN dt_person.dim_person dim_kravhaver
        ON dim_kravhaver.fk_person1 = base.fk_person1_kravhaver
        AND base.fk_person1_kravhaver != -1
        AND base.siste_dato_i_perioden BETWEEN dim_kravhaver.gyldig_fra_dato AND dim_kravhaver.gyldig_til_dato
    LEFT JOIN dt_person.dim_person dim_mottaker
        ON dim_mottaker.fk_person1 = base.fk_person1_mottaker
        AND base.fk_person1_mottaker != -1
        AND base.siste_dato_i_perioden BETWEEN dim_mottaker.gyldig_fra_dato AND dim_mottaker.gyldig_til_dato
    LEFT JOIN dt_person.dim_person dim_skyldner
        ON dim_skyldner.fk_person1 = base.fk_person1_skyldner
        AND base.fk_person1_skyldner != -1
        AND base.siste_dato_i_perioden BETWEEN dim_skyldner.gyldig_fra_dato AND dim_skyldner.gyldig_til_dato
{% endfor %}