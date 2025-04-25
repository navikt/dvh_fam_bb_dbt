{% set start_date = 202001 %}
{% set end_date = 202503 %}
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
        vedtakstidspunkt,
        pk_bb_fagsak AS fk_bb_fagsak, 
        saksnr,
        vedtaks_id, 
        behandlings_type, 
        pk_bb_forskudds_periode AS fk_bb_forskudds_periode,
        periode_fra, 
        periode_til, 
        belop, 
        resultat,
        barnets_alders_gruppe, 
        periode_fra_opphor, 
        aar,
        TO_DATE('{{ period['max_vedtaksdato'] }}', 'YYYYMMDD') MAX_VEDTAKSDATO,
        fk_dim_tid_mnd,
        '{{ period['periode_type'] }}' periode_type,
        dim_kravhaver.pk_dim_person AS fk_dim_person_kravhaver,
        FLOOR(MONTHS_BETWEEN(siste_dato_i_perioden, dim_kravhaver.fodt_dato)/12) alder_kravhaver,
        dim_kravhaver.kjonn_nr kjonn_kravhaver,
        dim_mottaker.pk_dim_person AS fk_dim_person_mottaker,
        dim_mottaker.bosted_kommune_nr AS bosted_kommune_nr_mottaker,
        dim_mottaker.fk_dim_land_statsborgerskap AS fk_dim_land_statsborgerskap_mottaker,
        dim_mottaker.fk_dim_geografi_bosted AS fk_dim_geografi_bosted_mottaker,
        FLOOR(MONTHS_BETWEEN(siste_dato_i_perioden, dim_mottaker.fodt_dato)/12) alder_mottaker,
        inntekt_total, 
        ANTALL_INTTEKTS_TYPER,
        type_inntekt_1,
        inntekt_1,
        type_inntekt_2,
        inntekt_2,
        type_inntekt_3,
        inntekt_3,
        type_inntekt_4,
        inntekt_4,
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
                fagsak.fk_person1_kravhaver, 
                fagsak.vedtaks_id, 
                fagsak.saksnr, 
                fagsak.behandlings_type,
                fagsak.vedtakstidspunkt, 
                fagsak.fk_person1_mottaker,
                periode.pk_bb_forskudds_periode, 
                periode.periode_fra, 
                periode.periode_til, 
                periode.belop,
                periode.resultat, 
                periode.barnets_alders_gruppe,
                tid.aar_maaned, 
                tid.siste_dato_i_perioden, 
                tid.aar, 
                tid.pk_dim_tid AS fk_dim_tid_mnd,
                row_number() over (partition by tid.aar_maaned, fagsak.fk_person1_kravhaver ,fagsak.saksnr 
	                order by fagsak.vedtakstidspunkt desc
                    ) nr
            FROM {{ source('fam_bb', 'fam_bb_fagsak') }} fagsak
            JOIN {{ source('fam_bb', 'fam_bb_forskudds_periode') }} periode
                ON fagsak.pk_bb_fagsak = periode.fk_bb_fagsak
                AND periode.belop > 0
            JOIN tid
                ON periode.periode_fra <= TO_DATE(tid.aar_maaned || '01', 'YYYYMMDD')
                AND NVL(periode.periode_til, tid.siste_dato_i_perioden) >= tid.siste_dato_i_perioden
            WHERE fagsak.behandlings_type NOT IN ('ENDRING_MOTTAKER', 'OPPHØR', 'ALDERSOPPHØR')
                AND TRUNC(fagsak.vedtakstidspunkt, 'DD') <= TO_DATE('{{ period['max_vedtaksdato'] }}', 'YYYYMMDD')
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
                MIN(periode.periode_fra) periode_fra_opphor
            FROM {{ source('fam_bb', 'fam_bb_fagsak') }} fagsak 
            JOIN {{ source('fam_bb', 'fam_bb_forskudds_periode') }} periode
                ON fagsak.pk_bb_fagsak = periode.fk_bb_fagsak
                AND periode.belop IS NULL -- Opphørt versjon
            WHERE fagsak.behandlings_type NOT IN ('ENDRING_MOTTAKER')
                AND TRUNC(fagsak.vedtakstidspunkt, 'DD') <= TO_DATE('{{ period['max_vedtaksdato'] }}', 'YYYYMMDD')
            GROUP BY fagsak.fk_person1_kravhaver, fagsak.saksnr, fagsak.vedtakstidspunkt
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
        ),

        opphor_hvis_finnes AS (
            SELECT 
                aar_maaned, 
                siste_dato_i_perioden, 
                aar, 
                fk_dim_tid_mnd,
                fk_person1_kravhaver, 
                fk_person1_mottaker,
                vedtakstidspunkt, 
                pk_bb_fagsak, 
                saksnr, 
                vedtaks_id, 
                behandlings_type,
                pk_bb_forskudds_periode, 
                periode_fra, 
                periode_til, 
                belop,
                resultat, 
                barnets_alders_gruppe,
                MIN(periode_fra_opphor) periode_fra_opphor
            FROM siste_opphør
            GROUP BY 
                aar_maaned, 
                siste_dato_i_perioden, 
                aar, 
                fk_dim_tid_mnd,
                fk_person1_kravhaver, 
                fk_person1_mottaker,
                vedtakstidspunkt, 
                pk_bb_fagsak, 
                saksnr, 
                vedtaks_id, 
                behandlings_type,
                pk_bb_forskudds_periode, 
                periode_fra, 
                periode_til, 
                belop,
                resultat, 
                barnets_alders_gruppe
        ),

        inntekt as (
            SELECT  FK_BB_FORSKUDDS_PERIODE,TYPE_INNTEKT,BELOP
                ,ROW_NUMBER() OVER (PARTITION BY FK_BB_FORSKUDDS_PERIODE ORDER BY FK_BB_FORSKUDDS_PERIODE, TYPE_INNTEKT) NR
            FROM FAM_BB_INNTEKT
        
        ),
        
        inntekts_typer as (
        SELECT  FK_BB_FORSKUDDS_PERIODE FK_BB_FORSKUDDS_PERIODE, 
        MAX(CASE WHEN NR=1 THEN TYPE_INNTEKT END) TYPE_INNTEKT_1,
        MAX(CASE WHEN NR=1 THEN BELOP END) INNTEKT_1,
        MAX(CASE WHEN NR=2 THEN TYPE_INNTEKT END) TYPE_INNTEKT_2,
        MAX(CASE WHEN NR=2 THEN BELOP END) INNTEKT_2,
        MAX(CASE WHEN NR=3 THEN TYPE_INNTEKT END) TYPE_INNTEKT_3,
        MAX(CASE WHEN NR=3 THEN BELOP END) INNTEKT_3,
        MAX(CASE WHEN NR=4 THEN TYPE_INNTEKT END) TYPE_INNTEKT_4,
        MAX(CASE WHEN NR=4 THEN BELOP END) INNTEKT_4,
        SUM(BELOP) INNTEKT_TOTAL,
        MAX(NR) ANTALL_INTTEKTS_TYPER
        FROM INNTEKT
        GROUP BY  FK_BB_FORSKUDDS_PERIODE
        )

        SELECT 
            vedtak.aar_maaned, 
            vedtak.fk_person1_kravhaver, 
            vedtak.fk_person1_mottaker, 
            vedtak.vedtakstidspunkt,
            vedtak.pk_bb_fagsak, 
            vedtak.saksnr,
            vedtak.vedtaks_id, 
            vedtak.behandlings_type, 
            vedtak.pk_bb_forskudds_periode,
            vedtak.periode_fra, 
            vedtak.periode_til, 
            vedtak.belop, 
            vedtak.resultat,
            vedtak.barnets_alders_gruppe, 
            vedtak.periode_fra_opphor, 
            vedtak.aar,
            vedtak.fk_dim_tid_mnd,
            vedtak.siste_dato_i_perioden,
            inntekts_typer.inntekt_total,
            inntekts_typer.ANTALL_INTTEKTS_TYPER,
            inntekts_typer.type_inntekt_1,
            inntekts_typer.inntekt_1,
            inntekts_typer.type_inntekt_2,
            inntekts_typer.inntekt_2,
            inntekts_typer.type_inntekt_3,
            inntekts_typer.inntekt_3,
            inntekts_typer.type_inntekt_4,
            inntekts_typer.inntekt_4
        FROM opphor_hvis_finnes vedtak
        LEFT JOIN inntekts_typer
            ON vedtak.pk_bb_forskudds_periode = inntekts_typer.fk_bb_forskudds_periode
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
{% endfor %}