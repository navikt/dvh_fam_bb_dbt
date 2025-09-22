{% macro oppdater_komment_kolonne(tabell_navn, kolonne_navn, komment) %}
    {% set relation = ref(tabell_navn) %}
    {% set schema = relation.schema %}
    {% set table = relation.identifier %}
    {% set sql %}
        COMMENT ON COLUMN {{ schema }}.{{ table }}.{{ kolonne_navn }} IS '{{ komment }}'
    {% endset %}
    {{ log("Executing SQL: " ~ sql, info=True) }}
    {% do run_query(sql) %}
{% endmacro %}
