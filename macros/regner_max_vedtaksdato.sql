{% macro regner_max_vedtaksdato(year, month) %}
    {% set last_day = modules.datetime.date(year, month, 1).replace(
        day=1,
        month=month + 1 if month < 12 else 1,
        year=year + 1 if month == 12 else year
    ) - modules.datetime.timedelta(days=1) %}
    {% set last_day_str = last_day.strftime('%Y%m%d') %}
    {{ return(last_day_str) }}
{% endmacro %}
