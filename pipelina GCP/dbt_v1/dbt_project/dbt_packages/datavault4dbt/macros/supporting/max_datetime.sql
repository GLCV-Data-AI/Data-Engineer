{%- macro max_datetime() -%}

    {{- return(adapter.dispatch('max_datetime', 'datavault4dbt')()) -}}

{%- endmacro %}

{%- macro default__max_datetime() %}

    {% do return('9999-12-31 23:59:59.999999') %}

{% endmacro -%}

{%- macro synapse__max_datetime() %}

    {% do return('9999-12-31 23:59:59.9999999') %}

{% endmacro -%}

{%- macro bigquery__max_datetime() %}

    {% do return('9999-12-31 23:59:59.999999') %}

{% endmacro -%}


{%- macro fabric__max_datetime() %}

    {% do return('9999-12-31 23:59:59.9999999') %}

{% endmacro -%}