/*
 * Copyright (c) Business Thinking Ltd. 2019-2023
 * This software includes code developed by the dbtvault Team at Business Thinking Ltd. Trading as Datavault
 */

{% materialization vault_insert_by_period, default -%}

    {% if target.type == "postgres" and execute %}
        {{ exceptions.raise_compiler_error("The vault_insert_by_period materialisation is currently unavailable on Postgres.") }}
    {% endif %}

    {%- set full_refresh_mode = (should_full_refresh()) -%}

    {% if target.type == "sqlserver" %}
        {%- set target_relation = this.incorporate(type='table') -%}
    {%  else %}
        {%- set target_relation = this -%}
    {% endif %}
    {%- set existing_relation = load_relation(this) -%}
    {%- set tmp_relation = make_temp_relation(target_relation) -%}

    {%- set timestamp_field = config.require('timestamp_field') -%}
    {%- set date_source_models = config.get('date_source_models', default=none) -%}

    {%- set start_stop_dates = dbtvault.get_start_stop_dates(timestamp_field, date_source_models) | as_native -%}

    {%- set period = config.get('period', default='day') -%}
    {%- set to_drop = [] -%}

    {%- do dbtvault.check_placeholder(sql) -%}

    {%- do dbtvault.check_num_periods(start_stop_dates.start_date, start_stop_dates.stop_date, period) -%}

    {{ run_hooks(pre_hooks, inside_transaction=False) }}

    -- `BEGIN` happens here:
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {% if existing_relation is none %}

        {% set filtered_sql = dbtvault.replace_placeholder_with_period_filter(sql, timestamp_field,
                                                                       start_stop_dates.start_date,
                                                                       start_stop_dates.stop_date,
                                                                       0, period) %}
        {% set build_sql = create_table_as(False, target_relation, filtered_sql) %}
        {% do to_drop.append(tmp_relation) %}

    {% elif existing_relation.is_view %}

        {{ log("Dropping relation " ~ target_relation ~ " because it is a view and this model is a table (vault_insert_by_period).") }}
        {% do adapter.drop_relation(existing_relation) %}
        {% set build_sql = create_table_as(False, target_relation, filtered_sql) %}

        {% set filtered_sql = dbtvault.replace_placeholder_with_period_filter(sql, timestamp_field,
                                                                       start_stop_dates.start_date,
                                                                       start_stop_dates.stop_date,
                                                                       0, period) %}
        {% set build_sql = create_table_as(False, target_relation, filtered_sql) %}

    {% elif full_refresh_mode %}
        {% set filtered_sql = dbtvault.replace_placeholder_with_period_filter(sql, timestamp_field,
                                                                       start_stop_dates.start_date,
                                                                       start_stop_dates.stop_date,
                                                                       0, period) %}
        {% set build_sql = create_table_as(False, target_relation, filtered_sql) %}
    {% else %}
        {% set period_boundaries = dbtvault.get_period_boundaries(target_relation,
                                                                  timestamp_field,
                                                                  start_stop_dates.start_date,
                                                                  start_stop_dates.stop_date,
                                                                  period) %}

        {% set target_columns = adapter.get_columns_in_relation(target_relation) %}
        {%- set target_cols_csv = target_columns | map(attribute='quoted') | join(', ') -%}
        {%- set loop_vars = {'sum_rows_inserted': 0} -%}

        {% for i in range(period_boundaries.num_periods) -%}

            {%- set iteration_number = i + 1 -%}

            {%- set period_of_load = dbtvault.get_period_of_load(period, i, period_boundaries.start_timestamp) -%}

            {{ dbt_utils.log_info("Running for {} {} of {} ({}) [{}]".format(period, iteration_number, period_boundaries.num_periods, period_of_load, model.unique_id)) }}

            {% set tmp_relation = make_temp_relation(target_relation) %}

            {% set tmp_table_sql = dbtvault.get_period_filter_sql(target_cols_csv, sql, timestamp_field, period,
                                                                  period_boundaries.start_timestamp,
                                                                  period_boundaries.stop_timestamp, i) %}



            {# This call statement drops and then creates a temporary table #}
            {# but MSSQL will fail to drop any temporary table created by a previous loop iteration #}
            {# See MSSQL note and drop code below #}

            {# [ ] TODO check dbt postgres implementation for a possible fix #}
            {% call statement() -%}
                {{ create_table_as(True, tmp_relation, tmp_table_sql) }}
            {%- endcall %}

            {{ adapter.expand_target_column_types(from_relation=tmp_relation,
                                                  to_relation=target_relation) }}

            {%- set insert_query_name = 'main-' ~ i -%}
            {% call statement(insert_query_name, fetch_result=True) -%}
                INSERT INTO {{ target_relation }} ({{ target_cols_csv }})
                (
                    SELECT {{ target_cols_csv }}
                    FROM {{ tmp_relation.include(schema=True) }}
                );
            {%- endcall %}

            {% set result = load_result(insert_query_name) %}

            {% if 'response' in result.keys() %} {# added in v0.19.0 #}
                {%- if not result['response']['rows_affected'] %}
                    {% if target.type == "databricks" and result['data'] | length > 0 %}
                        {% set rows_inserted = result['data'][0][1] | int %}
                    {% else %}
                        {% set rows_inserted = 0 %}
                    {% endif %}
                {%- else %}
                    {% set rows_inserted = result['response']['rows_affected'] %}
                {%- endif %}
            {% else %} {# older versions #}
                {% set rows_inserted = result['status'].split(" ")[2] | int %}
            {% endif %}

            {%- set sum_rows_inserted = loop_vars['sum_rows_inserted'] + rows_inserted -%}
            {%- do loop_vars.update({'sum_rows_inserted': sum_rows_inserted}) %}

            {{ dbt_utils.log_info("Ran for {} {} of {} ({}); {} records inserted [{}]".format(period, iteration_number,
                                                                                              period_boundaries.num_periods,
                                                                                              period_of_load, rows_inserted,
                                                                                              model.unique_id)) }}

            {# In databricks and sqlserver a temporary view/table can only be dropped by #}
            {# the connection or session that created it so drop it now before the commit below closes this session #}                                                                            model.unique_id)) }}
            {% if target.type in ['databricks', 'sqlserver'] %}
                {{ dbtvault.drop_temporary_special(tmp_relation) }}
            {% else %}
                {% do to_drop.append(tmp_relation) %}
            {% endif %}

            {% do adapter.commit() %}

        {% endfor %}

        {% call noop_statement('main', "INSERT {}".format(loop_vars['sum_rows_inserted']) ) -%}
            {{ tmp_table_sql }}
        {%- endcall %}

    {% endif %}

    {% if build_sql is defined %}
        {% call statement("main", fetch_result=True) %}
            {{ build_sql }}
        {% endcall %}

        {% set result = load_result('main') %}

        {% if 'response' in result.keys() %} {# added in v0.19.0 #}
            {%- if not result['response']['rows_affected'] %}
                {% if target.type == "databricks" and result['data'] | length > 0 %}
                    {% set rows_inserted = result['data'][0][1] | int %}
                {% else %}
                    {% set rows_inserted = 0 %}
                {% endif %}
            {%- else %}
                {% set rows_inserted = result['response']['rows_affected'] %}
            {%- endif %}
        {% else %} {# older versions #}
            {% set rows_inserted = result['status'].split(" ")[2] | int %}
        {% endif %}

        {% call noop_statement('main', "BASE LOAD {}".format(rows_inserted)) -%}
            {{ build_sql }}
        {%- endcall %}

        -- `COMMIT` happens here
        {% do adapter.commit() %}
    {% endif %}

    {{ run_hooks(post_hooks, inside_transaction=True) }}

    {% for rel in to_drop %}
        {% if rel.type is not none %}
            {% do adapter.drop_relation(rel) %}
        {% endif %}
    {% endfor %}

    {% set target_relation = target_relation.incorporate(type='table') %}

    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}