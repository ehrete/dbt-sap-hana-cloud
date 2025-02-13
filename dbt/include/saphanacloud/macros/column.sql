{% macro saphanacloud__get_empty_subquery_sql(select_sql, select_sql_header=none) %}
      {%- if select_sql_header is not none -%}
    {{ select_sql_header }}
    {%- endif -%}
    {# Directly select TOP 0 from the result #}
    select top 0 * from (
        {{ select_sql }}
    ) as __dbt_sbq
{% endmacro %}

{% macro saphanacloud__alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}

  {% if add_columns is none %}
    {% set add_columns = [] %}
  {% endif %}
  {% if remove_columns is none %}
    {% set remove_columns = [] %}
  {% endif %}

  {# First, process columns to be added #}
  {% if add_columns %}
    {% for column in add_columns %}
      {% set add_sql -%}
      alter table {{ relation.render() }}
        add ({{ column.name }} {{ column.data_type }})
      {%- endset %}
      {% do run_query(add_sql) %}
    {% endfor %}
  {% endif %}

  {# Then, process columns to be removed #}
  {% if remove_columns %}
    {% for column in remove_columns %}
      {% set remove_sql -%}
      alter table {{ relation.render() }}
        drop ({{ column.name }})
      {%- endset %}
      {% do run_query(remove_sql) %}
    {% endfor %}
  {% endif %}

{% endmacro %}


{%- macro get_table_columns_names() -%}
  {{ adapter.dispatch('get_table_columns_names', 'dbt')() }}
{%- endmacro -%}

{% macro saphanacloud__get_table_columns_names() -%}
  {{ return(saphanacloud__table_columns_names()) }}
{%- endmacro %}

{% macro saphanacloud__table_columns_names() %}
  {# loop through user_provided_columns to create DDL with data types and constraints #}
    {%- set raw_column_names = adapter.render_raw_columns_names(raw_columns=model['columns']) -%}
    {%- set raw_model_constraints = adapter.render_raw_model_constraints(raw_constraints=model['constraints'],raw_columns=model['columns']) -%}
    (
    {% for c in raw_column_names -%}
      {{ c }}{{ "," if not loop.last or raw_model_constraints }}
    {% endfor %}
    {% for c in raw_model_constraints -%}
        {{ c }}{{ "," if not loop.last }}
    {% endfor -%}
    )
{% endmacro %}