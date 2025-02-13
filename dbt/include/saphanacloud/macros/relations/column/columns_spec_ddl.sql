{% macro table_columns_and_constraints() %}
  {# loop through user_provided_columns to create DDL with data types and constraints #}
    {%- set raw_column_constraints = adapter.render_raw_columns_constraints(raw_columns=model['columns']) -%}
    {%- set raw_model_constraints = adapter.render_raw_model_constraints(raw_constraints=model['constraints'], raw_columns=model['columns']) -%}

    (
    {% for c in raw_column_constraints -%}
      {{ c }}{{ "," if not loop.last or raw_model_constraints }}
    {% endfor %}
    {% for c in raw_model_constraints -%}
        {{ c }}{{ "," if not loop.last }}
    {% endfor -%}
    )
{% endmacro %}