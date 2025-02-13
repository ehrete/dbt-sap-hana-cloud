{% macro saphanacloud__create_or_replace_view(target_relation, sql) %}
  {%- set identifier = target_relation.identifier -%}
  {%- set schema = target_relation.schema -%}
  {%- set database = target_relation.database -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}

  {%- set grant_config = config.get('grants') -%}

  {{ run_hooks(pre_hooks) }}

  {%- if old_relation is not none -%}
  {%- if old_relation.is_view -%}
    {# If the existing relation is a view, drop the view #}
    {%- call statement('drop_view', fetch_result=True) -%}
      {{ saphanacloud__drop_view_if_exists(old_relation) }}
    {%- endcall -%}
  {%- else -%}
    {# If the existing relation is not a view (i.e., a table), drop the table #}
    {%- call statement('drop_table', fetch_result=True) -%}
      {{ saphanacloud__drop_table(old_relation) }}
    {%- endcall -%}
  {%- endif -%}
{%- endif -%}

  -- Create or replace the view
  {% set create_sql %}
     CREATE OR REPLACE VIEW {{ target_relation }}
     {% set contract_config = config.get('contract') %}
    {% if contract_config.enforced %}
      {{ get_assert_columns_equivalent(sql) }}
    {%- endif %}
  AS (
    {{ sql }}
  );
 {% endset %}

  {{ return(create_sql) }}

{% endmacro %}

{% macro saphanacloud__handle_existing_table(full_refresh, old_relation) %}
    {%- if full_refresh -%}
      {{ log("Dropping relation " ~ old_relation ~ " because it is of type " ~ old_relation.type) }}
      {{ adapter.drop_relation(old_relation) }}
    {%- else -%}
      {{ exceptions.raise_compiler_error(
        "A table named `" ~ old_relation.identifier ~ "` already exists in `" ~ old_relation.schema ~ "`. To overwrite it, use `--full-refresh`."
      ) }}
    {%- endif -%}
{% endmacro %}

{% macro saphanacloud__create_view_as_with_temp_flag(relation, sql, is_temporary=False) -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  {{ sql_header if sql_header is not none }}
  
  create or replace view {{ relation }}
  {% if config.persist_column_docs() -%}
    {% set model_columns = model.columns %}
    {% set query_columns = get_columns_in_query(sql) %}
    {{ get_persist_docs_column_list(model_columns, query_columns) }}
  {%- endif %}
  {%- set contract_config = config.get('contract') -%}
  {%- if contract_config.enforced -%}
    {{ get_table_columns_names() }}
  {%- endif %}
  as (
    {{ sql }}
  );
{% endmacro %}



