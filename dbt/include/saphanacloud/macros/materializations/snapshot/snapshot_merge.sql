{% macro saphanacloud__snapshot_merge_sql(target, source, insert_cols) %}
    {%- set insert_cols_csv = insert_cols | join(', ') -%}

    -- Construct select columns with DBT_INTERNAL_SOURCE prefix for the insert
    {% set select_columns = [] %}
    {% for column in insert_cols %}
        {% do select_columns.append('DBT_INTERNAL_SOURCE.' ~ column) %}
    {% endfor %}

    -- MERGE statement to handle both updates and inserts
    merge into {{ target }} as TARGET
    using {{ source }} as DBT_INTERNAL_SOURCE
    on (DBT_INTERNAL_SOURCE.dbt_scd_id = TARGET.dbt_scd_id)

    -- Update records that have changed (dbt_change_type is 'update' or 'delete')
    when matched
    and TARGET.dbt_valid_to is null
    and DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'delete')
    then update
    set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to

    -- Insert new records (dbt_change_type is 'insert')
    when not matched
    and DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
    then insert ({{ insert_cols_csv }})
    values ({{ select_columns | join(', ') }});

{% endmacro %}






{% macro saphanacloud__snapshot_hash_arguments(args) -%}
    TO_VARCHAR(HASH_MD5(
        TO_BINARY(
            {%- for arg in args -%}
                COALESCE(CAST({{ arg }} AS VARCHAR(50)), '')
                {%- if not loop.last %} || '|' || {%- endif %}
            {%- endfor -%}
        )
    ))
{%- endmacro %}


