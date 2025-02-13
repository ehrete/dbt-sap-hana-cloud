{% macro saphanacloud__create_columns(relation, columns) %}

    {% set check_cols_config = config.get('check_cols', 'all') %}
    {% for column in columns %}
        {# If the column is a string, assume its the column name and use VARCHAR(255) #}
        {% if column is string %}
            {% set column_name = adapter.quote(column) %}
            {% set data_type = "VARCHAR(255)" %}  {# Default data type for string columns #}
        {% else %}
            {# Extract the column name and data type if column is an object #}
            {% set column_name = adapter.quote(column.name) %}
            {% set data_type = column.data_type %}
        {% endif %}

        {# Construct the SQL for adding the column with proper quoting #}
        {% set sql = "ALTER TABLE " ~ relation.render() ~ " ADD (" ~ column_name ~ " " ~ data_type ~ ")"%}

        {# Execute the SQL statement #}
        {% call statement() %}
            {{ sql }}
        {% endcall %}
    {% endfor %}
{% endmacro %}

{% macro saphanacloud__post_snapshot(staging_relation) %}
    {% do adapter.truncate_relation(staging_relation) %}
    
    -- Then, drop the staging table
    {% do adapter.drop_relation(staging_relation) %}
{% endmacro %}

{% macro saphanacloud__snapshot_staging_table(strategy, source_sql, target_relation) -%}
    {% set check_strategy = config.get('strategy', 'timestamp') %}
    {% set check_cols_config = config.get('check_cols', 'all') %}

    with snapshot_query as (

        {{ source_sql }}

    ),

    snapshotted_data as (

        select {{ target_relation }}.*,
            {{ strategy.unique_key }} as dbt_unique_key

        from {{ target_relation }}
        where dbt_valid_to is null

    ),

    insertions_source_data as (

        select
            snapshot_query.*,
            {{ strategy.unique_key }} as dbt_unique_key,
            {{ strategy.updated_at }} as dbt_updated_at,
            {{ strategy.updated_at }} as dbt_valid_from,
            nullif({{ strategy.updated_at }}, {{ strategy.updated_at }}) as dbt_valid_to,
            {{ strategy.scd_id }} as dbt_scd_id

        from snapshot_query
    ),

    updates_source_data as (

        select
            snapshot_query.*,
            {{ strategy.unique_key }} as dbt_unique_key,
            {{ strategy.updated_at }} as dbt_updated_at,
            {{ strategy.updated_at }} as dbt_valid_from,
            {{ strategy.updated_at }} as dbt_valid_to

        from snapshot_query
    ),

    {%- if strategy.invalidate_hard_deletes %}

    deletes_source_data as (

        select
            snapshot_query.*,
            {{ strategy.unique_key }} as dbt_unique_key
        from snapshot_query
    ),
    {% endif %}

    insertions as (

        select
            'insert' as dbt_change_type,
            source_data.*

        from insertions_source_data source_data
        left outer join snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where 
            (snapshotted_data.dbt_unique_key is null)
            or 
            (snapshotted_data.dbt_unique_key is not null and {{ strategy.row_changed }})

    ),

    updates as (

        select
            'update' as dbt_change_type,
            source_data.*,
            snapshotted_data.dbt_scd_id

        from updates_source_data source_data
        join snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where {{ strategy.row_changed }}
    )



    {%- if strategy.invalidate_hard_deletes -%}
    ,

    deletes as (

        select
            'delete' as dbt_change_type,
            source_data.*,
            {{ snapshot_get_time() }} as dbt_valid_from,
            {{ snapshot_get_time() }} as dbt_updated_at,
            {{ snapshot_get_time() }} as dbt_valid_to,
            snapshotted_data.dbt_scd_id

        from snapshotted_data
        left join deletes_source_data source_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where source_data.dbt_unique_key is null
    )
    {%- endif %}

    select * from insertions
    union all
    select * from updates
    {%- if strategy.invalidate_hard_deletes %}
    union all
    select * from deletes
    {%- endif %}

{%- endmacro %}

{% macro saphanacloud__build_snapshot_table(strategy, sql) %}
    select sbq.*,
        {{ strategy.scd_id }} as dbt_scd_id,
        {{ strategy.updated_at }} as dbt_updated_at,
        {{ strategy.updated_at }} as dbt_valid_from,
        cast(nullif({{ strategy.updated_at }}, {{ strategy.updated_at }}) as TIMESTAMP) as dbt_valid_to
    from (
        {{ sql }}
    ) sbq

{% endmacro %}
