models__incremental_sql = """
{{
  config(
    materialized = "incremental",
    indexes=[
      {'columns': ['column_a'], 'type': 'INVERTED VALUE'},
      {'columns': ['column_a', 'column_b'], 'unique': True},
    ]
  )
}}

select *
from (
  select 1 as column_a, 2 as column_b from dummy
) t

{% if is_incremental() %}
    where column_a > (select max(column_a) from {{this}})
{% endif %}

"""

models__table_sql = """
{{
  config(
    materialized = "table",
    indexes=[
      {'columns': ['column_b'], 'type': 'INVERTED VALUE'},
      {'columns': ['column_a', 'column_b'], 'type': 'INVERTED VALUE'},
      {'columns': ['column_b', 'column_a'], 'type': 'INVERTED VALUE', 'unique': True},
      {'columns': ['column_b', 'column_c'], 'type': 'INVERTED HASH', 'unique': True},
      {'columns': ['column_a', 'column_c'], 'type': 'INVERTED INDIVIDUAL', 'unique': True}
    ]
  )
}}

select 1 as column_a, 2 as column_b, 3 as column_c from dummy
"""
models__row_table_sql = """
{{
  config(
    materialized = "table",
    table_type='row',
    indexes=[
      {'columns': ['column_a'], 'type': 'BTREE'},
      {'columns': ['column_b'], 'type': 'BTREE', 'unique': True},
      {'columns': ['column_a', 'column_b'], 'type': 'CPBTREE'},
      {'columns': ['column_b', 'column_a'], 'type': 'CPBTREE', 'unique': True}
    ]
  )
}}

select 1 as column_a, 2 as column_b, 3 as column_c from dummy
"""

models_invalid__invalid_columns_type_sql = """
{{
  config(
    materialized = "table",
    indexes=[
      {'columns': 'column_a, column_b'}
    ]
  )
}}

select 1 as column_a, 2 as column_b from dummy
"""

models_invalid__invalid_type_sql = """
{{
  config(
    materialized = "table",
    indexes=[
      {'columns': ['column_a'], 'type': 'NON_EXISTENT_TYPE'}
    ]
  )
}}

select 1 as column_a, 2 as column_b from dummy
"""

models_invalid__invalid_unique_config_sql = """
{{
  config(
    materialized = "table",
    indexes=[
      {'columns': ['column_a'], 'unique': 'yes'}
    ]
  )
}}

select 1 as column_a, 2 as column_b from dummy
"""

models_invalid__missing_columns_sql = """
{{
  config(
    materialized = "table",
    indexes=[
      {'unique': True}
    ]
  )
}}

select 1 as column_a, 2 as column_b from dummy
"""

snapshots__colors_sql = """
{% snapshot colors %}

    {{
        config(
            target_database=database,
            target_schema=schema,
            unique_key='id',
            strategy='check',
            check_cols=['color'],
            indexes=[
              {'columns': ['id'], 'type': 'INVERTED VALUE'},
              {'columns': ['id', 'color'], 'unique': True, 'type': 'INVERTED HASH'},
            ]
        )
    }}

    {% if var('version') == 1 %}

        select 1 as id, 'red' as color from dummy union all
        select 2 as id, 'green' as color from dummy

    {% else %}

        select 1 as id, 'blue' as color from dummy union all
        select 2 as id, 'green' as color from dummy

    {% endif %}

{% endsnapshot %}
"""

seeds__seed_csv = """country_code,country_name
US,United States
CA,Canada
GB,United Kingdom
"""
