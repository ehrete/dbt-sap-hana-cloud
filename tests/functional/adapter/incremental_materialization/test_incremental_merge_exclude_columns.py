import pytest
from dbt.tests.adapter.incremental.test_incremental_merge_exclude_columns import (
    BaseMergeExcludeColumns,
)

models__merge_exclude_columns_sql = """
{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    incremental_strategy='merge',
    merge_exclude_columns=['msg']
) }}

{% if not is_incremental() %}

-- data for first invocation of model

select 1 as id, 'hello' as msg, 'blue' as color from dummy
union all
select 2 as id, 'goodbye' as msg, 'red' as color from dummy

{% else %}

-- data for subsequent incremental update

select 1 as id, 'hey' as msg, 'blue' as color from dummy
union all
select 2 as id, 'yo' as msg, 'green' as color from dummy
union all
select 3 as id, 'anyway' as msg, 'purple' as color from dummy

{% endif %}
"""

class TestBaseMergeExcludeColumnsHana(BaseMergeExcludeColumns):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "merge_exclude_columns.sql": models__merge_exclude_columns_sql
        }