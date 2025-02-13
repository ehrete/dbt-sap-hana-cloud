import pytest
from dbt.tests.adapter.incremental.test_incremental_predicates import BaseIncrementalPredicates

from dbt.tests.adapter.incremental.test_incremental_on_schema_change import (
    BaseIncrementalOnSchemaChange,
)


models__delete_insert_incremental_predicates_sql = """
{{ config(
    materialized = 'incremental',
    unique_key = 'id',
) }}

{% if not is_incremental() %}

select 1 as id, 'hello' as msg, 'blue' as color from dummy
union all
select 2 as id, 'goodbye' as msg, 'red' as color from dummy

{% else %}

-- delete will not happen on the above record where id = 2, so new record will be inserted instead
select 1 as id, 'hey' as msg, 'blue' as color from dummy
union all
select 2 as id, 'yo' as msg, 'green' as color from dummy
union all
select 3 as id, 'anyway' as msg, 'purple' as color from dummy

{% endif %}
"""

models__merge_incremental_predicates_sql = """
{{ config(
    materialized = 'incremental',
    unique_key = 'id',
) }}

{% if not is_incremental() %}

select 1 as id, 'hello' as msg, 'blue' as color from dummy
union all
select 2 as id, 'goodbye' as msg, 'red' as color from dummy

{% else %}

-- delete will not happen on the above record where id = 2, so new record will be inserted instead
select 1 as id, 'hey' as msg, 'blue' as color from dummy
union all
select 2 as id, 'yo' as msg, 'green' as color from dummy
union all
select 3 as id, 'anyway' as msg, 'purple' as color from dummy

{% endif %}
"""


class TestIncrementalPredicatesDeleteInsert(BaseIncrementalPredicates):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "delete_insert_incremental_predicates.sql": models__delete_insert_incremental_predicates_sql
        }

    
class TestIncrementalPredicatesMergeHana(BaseIncrementalPredicates):

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "delete_insert_incremental_predicates.sql": models__merge_incremental_predicates_sql
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+incremental_predicates": ["dbt_internal_dest.id != 2"],
                "+incremental_strategy": "merge"
            }
        }


class TestPredicatesMergeHana(BaseIncrementalPredicates):

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "delete_insert_incremental_predicates.sql": models__merge_incremental_predicates_sql
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+predicates": ["dbt_internal_dest.id != 2"],
                "+incremental_strategy": "merge"
            }
        }