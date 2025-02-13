import pytest

from dbt.tests.adapter.basic.files import (
    config_materialized_ephemeral,
    test_ephemeral_passing_sql,
    test_ephemeral_failing_sql,
    schema_base_yml,
)

from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import (
    BaseSingularTestsEphemeral
)
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_incremental import BaseIncremental, BaseIncrementalNotSchemaChange
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod, models__expected_sql, models__upstream_sql


test_passing_sql = """
SELECT * FROM (
    SELECT 1 AS id FROM DUMMY
)
WHERE id = 2
"""

test_failing_sql = """
SELECT * FROM (
    SELECT 1 AS id FROM DUMMY
)
WHERE id = 1
"""
model_ephemeral = """
SELECT id, name 
FROM {{ ref('base') }} 
WHERE id IS NOT null
"""

models__upstream_sql = """
SELECT 1 as id FROM DUMMY
"""

models__expected_sql = """
-- make sure this runs after 'model'
-- {{ ref('model') }}
SELECT 2 as id FROM DUMMY
"""

models__model_sql = """

{% set upstream = ref('upstream') %}

{% if execute %}
    {# don't ever do any of this #}
    {%- do adapter.drop_schema(upstream) -%}
    {% set existing = adapter.get_relation(upstream.database, upstream.schema, upstream.identifier) %}
    {% if existing is not none %}
        {% do exceptions.raise_compiler_error('expected ' ~ ' to not exist, but it did') %}
    {% endif %}

    {%- do adapter.create_schema(upstream) -%}

    {% set sql = create_view_as(upstream, 'select 2 as id from DUMMY') %}
    {% do run_query(sql) %}
{% endif %}


select * from {{ upstream }}

"""

incremental_not_schema_change_sql = """
{{ config(materialized="incremental", unique_key="user_id_current_time",on_schema_change="sync_all_columns") }}
select
    1 || '-' || current_timestamp as user_id_current_time,
    {% if is_incremental() %}
        'thisis18characters' as platform
    {% else %}
        'okthisis20characters' as platform
    {% endif %}
    FROM DUMMY
"""

class TestSimpleMaterializationsHana(BaseSimpleMaterializations):
    pass

class TestSingularTestsMyAdapterHana(BaseSingularTests):
    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "passing.sql": test_passing_sql,
            "failing.sql": test_failing_sql,
        }
    
class TestSingularTestsEphemeralHana(BaseSingularTestsEphemeral):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "ephemeral.sql": config_materialized_ephemeral + model_ephemeral,
            "passing_model.sql": test_ephemeral_passing_sql,
            "failing_model.sql": test_ephemeral_failing_sql,
            "schema.yml": schema_base_yml,
        }

class TestEmptyHana(BaseEmpty):
    pass

class TestEphemeralHana(BaseEphemeral):
    pass

class TestIncrementalHana(BaseIncremental):
    pass

class TestGenericTestsHana(BaseGenericTests):
    pass

class TestSnapshotCheckColsHana(BaseSnapshotCheckCols):
    pass

class TestSnapshotTimestampHana(BaseSnapshotTimestamp):
    pass


class TestBaseAdapterMethodHana(BaseAdapterMethod):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "upstream.sql": models__upstream_sql,
            "expected.sql": models__expected_sql,
            "model.sql": models__model_sql,
        }
    
class TestIncrementalNotSchemaChangeHana(BaseIncrementalNotSchemaChange):

    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_not_schema_change.sql": incremental_not_schema_change_sql}