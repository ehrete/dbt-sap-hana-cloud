import pytest

from dbt.tests.adapter.grants.test_incremental_grants import BaseIncrementalGrants, incremental_model_schema_yml
from dbt.tests.adapter.grants.test_invalid_grants import BaseInvalidGrants
from dbt.tests.adapter.grants.test_model_grants import BaseModelGrants, model_schema_yml
from dbt.tests.adapter.grants.test_seed_grants import BaseSeedGrants
from dbt.tests.adapter.grants.test_snapshot_grants import BaseSnapshotGrants, snapshot_schema_yml

my_model_sql = """
  select 1 as fun from dummy
"""
my_incremental_model_sql = """
  select 1 as fun from dummy
"""
my_invalid_model_sql = """
  select 1 as fun from dummy
"""
my_snapshot_sql = """
{% snapshot my_snapshot %}
    {{ config(
        check_cols='all', unique_key='id', strategy='check',
        target_database=database, target_schema=schema
    ) }}
    select 1 as id, 'blue' as color from dummy
{% endsnapshot %}
"""

class TestSeedGrantsHana(BaseSeedGrants):
    pass

class TestModelGrantsHana(BaseModelGrants):
    @pytest.fixture(scope="class")
    def models(self):
        updated_schema = self.interpolate_name_overrides(model_schema_yml)

        return {
            "my_model.sql": my_model_sql,
            "schema.yml": updated_schema,
        }
    
class TestIncrementalGrantsHana(BaseIncrementalGrants):

    @pytest.fixture(scope="class")
    def models(self):
        updated_schema = self.interpolate_name_overrides(incremental_model_schema_yml)
        return {
            "my_incremental_model.sql": my_incremental_model_sql,
            "schema.yml": updated_schema,
        }
    
class TestInvalidGrantsHana(BaseInvalidGrants):

    def grantee_does_not_exist_error(self):
        return "invalid user name: INVALID_USER"

    def privilege_does_not_exist_error(self):
        return "invalid user privilege: FAKE_PRIVILEGE"

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_invalid_model.sql": my_invalid_model_sql,
        }
    
class TestSnapshotGrantsHana(BaseSnapshotGrants):

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "my_snapshot.sql": my_snapshot_sql,
            "schema.yml": self.interpolate_name_overrides(snapshot_schema_yml),
        }