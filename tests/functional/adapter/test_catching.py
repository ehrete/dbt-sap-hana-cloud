import pytest

from dbt.tests.adapter.caching.test_caching import BaseCachingTest

from dbt.tests.util import run_dbt

model_sql = """
{{
    config(
        materialized='table'
    )
}}
select 1 as id from dummy
"""

another_schema_model_sql = """
{{
    config(
        materialized='table',
        schema='another_schema'
    )
}}
select 1 as id from dummy
"""
class HanaBaseCaching(BaseCachingTest):

    def run_and_inspect_cache(self, project, run_args=None):
        run_dbt(run_args)
        # the cache was empty at the start of the run.
        # the model materialization returned an unquoted relation and added to the cache.
        adapter = project.adapter
        assert len(adapter.cache.relations) == 1
        relation = list(adapter.cache.relations).pop()
        # assert relation.schema == project.test_schema
        assert relation.schema == project.test_schema.lower()

        # on the second run, dbt will find a relation in the database during cache population.
        # this relation will be quoted, because list_relations_without_caching (by default) uses
        # quote_policy = {"database": True, "schema": True, "identifier": True}
        # when adding relations to the cache.
        run_dbt(run_args)
        adapter = project.adapter
        assert len(adapter.cache.relations) == 1
        second_relation = list(adapter.cache.relations).pop()

        # perform a case-insensitive + quote-insensitive comparison
        for key in ["database", "schema", "identifier"]:
            assert getattr(relation, key).lower() == getattr(second_relation, key).lower()


class TestCachingLowerCaseModel(HanaBaseCaching):

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": model_sql,
        }

class TestCachingUppercaseModel(HanaBaseCaching):

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "MODEL.sql": model_sql,
        }

class TestCachingSelectedSchemaOnly(HanaBaseCaching):

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": model_sql,
            "another_schema_model.sql": another_schema_model_sql,
        }

    def test_cache(self, project):
        # this should only cache the schema containing the selected model
        run_args = ["--cache-selected-only", "run", "--select", "model"]
        self.run_and_inspect_cache(project, run_args)