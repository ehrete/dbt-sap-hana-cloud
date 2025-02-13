import pytest

from dbt.tests.util import check_relations_equal, run_dbt
from dbt.tests.adapter.incremental.fixtures import (_MODELS__INCREMENTAL_SYNC_REMOVE_ONLY,
                                                    _MODELS__INCREMENTAL_IGNORE,
                                                    _MODELS__INCREMENTAL_SYNC_REMOVE_ONLY_TARGET,
                                                    _MODELS__INCREMENTAL_IGNORE_TARGET,
                                                    _MODELS__INCREMENTAL_FAIL,
                                                    _MODELS__INCREMENTAL_SYNC_ALL_COLUMNS,
                                                    _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_REMOVE_ONE,
                                                    _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_TARGET,
                                                    _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS,
                                                    _MODELS__INCREMENTAL_SYNC_ALL_COLUMNS_TARGET,
                                                    _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_REMOVE_ONE_TARGET)



_MODELS__A = """
{{
    config(materialized='table')
}}

with source_data as (

    select 1 as id, 'aaa' as field1, 'bbb' as field2, 111 as field3, 'TTT' as field4 from dummy
    union all select 2 as id, 'ccc' as field1, 'ddd' as field2, 222 as field3, 'UUU' as field4 from dummy
    union all select 3 as id, 'eee' as field1, 'fff' as field2, 333 as field3, 'VVV' as field4 from dummy
    union all select 4 as id, 'ggg' as field1, 'hhh' as field2, 444 as field3, 'WWW' as field4 from dummy
    union all select 5 as id, 'iii' as field1, 'jjj' as field2, 555 as field3, 'XXX' as field4 from dummy
    union all select 6 as id, 'kkk' as field1, 'lll' as field2, 666 as field3, 'YYY' as field4 from dummy

)

select id
       ,field1
       ,field2
       ,field3
       ,field4

from source_data
"""

class BaseIncrementalOnSchemaChangeSetup:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_sync_remove_only.sql": _MODELS__INCREMENTAL_SYNC_REMOVE_ONLY,
            "incremental_ignore.sql": _MODELS__INCREMENTAL_IGNORE,
            "incremental_sync_remove_only_target.sql": _MODELS__INCREMENTAL_SYNC_REMOVE_ONLY_TARGET,
            "incremental_ignore_target.sql": _MODELS__INCREMENTAL_IGNORE_TARGET,
            "incremental_fail.sql": _MODELS__INCREMENTAL_FAIL,
            "incremental_sync_all_columns.sql": _MODELS__INCREMENTAL_SYNC_ALL_COLUMNS,
            "incremental_append_new_columns_remove_one.sql": _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_REMOVE_ONE,
            "model_a.sql": _MODELS__A,
            "incremental_append_new_columns_target.sql": _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_TARGET,
            "incremental_append_new_columns.sql": _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS,
            "incremental_sync_all_columns_target.sql": _MODELS__INCREMENTAL_SYNC_ALL_COLUMNS_TARGET,
            "incremental_append_new_columns_remove_one_target.sql": _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_REMOVE_ONE_TARGET,
        }

    def run_twice_and_assert(self, include, compare_source, compare_target, project):
        # dbt run (twice)
        run_args = ["run"]
        if include:
            run_args.extend(("--select", include))
        results_one = run_dbt(run_args)
        assert len(results_one) == 3

        results_two = run_dbt(run_args)
        assert len(results_two) == 3

        check_relations_equal(project.adapter, [compare_source, compare_target])

    def run_incremental_append_new_columns(self, project):
        select = "model_a incremental_append_new_columns incremental_append_new_columns_target"
        compare_source = "incremental_append_new_columns"
        compare_target = "incremental_append_new_columns_target"
        self.run_twice_and_assert(select, compare_source, compare_target, project)

    def run_incremental_append_new_columns_remove_one(self, project):
        select = "model_a incremental_append_new_columns_remove_one incremental_append_new_columns_remove_one_target"
        compare_source = "incremental_append_new_columns_remove_one"
        compare_target = "incremental_append_new_columns_remove_one_target"
        self.run_twice_and_assert(select, compare_source, compare_target, project)

    def run_incremental_sync_all_columns(self, project):
        select = "model_a incremental_sync_all_columns incremental_sync_all_columns_target"
        compare_source = "incremental_sync_all_columns"
        compare_target = "incremental_sync_all_columns_target"
        self.run_twice_and_assert(select, compare_source, compare_target, project)

    def run_incremental_sync_remove_only(self, project):
        select = "model_a incremental_sync_remove_only incremental_sync_remove_only_target"
        compare_source = "incremental_sync_remove_only"
        compare_target = "incremental_sync_remove_only_target"
        self.run_twice_and_assert(select, compare_source, compare_target, project)

class BaseIncrementalOnSchemaChange(BaseIncrementalOnSchemaChangeSetup):
    def test_run_incremental_ignore(self, project):
        select = "model_a incremental_ignore incremental_ignore_target"
        compare_source = "incremental_ignore"
        compare_target = "incremental_ignore_target"
        self.run_twice_and_assert(select, compare_source, compare_target, project)

    def test_run_incremental_append_new_columns(self, project):
        self.run_incremental_append_new_columns(project)
        self.run_incremental_append_new_columns_remove_one(project)

    def test_run_incremental_sync_all_columns(self, project):
        self.run_incremental_sync_all_columns(project)
        self.run_incremental_sync_remove_only(project)

    def test_run_incremental_fail_on_schema_change(self, project):
        select = "model_a incremental_fail"
        run_dbt(["run", "--models", select, "--full-refresh"])
        results_two = run_dbt(["run", "--models", select], expect_pass=False)
        assert "Compilation Error" in results_two[1].message

class TestIncrementalOnSchemaChangeHana(BaseIncrementalOnSchemaChange):
    pass