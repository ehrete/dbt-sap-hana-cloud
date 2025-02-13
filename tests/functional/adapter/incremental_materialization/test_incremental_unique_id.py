import pytest
from dbt.tests.adapter.incremental.test_incremental_unique_id import (BaseIncrementalUniqueKey,
                                                                      seeds__seed_csv,
                                                                      seeds__add_new_rows_sql,
                                                                      seeds__duplicate_insert_sql,
                                                                      models__empty_str_unique_key_sql,
                                                                      models__expected__one_str__overwrite_sql,
                                                                      models__empty_unique_key_list_sql,
                                                                      models__no_unique_key_sql,
                                                                      models__not_found_unique_key_sql,
                                                                      models__str_unique_key_sql,
                                                                      models__trinary_unique_key_list_sql,
                                                                      models__unary_unique_key_list_sql,
                                                                      models__nontyped_trinary_unique_key_list_sql,
                                                                      models__duplicated_unary_unique_key_list_sql,
                                                                      models__not_found_unique_key_list_sql,
                                                                      models__expected__unique_key_list__inplace_overwrite_sql)

models__expected__unique_key_list__inplace_overwrite_sql = """
SELECT
    'CT' AS state,
    'Hartford' AS county,
    'Hartford' AS city,
    TO_DATE('2022-02-14', 'YYYY-MM-DD') as last_visit_date FROM DUMMY
union all
SELECT 'MA','Suffolk','Boston',TO_DATE('2020-02-12',  'YYYY-MM-DD') FROM DUMMY
union all
SELECT 'NJ','Mercer','Trenton',TO_DATE('2022-01-01',  'YYYY-MM-DD') FROM DUMMY
union all
SELECT 'NY','Kings','Brooklyn', TO_DATE('2021-04-02',  'YYYY-MM-DD') FROM DUMMY
union all
SELECT 'NY','New York','Manhattan', TO_DATE('2021-04-01',  'YYYY-MM-DD') FROM DUMMY
union all
SELECT 'PA','Philadelphia','Philadelphia', TO_DATE('2021-05-21', 'YYYY-MM-DD') FROM DUMMY
"""

models__expected__one_str__overwrite_sql  = """
SELECT
    'CT' AS state,
    'Hartford' AS county,
    'Hartford' AS city,
    TO_DATE('2022-02-14', 'YYYY-MM-DD') as last_visit_date FROM DUMMY
union all
SELECT 'MA','Suffolk','Boston',TO_DATE('2020-02-12',  'YYYY-MM-DD') FROM DUMMY
union all
SELECT 'NJ','Mercer','Trenton',TO_DATE('2022-01-01',  'YYYY-MM-DD') FROM DUMMY
union all
SELECT 'NY','Kings','Brooklyn', TO_DATE('2021-04-02',  'YYYY-MM-DD') FROM DUMMY
union all
SELECT 'NY','New York','Manhattan', TO_DATE('2021-04-01',  'YYYY-MM-DD') FROM DUMMY
union all
SELECT 'PA','Philadelphia','Philadelphia', TO_DATE('2021-05-21', 'YYYY-MM-DD') FROM DUMMY
"""

class TestIncrementalUniqueKey(BaseIncrementalUniqueKey):

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "trinary_unique_key_list.sql": models__trinary_unique_key_list_sql,
            "nontyped_trinary_unique_key_list.sql": models__nontyped_trinary_unique_key_list_sql,
            "unary_unique_key_list.sql": models__unary_unique_key_list_sql,
            "not_found_unique_key.sql": models__not_found_unique_key_sql,
            "empty_unique_key_list.sql": models__empty_unique_key_list_sql,
            "no_unique_key.sql": models__no_unique_key_sql,
            "empty_str_unique_key.sql": models__empty_str_unique_key_sql,
            "str_unique_key.sql": models__str_unique_key_sql,
            "duplicated_unary_unique_key_list.sql": models__duplicated_unary_unique_key_list_sql,
            "not_found_unique_key_list.sql": models__not_found_unique_key_list_sql,
            "expected": {
                "one_str__overwrite.sql": models__expected__one_str__overwrite_sql,
                "unique_key_list__inplace_overwrite.sql": models__expected__unique_key_list__inplace_overwrite_sql,
            },
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "duplicate_insert.sql": seeds__duplicate_insert_sql,
            "seed.csv": seeds__seed_csv,
            "add_new_rows.sql": seeds__add_new_rows_sql,
        }