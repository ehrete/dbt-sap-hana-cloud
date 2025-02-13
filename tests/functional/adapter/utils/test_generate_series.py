import pytest
from dbt.tests.adapter.utils.base_utils import BaseUtils
from dbt.tests.adapter.utils.fixture_generate_series import (
    models__test_generate_series_yml,
)

models__test_generate_series_sql = """
WITH generated_numbers AS (
    SELECT SEQ AS generated_number
    FROM (SELECT GENERATED_PERIOD_START AS seq FROM SERIES_GENERATE_INTEGER(1, 1, 10))
), expected_numbers AS (
    SELECT 1 AS expected FROM DUMMY
    UNION ALL
    SELECT 2 AS expected FROM DUMMY
    UNION ALL
    SELECT 3 AS expected FROM DUMMY
    UNION ALL
    SELECT 4 AS expected FROM DUMMY
    UNION ALL
    SELECT 5 AS expected FROM DUMMY
    UNION ALL
    SELECT 6 AS expected FROM DUMMY
    UNION ALL
    SELECT 7 AS expected FROM DUMMY
    UNION ALL
    SELECT 8 AS expected FROM DUMMY
    UNION ALL
    SELECT 9 AS expected FROM DUMMY
    UNION ALL
    SELECT 10 AS expected FROM DUMMY
), joined AS (
    SELECT
        generated_numbers.generated_number,
        expected_numbers.expected
    FROM generated_numbers
    LEFT JOIN expected_numbers
    ON generated_numbers.generated_number = expected_numbers.expected
)
SELECT * FROM joined
"""


class BaseGenerateSeries(BaseUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_generate_series.yml": models__test_generate_series_yml,
            "test_generate_series.sql": self.interpolate_macro_namespace(
                models__test_generate_series_sql, "generate_series"
            ),
        }

class TestGenerateSeries(BaseGenerateSeries):
    pass