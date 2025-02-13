import pytest

from dbt.tests.adapter.utils.base_utils import BaseUtils
from dbt.tests.adapter.utils.fixture_date_spine import (
    models__test_date_spine_yml,
)

models__test_date_spine_sql = """
WITH generated_dates AS (
    SELECT ADD_DAYS(DATE '2023-09-01', seq) AS date_day
        FROM (SELECT GENERATED_PERIOD_START AS seq FROM SERIES_GENERATE_INTEGER(1, 0, 9))
),
expected_dates AS (
    SELECT DATE '2023-09-01' AS expected FROM DUMMY
    UNION ALL
    SELECT DATE '2023-09-02' FROM DUMMY
    UNION ALL
    SELECT DATE '2023-09-03' FROM DUMMY
    UNION ALL
    SELECT DATE '2023-09-04' FROM DUMMY
    UNION ALL
    SELECT DATE '2023-09-05' FROM DUMMY
    UNION ALL
    SELECT DATE '2023-09-06' FROM DUMMY
    UNION ALL
    SELECT DATE '2023-09-07' FROM DUMMY
    UNION ALL
    SELECT DATE '2023-09-08' FROM DUMMY
    UNION ALL
    SELECT DATE '2023-09-09' FROM DUMMY
)
SELECT
    generated_dates.date_day,
    expected_dates.expected
FROM generated_dates
LEFT JOIN expected_dates ON generated_dates.date_day = expected_dates.expected
"""


class BaseDateSpine(BaseUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_date_spine.yml": models__test_date_spine_yml,
            "test_date_spine.sql": self.interpolate_macro_namespace(
                models__test_date_spine_sql, "date_spine"
            ),
        }


class TestDateSpine(BaseDateSpine):
    pass