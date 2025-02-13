import pytest

from dbt.tests.adapter.utils.test_dateadd import BaseDateAdd
from dbt.tests.adapter.utils.test_datediff import BaseDateDiff

from dbt.tests.adapter.utils.fixture_dateadd import models__test_dateadd_yml
from dbt.tests.adapter.utils.fixture_datediff import models__test_datediff_yml



seeds__data_dateadd_csv = """from_time,interval_length,datepart,expected
2018-01-20T01:00:00,1,day,2018-01-21T01:00:00
2018-01-20T01:00:00,1,month,2018-02-20T01:00:00
2018-01-20T01:00:00,1,year,2019-01-20T01:00:00
2018-01-20T01:00:00,1,hour,2018-01-20T02:00:00
2021-02-28T01:23:45,1,quarter,2021-05-28T01:23:45
"""


models__test_dateadd_sql = """
WITH data AS (
    SELECT * FROM {{ ref('data_dateadd') }}
)
SELECT
    CASE
        WHEN datepart = 'hour' THEN ADD_SECONDS(from_time, interval_length * 3600)
        WHEN datepart = 'day' THEN ADD_DAYS(from_time, interval_length)
        WHEN datepart = 'month' THEN ADD_MONTHS(from_time, interval_length)
        WHEN datepart = 'year' THEN ADD_YEARS(from_time, interval_length)
        WHEN datepart = 'quarter' THEN ADD_MONTHS(from_time, interval_length * 3)
        ELSE NULL
    END AS actual,
    expected
FROM data
"""

# class TestDateAdd(BaseDateAdd):

#     @pytest.fixture(scope="class")
#     def seeds(self):
#         return {"data_dateadd.csv": seeds__data_dateadd_csv}

#     @pytest.fixture(scope="class")
#     def models(self):
#         return {
#             "test_dateadd.yml": models__test_dateadd_yml,
#             "test_dateadd.sql": self.interpolate_macro_namespace(
#                 models__test_dateadd_sql, "dateadd"
#             ),
#         }

# Hana doesnt have any equivalent date_trunc
# class TestDateTrunc(BaseDateTrunc):
#     pass

# Implement this macro when needed
# class TestLastDay(BaseLastDay):
#     pass

