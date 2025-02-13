import pytest

from dbt.tests.adapter.utils.test_concat import BaseConcat
from dbt.tests.adapter.utils.test_intersect import BaseIntersect
from dbt.tests.adapter.utils.test_escape_single_quotes import BaseEscapeSingleQuotesQuote
from dbt.tests.adapter.utils.test_except import BaseExcept
from dbt.tests.adapter.utils.test_hash import BaseHash
from dbt.tests.adapter.utils.test_string_literal import BaseStringLiteral
from dbt.tests.adapter.utils.test_position import BasePosition
from dbt.tests.adapter.utils.test_right import BaseRight
from dbt.tests.adapter.utils.test_cast_bool_to_text import BaseCastBoolToText
from dbt.tests.adapter.utils.test_replace import BaseReplace
from dbt.tests.adapter.utils.test_length import BaseLength
from dbt.tests.adapter.utils.test_current_timestamp import BaseCurrentTimestampNaive


from dbt.tests.adapter.utils.fixture_cast_bool_to_text import models__test_cast_bool_to_text_yml
from dbt.tests.adapter.utils.fixture_escape_single_quotes import models__test_escape_single_quotes_yml
from dbt.tests.adapter.utils.fixture_string_literal import models__test_string_literal_yml
from dbt.tests.adapter.utils.fixture_right import models__test_right_yml
from dbt.tests.adapter.utils.fixture_replace import models__test_replace_yml



seeds__data_hash_csv = """input_1,output
ab,187EF4436122D1CC2F40DC2B92F0EBA0
a,0CC175B9C0F1B6A831C399E269772661
1,C4CA4238A0B923820DCC509A6F75849B
,6ADF97F83ACF6453D4A6A4B1070F3754
"""

models__test_string_literal_sql = """
SELECT {{ string_literal("abc") }} AS actual, 'abc' AS expected FROM DUMMY
UNION ALL
SELECT {{ string_literal("1") }} AS actual, '1' AS expected FROM DUMMY
UNION ALL
SELECT {{ string_literal("") }} AS actual, '' AS expected FROM DUMMY
UNION ALL
SELECT {{ string_literal(none) }} AS actual, 'None' AS expected FROM DUMMY
"""


models__test_escape_single_quotes_quote_sql = """
SELECT
  'they''re' AS actual,
  'they''re' AS expected,
  LENGTH('they''re') AS actual_length,
  7 AS expected_length
FROM DUMMY
UNION ALL
SELECT
  'they are' AS actual,
  'they are' AS expected,
  LENGTH('they are') AS actual_length,
  8 AS expected_length
FROM DUMMY
"""

models__test_cast_bool_to_text_sql = """
WITH data AS (
    SELECT 0 AS input, 'false' AS expected FROM dummy UNION ALL 
    SELECT 1 AS input, 'true' AS expected FROM dummy
)
SELECT
    CASE 
        WHEN input = 1 THEN 'true'
        ELSE 'false'
    END AS actual,
    expected
FROM data
"""

models__test_right_sql = """
WITH data AS (
    SELECT * FROM {{ ref('data_right') }}
)

SELECT
    CASE
        WHEN string_text IS NULL OR length_expression IS NULL THEN 'None'
        WHEN LENGTH(string_text) = 0 OR length_expression <= 0 THEN 'None'
        WHEN LENGTH(string_text) < length_expression THEN 'None'
        ELSE RIGHT(string_text, length_expression)
    END AS actual,
    COALESCE(output, 'None') AS expected
FROM data
"""
models__test_replace_sql = """
with data as (

    select

        string_text,
        coalesce(search_chars, '') as old_chars,
        coalesce(replace_chars, '') as new_chars,
        result

    from {{ ref('data_replace') }}

)

select

   REPLACE(
        string_text,
        old_chars,
        CASE 
            WHEN new_chars = 'None' THEN ''
            WHEN new_chars IS NULL THEN ''
            ELSE new_chars
        END                   
    ) AS actual,
    result AS expected
FROM data
"""

models__current_ts_sql = """
SELECT CURRENT_TIMESTAMP AS current_ts_column FROM DUMMY
"""

class TestCurrentTimestampNaive(BaseCurrentTimestampNaive):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "current_ts.sql": models__current_ts_sql,
        }
    
    
class TestCastBoolToText(BaseCastBoolToText):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_cast_bool_to_text.yml": models__test_cast_bool_to_text_yml,
            "test_cast_bool_to_text.sql": self.interpolate_macro_namespace(
                models__test_cast_bool_to_text_sql, "cast_bool_to_text"
            ),
        }
    
class TestIntersect(BaseIntersect):
    pass

class TestConcat(BaseConcat):
    pass

class TestEscapeSingleQuotesQuote(BaseEscapeSingleQuotesQuote):

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_escape_single_quotes.yml": models__test_escape_single_quotes_yml,
            "test_escape_single_quotes.sql": self.interpolate_macro_namespace(
                models__test_escape_single_quotes_quote_sql, "escape_single_quotes"
            ),
        }

class TestExcept(BaseExcept):
    pass

class TestHash(BaseHash):

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_hash.csv": seeds__data_hash_csv}

class TestStringLiteral(BaseStringLiteral):

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_string_literal.yml": models__test_string_literal_yml,
            "test_string_literal.sql": self.interpolate_macro_namespace(
                models__test_string_literal_sql, "string_literal"
            ),
        }
    
class TestStringPosition(BasePosition):
    pass

class TestStringRight(BaseRight):
    
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_right.yml": models__test_right_yml,
            "test_right.sql": self.interpolate_macro_namespace(
                models__test_right_sql, "right"
            ),
        }

class TestStringReplace(BaseReplace):

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_replace.yml": models__test_replace_yml,
            "test_replace.sql": self.interpolate_macro_namespace(
                models__test_replace_sql, "replace"
            ),
        }
    
class TestStringLength(BaseLength):
    pass

class TestCurrentTimestampNaiveHana(BaseCurrentTimestampNaive):

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "current_ts.sql": models__current_ts_sql,
        }