import pytest

from dbt.tests.adapter.utils.data_types.test_type_boolean import BaseTypeBoolean
from dbt.tests.adapter.utils.data_types.test_type_bigint import BaseTypeBigInt
from dbt.tests.adapter.utils.data_types.test_type_float import BaseTypeFloat
from dbt.tests.adapter.utils.data_types.test_type_int import BaseTypeInt
from dbt.tests.adapter.utils.data_types.test_type_numeric import BaseTypeNumeric

models__bigint_expected_sql = """
select 9223372036854775800 as bigint_col from dummy
"""

models__bigint_actual_sql = """
select cast('9223372036854775800' as {{ type_bigint() }}) as bigint_col from dummy
"""

models__float_actual_sql = """
select cast('1.2345' as {{ type_float() }}) as float_col from dummy
"""

models__int_actual_sql = """
select cast('12345678' as {{ type_int() }}) as int_col from dummy
"""

models__numeric_actual_sql = """
select cast('1.2345' as {{ type_numeric() }}) as numeric_col from dummy
"""

seeds__boolean_expected_csv = """boolean_col
TRUE
""".lstrip()

models__boolean_actual_sql = """
select cast('TRUE' as {{ type_boolean() }}) as boolean_col from dummy
"""

class TestTypeBigIntHana(BaseTypeBigInt):

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "expected.sql": models__bigint_expected_sql,
            "actual.sql": self.interpolate_macro_namespace(models__bigint_actual_sql, "type_bigint"),
        }
    
class TestTypeFloatHana(BaseTypeFloat):

    @pytest.fixture(scope="class")
    def models(self):
        return {"actual.sql": self.interpolate_macro_namespace(models__float_actual_sql, "type_float")}

class TestTypeIntHana(BaseTypeInt):

    @pytest.fixture(scope="class")
    def models(self):
        return {"actual.sql": self.interpolate_macro_namespace(models__int_actual_sql, "type_int")}
    
class TestTypeNumericHana(BaseTypeNumeric):

    @pytest.fixture(scope="class")
    def models(self):
        return {"actual.sql": self.interpolate_macro_namespace(models__numeric_actual_sql, "type_numeric")}
    
class TestTypeBooleanHana(BaseTypeBoolean):

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"expected.csv": seeds__boolean_expected_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {"actual.sql": self.interpolate_macro_namespace(models__boolean_actual_sql, "type_boolean")}