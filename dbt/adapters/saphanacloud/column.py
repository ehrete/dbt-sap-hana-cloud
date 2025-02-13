from dataclasses import dataclass
from typing import Dict, ClassVar
from dbt.adapters.base.column import Column


@dataclass
class SapHanaCloudColumn(Column):
    # Map commonly used HANA types
    TYPE_LABELS: ClassVar[Dict[str, str]] = {
        "STRING": "NVARCHAR(5000)",  # Adjusted default size for HANA
        "TIMESTAMP": "TIMESTAMP",
        "FLOAT": "FLOAT",
        "INTEGER": "INTEGER",
    }


    STRING_DATATYPES = {'char', 'varchar', 'nvarchar'}
    NUMBER_DATATYPES = {'decimal', 'float', 'integer', 'bigint'}


    @property
    def data_type(self) -> str:
        if self.is_string():
            return self.hana_string_type(self.dtype, self.string_size())
        elif self.is_numeric():
            return self.numeric_type(self.dtype, self.numeric_precision, self.numeric_scale)
        else:
            return self.dtype


    @classmethod
    def hana_string_type(cls, dtype: str, size: int = None):
        """
            - CHAR(SIZE)
            - VARCHAR(SIZE)
            - NVARCHAR(SIZE)
        """
        if size is None:
            return dtype
        else:
            return "{}({})".format(dtype, size)


    def is_numeric(self) -> bool:
        if self.dtype.lower() in self.NUMBER_DATATYPES:
            return True
        return super().is_numeric()


    def is_string(self) -> bool:
        if self.dtype.lower() in self.STRING_DATATYPES:
            return True
        return super().is_string()


