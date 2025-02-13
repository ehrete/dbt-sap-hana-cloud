from dataclasses import dataclass, field
from typing import Set, FrozenSet
import agate
from dbt_common.dataclass_schema import StrEnum
from dbt_common.exceptions import DbtRuntimeError
from dbt.adapters.relation_configs import (
    RelationConfigBase,
    RelationConfigValidationMixin,
    RelationConfigValidationRule,
    RelationConfigChangeAction,
    RelationConfigChange,
)


class SapHanaCloudIndexMethod(StrEnum):
    btree = "BTREE"
    cpbtree = "CPBTREE"
    inverted_hash = "INVERTED HASH"
    inverted_value = "INVERTED VALUE"
    inverted_individual = "INVERTED INDIVIDUAL"

    @classmethod
    def default(cls) -> "SapHanaCloudIndexMethod":
        return cls("BTREE")


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SapHanaCloudIndexConfig(RelationConfigBase, RelationConfigValidationMixin):
    """
    SAP HANA Index Configuration:
    - Supports BTREE and CPBTREE for row tables.
    - Supports INVERTED HASH, INVERTED VALUE, and INVERTED INDIVIDUAL for column tables.
    - Configures index properties such as unique constraint and column names.
    """

    name: str = field(default="", hash=False, compare=False)
    column_names: FrozenSet[str] = field(default_factory=frozenset, hash=True)
    unique: bool = field(default=False, hash=True)
    method: SapHanaCloudIndexMethod = field(default=SapHanaCloudIndexMethod.default(), hash=True)

    @property
    def validation_rules(self) -> Set[RelationConfigValidationRule]:
        return {
            RelationConfigValidationRule(
                validation_check=self.column_names is not None,
                validation_error=DbtRuntimeError(
                    "Indexes require at least one column, but none were provided"
                ),
            ),
        }

    @classmethod
    def from_dict(cls, config_dict) -> "SapHanaCloudIndexConfig":
        kwargs_dict = {
            "name": config_dict.get("name"),
            "column_names": frozenset(
                column.lower() for column in config_dict.get("column_names", set())
            ),
            "unique": config_dict.get("unique"),
            "method": config_dict.get("method"),
        }
        index: "SapHanaCloudIndexConfig" = super().from_dict(kwargs_dict)  # type: ignore
        return index

    @classmethod
    def parse_model_node(cls, model_node_entry: dict) -> dict:
        config_dict = {
            "column_names": set(model_node_entry.get("columns", set())),
            "unique": model_node_entry.get("unique"),
            "method": model_node_entry.get("type"),
        }
        return config_dict

    @classmethod
    def parse_relation_results(cls, relation_results_entry: agate.Row) -> dict:
        config_dict = {
            "name": relation_results_entry.get("name"),
            "column_names": set(relation_results_entry.get("column_names", "").split(",")),
            "unique": relation_results_entry.get("unique"),
            "method": relation_results_entry.get("method"),
        }
        return config_dict

    @property
    def as_node_config(self) -> dict:
        """
        Returns: a dictionary that can be passed into `get_create_index_sql()`
        """
        node_config = {
            "columns": list(self.column_names),
            "unique": self.unique,
            "type": self.method.value,
        }
        return node_config


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SapHanaCloudIndexConfigChange(RelationConfigChange, RelationConfigValidationMixin):
    """
    Tracks changes in index configuration for SAP HANA.
    """

    context: SapHanaCloudIndexConfig

    @property
    def requires_full_refresh(self) -> bool:
        return False

    @property
    def validation_rules(self) -> Set[RelationConfigValidationRule]:
        return {
            RelationConfigValidationRule(
                validation_check=self.action
                in {RelationConfigChangeAction.create, RelationConfigChangeAction.drop},
                validation_error=DbtRuntimeError(
                    "Invalid operation, only `drop` and `create` changes are supported for indexes."
                ),
            ),
            RelationConfigValidationRule(
                validation_check=not (
                    self.action == RelationConfigChangeAction.drop and self.context.name is None
                ),
                validation_error=DbtRuntimeError(
                    "Invalid operation, attempting to drop an index with no name."
                ),
            ),
            RelationConfigValidationRule(
                validation_check=not (
                    self.action == RelationConfigChangeAction.create
                    and self.context.column_names == set()
                ),
                validation_error=DbtRuntimeError(
                    "Invalid operations, attempting to create an index with no columns."
                ),
            ),
        }
