from dataclasses import dataclass, field
from typing import List, FrozenSet, Type
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.contracts.relation import RelationType
from dbt.adapters.relation_configs import RelationConfigBase
from dbt_common.exceptions import DbtRuntimeError
import logging
from typing import Dict, final
from dbt.adapters.saphanacloud.relation_configs.index import (
    SapHanaCloudIndexConfig,
    SapHanaCloudIndexConfigChange
)
from dbt.adapters.relation_configs import (
    RelationConfigChangeAction
)


@dataclass(frozen=True, eq=False, repr=False)
class SapHanaCloudRelation(BaseRelation):
    DEFAULTS = {
        'type': None,
    }
    quote_character: str = '"'
    logging.basicConfig(level=logging.INFO)

    def log(message):
        logging.info(message)

    renameable_relations: FrozenSet[RelationType] = field(
        default_factory=lambda: frozenset({
            RelationType.Table,
            RelationType.View,
        })
    )

    replaceable_relations: FrozenSet[RelationType] = field(
        default_factory=lambda: frozenset({
            RelationType.Table,
            RelationType.View,
        })
    )

    def __init__(self, *args, **kwargs):
        # Filter out any keyword arguments not supported by the superclass
        supported_kwargs = {key: kwargs[key] for key in kwargs if key not in ['schema', 'identifier', 'database']}
        super().__init__(*args, **supported_kwargs)

    @classmethod
    def get_relation_type(cls) -> Type[RelationType]:
        return RelationType

    @classmethod
    def from_config(cls, config: RelationConfigBase) -> RelationConfigBase:
        relation_type: str = config.config.materialized

        if relation_type not in [RelationType.Table, RelationType.View]:
            raise DbtRuntimeError(
                f"from_config() is not supported for the provided relation type: {relation_type}"
            )
        return cls(relation_type=RelationType(relation_type))

    @property
    def is_table(self) -> bool:
        return self.type == RelationType.Table

    @property
    def is_view(self) -> bool:
        return self.type == RelationType.View
    
    def to_dict(self):
        value = super().to_dict()
        return value

    @classmethod
    @final
    def from_dict(cls, value: Dict):
        return super().from_dict(value)

    def render(self) -> str:
        # Ensure all parts are strings before joining
        return ".".join(
            str(part) for part in filter(None, [
                self.schema,   # Ensure this is not a list
                self.identifier      # Ensure this is not a list
            ])
        )
    
    def _get_index_config_changes(
        self,
        existing_indexes: FrozenSet[SapHanaCloudIndexConfig],
        new_indexes: FrozenSet[SapHanaCloudIndexConfig],
    ) -> List[SapHanaCloudIndexConfigChange]:
        """
        Get the index updates that will occur as a result of a new run

        There are four scenarios:

        1. Indexes are equal -> don't return these
        2. Index is new -> create these
        3. Index is old -> drop these
        4. Indexes are not equal -> drop old, create new -> two actions

        *Note:*
            The order of the operations matters here because if the same index is dropped and recreated
            (e.g. via --full-refresh) then we need to drop it first, then create it.

        Returns: an ordered list of index updates in the form {"action": "drop/create", "context": <IndexConfig>}
        """
        drop_changes = [
            SapHanaCloudIndexConfigChange.from_dict(
                {"action": RelationConfigChangeAction.drop, "context": index}
            )
            for index in existing_indexes.difference(new_indexes)
        ]
        create_changes = [
            SapHanaCloudIndexConfigChange.from_dict(
                {"action": RelationConfigChangeAction.create, "context": index}
            )
            for index in new_indexes.difference(existing_indexes)
        ]
        return drop_changes + create_changes