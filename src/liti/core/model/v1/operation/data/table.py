from datetime import datetime
from typing import ClassVar

from pydantic import field_validator

from liti.core.model.v1.operation.data.base import Operation
from liti.core.model.v1.schema import ColumnName, ForeignKey, Identifier, IntervalLiteral, PrimaryKey, RoundingMode, \
    Table, TableName


class CreateTable(Operation):
    """ Semantics: CREATE """

    table: Table

    KIND: ClassVar[str] = 'create_table'


class DropTable(Operation):
    """ Semantics: DROP """

    table_name: TableName

    KIND: ClassVar[str] = 'drop_table'


class RenameTable(Operation):
    from_name: TableName
    to_name: Identifier

    KIND: ClassVar[str] = 'rename_table'


class SetPrimaryKey(Operation):
    table_name: TableName
    primary_key: PrimaryKey | None = None

    KIND: ClassVar[str] = 'set_primary_key'


class AddForeignKey(Operation):
    table_name: TableName
    foreign_key: ForeignKey

    KIND: ClassVar[str] = 'add_foreign_key'


class DropConstraint(Operation):
    table_name: TableName
    constraint_name: Identifier

    KIND: ClassVar[str] = 'drop_constraint'


class SetPartitionExpiration(Operation):
    table_name: TableName
    expiration_days: float | None = None

    KIND: ClassVar[str] = 'set_partition_expiration'


class SetRequirePartitionFilter(Operation):
    table_name: TableName
    require_filter: bool

    KIND: ClassVar[str] = 'set_require_partition_filter'


class SetClustering(Operation):
    table_name: TableName
    column_names: list[ColumnName] | None = None

    KIND: ClassVar[str] = 'set_clustering'

    @field_validator('column_names', mode='before')
    @classmethod
    def validate_column_names(cls, value: list[ColumnName] | None) -> list[ColumnName] | None:
        if value:
            return value
        else:
            return None


class SetDescription(Operation):
    table_name: TableName
    description: str | None = None

    KIND: ClassVar[str] = 'set_description'


class SetLabels(Operation):
    table_name: TableName
    labels: dict[str, str] | None = None

    KIND: ClassVar[str] = 'set_labels'


class SetTags(Operation):
    table_name: TableName
    tags: dict[str, str] | None = None

    KIND: ClassVar[str] = 'set_tags'


class SetExpirationTimestamp(Operation):
    table_name: TableName
    expiration_timestamp: datetime | None = None

    KIND: ClassVar[str] = 'set_expiration_timestamp'


class SetDefaultRoundingMode(Operation):
    table_name: TableName
    rounding_mode: RoundingMode | None = None

    KIND: ClassVar[str] = 'set_default_rounding_mode'


class SetMaxStaleness(Operation):
    table_name: TableName
    max_staleness: IntervalLiteral | None = None

    KIND: ClassVar[str] = 'set_max_staleness'


class SetEnableChangeHistory(Operation):
    table_name: TableName
    enabled: bool

    KIND: ClassVar[str] = 'set_enable_change_history'


class SetEnableFineGrainedMutations(Operation):
    table_name: TableName
    enabled: bool

    KIND: ClassVar[str] = 'set_enable_fine_grained_mutations'


class SetKmsKeyName(Operation):
    table_name: TableName
    key_name: str | None = None

    KIND: ClassVar[str] = 'set_kms_key_name'
