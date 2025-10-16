from datetime import datetime, timedelta
from typing import ClassVar

from pydantic import field_serializer, field_validator, TypeAdapter

from liti.core.model.v1.operation.data.base import EntityKind, Operation
from liti.core.model.v1.schema import ColumnName, ConstraintName, ForeignKey, Identifier, IntervalLiteral, PrimaryKey, \
    QualifiedName, RoundingMode, Schema, StorageBilling, Table


class CreateSchema(Operation):
    """ Semantics: `CREATE SCHEMA` """

    schema_object: Schema

    KIND: ClassVar[str] = 'create_schema'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'SCHEMA'}


class DropSchema(Operation):
    """ Semantics: `DROP SCHEMA`

    The down migration for drop schema does NOT attempt to recreate anything within the schema. It only recreates the
    schema itself with the same properties it had when dropped. This behavior may change in the future.
    """

    schema_name: QualifiedName

    KIND: ClassVar[str] = 'drop_schema'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'SCHEMA'}


class SetDefaultTableExpiration(Operation):
    schema_name: QualifiedName
    expiration: timedelta | None = None

    KIND: ClassVar[str] = 'set_default_table_expiration'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'SCHEMA'}

    @field_serializer('expiration')
    @classmethod
    def serialize_timedelta(cls, value: timedelta | None) -> str | None:
        return value and TypeAdapter(timedelta).dump_python(value, mode='json')


class SetDefaultPartitionExpiration(Operation):
    schema_name: QualifiedName
    expiration: timedelta | None = None

    KIND: ClassVar[str] = 'set_default_partition_expiration'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'SCHEMA'}

    @field_serializer('expiration')
    @classmethod
    def serialize_timedelta(cls, value: timedelta | None) -> str | None:
        return value and TypeAdapter(timedelta).dump_python(value, mode='json')


class SetDefaultKmsKeyName(Operation):
    schema_name: QualifiedName
    key_name: str | None = None

    KIND: ClassVar[str] = 'set_default_kms_key_name'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'SCHEMA'}


class SetFailoverReservation(Operation):
    schema_name: QualifiedName
    reservation: str | None = None

    KIND: ClassVar[str] = 'set_failover_reservation'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'SCHEMA'}


class SetCaseSensitive(Operation):
    schema_name: QualifiedName
    case_sensitive: bool

    KIND: ClassVar[str] = 'set_case_sensitive'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'SCHEMA'}


class SetIsPrimaryReplica(Operation):
    schema_name: QualifiedName
    is_primary: bool

    KIND: ClassVar[str] = 'set_is_primary_replica'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'SCHEMA'}


class SetPrimaryReplica(Operation):
    schema_name: QualifiedName
    replica: str | None = None

    KIND: ClassVar[str] = 'set_primary_replica'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'SCHEMA'}


class SetMaxTimeTravel(Operation):
    schema_name: QualifiedName
    duration: timedelta | None = None

    KIND: ClassVar[str] = 'set_max_time_travel'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'SCHEMA'}

    @field_serializer('duration')
    @classmethod
    def serialize_timedelta(cls, value: timedelta | None) -> str | None:
        return value and TypeAdapter(timedelta).dump_python(value, mode='json')


class SetStorageBilling(Operation):
    schema_name: QualifiedName
    storage_billing: StorageBilling

    KIND: ClassVar[str] = 'set_storage_billing'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'SCHEMA'}


class CreateTable(Operation):
    """ Semantics: `CREATE TABLE` """

    table: Table

    KIND: ClassVar[str] = 'create_table'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'TABLE'}


class DropTable(Operation):
    """ Semantics: `DROP TABLE` """

    table_name: QualifiedName

    KIND: ClassVar[str] = 'drop_table'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'TABLE'}


class RenameTable(Operation):
    from_name: QualifiedName
    to_name: Identifier

    KIND: ClassVar[str] = 'rename_table'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'TABLE'}


class SetPrimaryKey(Operation):
    table_name: QualifiedName
    primary_key: PrimaryKey | None = None

    KIND: ClassVar[str] = 'set_primary_key'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'TABLE'}


class AddForeignKey(Operation):
    table_name: QualifiedName
    foreign_key: ForeignKey

    KIND: ClassVar[str] = 'add_foreign_key'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'TABLE'}


class DropConstraint(Operation):
    table_name: QualifiedName
    constraint_name: ConstraintName

    KIND: ClassVar[str] = 'drop_constraint'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'TABLE'}


class SetPartitionExpiration(Operation):
    table_name: QualifiedName
    expiration: timedelta | None = None

    KIND: ClassVar[str] = 'set_partition_expiration'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'TABLE'}

    @field_serializer('expiration')
    @classmethod
    def serialize_timedelta(cls, value: timedelta | None) -> str | None:
        return value and TypeAdapter(timedelta).dump_python(value, mode='json')


class SetRequirePartitionFilter(Operation):
    table_name: QualifiedName
    require_filter: bool

    KIND: ClassVar[str] = 'set_require_partition_filter'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'TABLE'}


class SetClustering(Operation):
    table_name: QualifiedName
    column_names: list[ColumnName] | None = None

    KIND: ClassVar[str] = 'set_clustering'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'TABLE'}

    @field_validator('column_names', mode='before')
    @classmethod
    def validate_column_names(cls, value: list[ColumnName] | None) -> list[ColumnName] | None:
        if value:
            return value
        else:
            return None


class SetFriendlyName(Operation):
    entity_name: QualifiedName
    friendly_name: str | None = None

    KIND: ClassVar[str] = 'set_friendly_name'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'SCHEMA', 'TABLE', 'VIEW', 'MATERIALIZED_VIEW'}


class SetDescription(Operation):
    entity_name: QualifiedName
    description: str | None = None

    KIND: ClassVar[str] = 'set_description'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'SCHEMA', 'TABLE', 'VIEW', 'MATERIALIZED_VIEW'}


class SetLabels(Operation):
    entity_name: QualifiedName
    labels: dict[str, str] | None = None

    KIND: ClassVar[str] = 'set_labels'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'SCHEMA', 'TABLE', 'VIEW', 'MATERIALIZED_VIEW'}


class SetTags(Operation):
    entity_name: QualifiedName
    tags: dict[str, str] | None = None

    KIND: ClassVar[str] = 'set_tags'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'SCHEMA', 'TABLE', 'VIEW', 'MATERIALIZED_VIEW'}


class SetExpirationTimestamp(Operation):
    entity_name: QualifiedName
    expiration_timestamp: datetime | None = None

    KIND: ClassVar[str] = 'set_expiration_timestamp'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'TABLE', 'VIEW', 'MATERIALIZED_VIEW'}


class SetDefaultRoundingMode(Operation):
    entity_name: QualifiedName
    rounding_mode: RoundingMode | None = None

    KIND: ClassVar[str] = 'set_default_rounding_mode'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'SCHEMA', 'TABLE'}


class SetMaxStaleness(Operation):
    entity_name: QualifiedName
    max_staleness: IntervalLiteral | None = None

    KIND: ClassVar[str] = 'set_max_staleness'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'TABLE', 'MATERIALIZED_VIEW'}


class SetEnableChangeHistory(Operation):
    table_name: QualifiedName
    enabled: bool

    KIND: ClassVar[str] = 'set_enable_change_history'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'TABLE'}


class SetEnableFineGrainedMutations(Operation):
    table_name: QualifiedName
    enabled: bool

    KIND: ClassVar[str] = 'set_enable_fine_grained_mutations'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'TABLE'}


class SetKmsKeyName(Operation):
    table_name: QualifiedName
    key_name: str | None = None

    KIND: ClassVar[str] = 'set_kms_key_name'

    @property
    def supported_entity_kinds(self) -> set[EntityKind]:
        return {'TABLE'}
