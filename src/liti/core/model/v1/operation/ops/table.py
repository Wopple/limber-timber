from liti.core.context import Context
from liti.core.model.v1.operation.data import table as d
from liti.core.model.v1.operation.ops.base import OperationOps


class CreateSchemaOps(OperationOps):
    op: d.CreateSchema

    def __init__(self, op: d.CreateSchema, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.db_backend.create_schema(self.op.schema_object)

    def down(self) -> d.DropSchema:
        return d.DropSchema(schema_name=self.op.schema_object.name)

    def is_up(self) -> bool:
        return self.db_backend.has_schema(self.op.schema_object.name)


class DropSchemaOps(OperationOps):
    op: d.DropSchema

    def __init__(self, op: d.DropSchema, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.db_backend.drop_schema(self.op.schema_name)

    # TODO: figure out how to recreate the resources within the schema when it was dropped
    def down(self) -> d.CreateSchema:
        sim_db = self.simulate(self.meta_backend.get_previous_operations())
        sim_schema = sim_db.get_schema(self.op.schema_name)
        return d.CreateSchema(schema_object=sim_schema)

    def is_up(self) -> bool:
        return not self.db_backend.has_schema(self.op.schema_name)


class SetDefaultTableExpirationOps(OperationOps):
    op: d.SetDefaultTableExpiration

    def __init__(self, op: d.SetDefaultTableExpiration, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.db_backend.set_default_table_expiration(self.op.schema_name, self.op.expiration)

    def down(self) -> d.SetDefaultTableExpiration:
        sim_db = self.simulate(self.meta_backend.get_previous_operations())
        sim_schema = sim_db.get_schema(self.op.schema_name)

        return d.SetDefaultTableExpiration(
            schema_name=self.op.schema_name,
            expiration=sim_schema.default_table_expiration,
        )

    def is_up(self) -> bool:
        return self.db_backend.get_schema(self.op.schema_name).default_table_expiration == self.op.expiration


class SetDefaultPartitionExpirationOps(OperationOps):
    op: d.SetDefaultPartitionExpiration

    def __init__(self, op: d.SetDefaultPartitionExpiration, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.db_backend.set_default_partition_expiration(self.op.schema_name, self.op.expiration)

    def down(self) -> d.SetDefaultPartitionExpiration:
        sim_db = self.simulate(self.meta_backend.get_previous_operations())
        sim_schema = sim_db.get_schema(self.op.schema_name)

        return d.SetDefaultPartitionExpiration(
            schema_name=self.op.schema_name,
            expiration=sim_schema.default_partition_expiration,
        )

    def is_up(self) -> bool:
        return self.db_backend.get_schema(self.op.schema_name).default_partition_expiration == self.op.expiration


class SetDefaultKmsKeyNameOps(OperationOps):
    op: d.SetDefaultKmsKeyName

    def __init__(self, op: d.SetDefaultKmsKeyName, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.db_backend.set_default_kms_key_name(self.op.schema_name, self.op.key_name)

    def down(self) -> d.SetDefaultKmsKeyName:
        sim_db = self.simulate(self.meta_backend.get_previous_operations())
        sim_schema = sim_db.get_schema(self.op.schema_name)
        return d.SetDefaultKmsKeyName(schema_name=self.op.schema_name, key_name=sim_schema.default_kms_key_name)

    def is_up(self) -> bool:
        return self.db_backend.get_schema(self.op.schema_name).default_kms_key_name == self.op.key_name


class SetFailoverReservationOps(OperationOps):
    op: d.SetFailoverReservation

    def __init__(self, op: d.SetFailoverReservation, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.db_backend.set_failover_reservation(self.op.schema_name, self.op.reservation)

    def down(self) -> d.SetFailoverReservation:
        sim_db = self.simulate(self.meta_backend.get_previous_operations())
        sim_schema = sim_db.get_schema(self.op.schema_name)
        return d.SetFailoverReservation(schema_name=self.op.schema_name, reservation=sim_schema.failover_reservation)

    def is_up(self) -> bool:
        # circular import
        from liti.core.backend.bigquery import BigQueryDbBackend

        if isinstance(self.db_backend, BigQueryDbBackend):
            # the big query API does not provide the failover reservation, always apply
            return False
        else:
            return self.db_backend.get_schema(self.op.schema_name).failover_reservation == self.op.reservation


class SetCaseSensitiveOps(OperationOps):
    op: d.SetCaseSensitive

    def __init__(self, op: d.SetCaseSensitive, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.db_backend.set_case_sensitive(self.op.schema_name, self.op.case_sensitive)

    def down(self) -> d.SetCaseSensitive:
        sim_db = self.simulate(self.meta_backend.get_previous_operations())
        sim_schema = sim_db.get_schema(self.op.schema_name)
        return d.SetCaseSensitive(schema_name=self.op.schema_name, case_sensitive=sim_schema.is_case_sensitive)

    def is_up(self) -> bool:
        return self.db_backend.get_schema(self.op.schema_name).is_case_sensitive is self.op.case_sensitive


class SetIsPrimaryReplicaOps(OperationOps):
    op: d.SetIsPrimaryReplica

    def __init__(self, op: d.SetIsPrimaryReplica, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.db_backend.set_is_primary_replica(self.op.schema_name, self.op.is_primary)

    def down(self) -> d.SetIsPrimaryReplica:
        sim_db = self.simulate(self.meta_backend.get_previous_operations())
        sim_schema = sim_db.get_schema(self.op.schema_name)
        return d.SetIsPrimaryReplica(schema_name=self.op.schema_name, is_primary=sim_schema.is_primary_replica)

    def is_up(self) -> bool:
        # circular import
        from liti.core.backend.bigquery import BigQueryDbBackend

        if isinstance(self.db_backend, BigQueryDbBackend):
            # the big query API does not say if it is the primary replica, always apply
            return False
        else:
            return self.db_backend.get_schema(self.op.schema_name).is_primary_replica is self.op.is_primary


class SetPrimaryReplicaOps(OperationOps):
    op: d.SetPrimaryReplica

    def __init__(self, op: d.SetPrimaryReplica, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.db_backend.set_primary_replica(self.op.schema_name, self.op.replica)

    def down(self) -> d.SetPrimaryReplica:
        sim_db = self.simulate(self.meta_backend.get_previous_operations())
        sim_schema = sim_db.get_schema(self.op.schema_name)
        return d.SetPrimaryReplica(schema_name=self.op.schema_name, replica=sim_schema.primary_replica)

    def is_up(self) -> bool:
        # circular import
        from liti.core.backend.bigquery import BigQueryDbBackend

        if isinstance(self.db_backend, BigQueryDbBackend):
            # the big query API does not provide the primary replica, always apply
            return False
        else:
            return self.db_backend.get_schema(self.op.schema_name).primary_replica == self.op.replica


class SetMaxTimeTravelOps(OperationOps):
    op: d.SetMaxTimeTravel

    def __init__(self, op: d.SetMaxTimeTravel, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.db_backend.set_max_time_travel(self.op.schema_name, self.op.duration)

    def down(self) -> d.SetMaxTimeTravel:
        sim_db = self.simulate(self.meta_backend.get_previous_operations())
        sim_schema = sim_db.get_schema(self.op.schema_name)
        return d.SetMaxTimeTravel(schema_name=self.op.schema_name, duration=sim_schema.max_time_travel)

    def is_up(self) -> bool:
        return self.db_backend.get_schema(self.op.schema_name).max_time_travel == self.op.duration


class SetStorageBillingOps(OperationOps):
    op: d.SetStorageBilling

    def __init__(self, op: d.SetStorageBilling, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.db_backend.set_storage_billing(self.op.schema_name, self.op.storage_billing)

    def down(self) -> d.SetStorageBilling:
        sim_db = self.simulate(self.meta_backend.get_previous_operations())
        sim_schema = sim_db.get_schema(self.op.schema_name)
        return d.SetStorageBilling(schema_name=self.op.schema_name, storage_billing=sim_schema.storage_billing)

    def is_up(self) -> bool:
        return self.db_backend.get_schema(self.op.schema_name).storage_billing == self.op.storage_billing


class CreateTableOps(OperationOps):
    op: d.CreateTable

    def __init__(self, op: d.CreateTable, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.db_backend.create_table(self.op.table)

    def down(self) -> d.DropTable:
        return d.DropTable(table_name=self.op.table.name)

    def is_up(self) -> bool:
        return self.db_backend.has_table(self.op.table.name)


class DropTableOps(OperationOps):
    op: d.DropTable

    def __init__(self, op: d.DropTable, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.db_backend.drop_table(self.op.table_name)

    def down(self) -> d.CreateTable:
        sim_db = self.simulate(self.meta_backend.get_previous_operations())
        sim_table = sim_db.get_table(self.op.table_name)
        return d.CreateTable(table=sim_table)

    def is_up(self) -> bool:
        return not self.db_backend.has_table(self.op.table_name)


class RenameTableOps(OperationOps):
    op: d.RenameTable

    def __init__(self, op: d.RenameTable, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.db_backend.rename_table(self.op.from_name, self.op.to_name)

    def down(self) -> d.RenameTable:
        return d.RenameTable(
            from_name=self.op.from_name.with_name(self.op.to_name),
            to_name=self.op.from_name.name,
        )

    def is_up(self) -> bool:
        return self.db_backend.has_table(self.op.from_name.with_name(self.op.to_name))


class SetPrimaryKeyOps(OperationOps):
    op: d.SetPrimaryKey

    def __init__(self, op: d.SetPrimaryKey, context: Context):
        self.op = op
        self.context = context

    def up(self):
        # TODO: implement removal of the existing primary key if any
        self.db_backend.set_primary_key(self.op.table_name, self.op.primary_key)

    def down(self) -> d.SetPrimaryKey:
        sim_db = self.simulate(self.meta_backend.get_previous_operations())
        sim_table = sim_db.get_table(self.op.table_name)
        return d.SetPrimaryKey(table_name=self.op.table_name, primary_key=sim_table.primary_key)

    def is_up(self) -> bool:
        return self.db_backend.get_table(self.op.table_name).primary_key == self.op.primary_key


class AddForeignKeyOps(OperationOps):
    op: d.AddForeignKey

    def __init__(self, op: d.AddForeignKey, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.db_backend.add_foreign_key(self.op.table_name, self.op.foreign_key)

    def down(self) -> d.DropConstraint:
        return d.DropConstraint(table_name=self.op.table_name, constraint_name=self.op.foreign_key.name)

    def is_up(self) -> bool:
        fk = self.op.foreign_key
        return self.db_backend.get_table(self.op.table_name).foreign_key_map.get(fk.name) == fk


class DropConstraintOps(OperationOps):
    op: d.DropConstraint

    def __init__(self, op: d.DropConstraint, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.db_backend.drop_constraint(self.op.table_name, self.op.constraint_name)

    def down(self) -> d.AddForeignKey:
        sim_db = self.simulate(self.meta_backend.get_previous_operations())
        sim_table = sim_db.get_table(self.op.table_name)

        return d.AddForeignKey(
            table_name=self.op.table_name,
            foreign_key=sim_table.foreign_key_map[self.op.constraint_name],
        )

    def is_up(self) -> bool:
        return self.op.constraint_name not in self.db_backend.get_table(self.op.table_name).foreign_key_map


class SetPartitionExpirationOps(OperationOps):
    op: d.SetPartitionExpiration

    def __init__(self, op: d.SetPartitionExpiration, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.db_backend.set_partition_expiration(self.op.table_name, self.op.expiration)

    def down(self) -> d.SetPartitionExpiration:
        sim_db = self.simulate(self.meta_backend.get_previous_operations())
        sim_table = sim_db.get_table(self.op.table_name)

        return d.SetPartitionExpiration(
            table_name=self.op.table_name,
            expiration=sim_table.partitioning and sim_table.partitioning.expiration,
        )

    def is_up(self) -> bool:
        partitioning = self.db_backend.get_table(self.op.table_name).partitioning
        return (partitioning and partitioning.expiration) == self.op.expiration


class SetRequirePartitionFilterOps(OperationOps):
    op: d.SetRequirePartitionFilter

    def __init__(self, op: d.SetRequirePartitionFilter, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.db_backend.set_require_partition_filter(self.op.table_name, self.op.require_filter)

    def down(self) -> d.SetRequirePartitionFilter:
        sim_db = self.simulate(self.meta_backend.get_previous_operations())
        sim_table = sim_db.get_table(self.op.table_name)

        return d.SetRequirePartitionFilter(
            table_name=self.op.table_name,
            require_filter=sim_table.partitioning and sim_table.partitioning.require_filter,
        )

    def is_up(self) -> bool:
        partitioning = self.db_backend.get_table(self.op.table_name).partitioning
        return (partitioning and partitioning.require_filter) == self.op.require_filter


class SetClusteringOps(OperationOps):
    op: d.SetClustering

    def __init__(self, op: d.SetClustering, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.db_backend.set_clustering(self.op.table_name, self.op.column_names)

    def down(self) -> d.SetClustering:
        sim_db = self.simulate(self.meta_backend.get_previous_operations())
        sim_table = sim_db.get_table(self.op.table_name)
        return d.SetClustering(table_name=self.op.table_name, column_names=sim_table.clustering)

    def is_up(self) -> bool:
        return self.db_backend.get_table(self.op.table_name).clustering == self.op.column_names


class SetFriendlyNameOps(OperationOps):
    op: d.SetFriendlyName

    def __init__(self, op: d.SetFriendlyName, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.db_backend.set_friendly_name(self.op.entity_name, self.op.friendly_name)

    def down(self) -> d.SetFriendlyName:
        sim_db = self.simulate(self.meta_backend.get_previous_operations())
        sim_entity = self.get_entity(self.op.entity_name, sim_db)

        if sim_entity is not None:
            return d.SetFriendlyName(entity_name=self.op.entity_name, friendly_name=sim_entity.friendly_name)
        else:
            raise ValueError(f'Entity {self.op.entity_name} not found in simulated database')

    def is_up(self) -> bool:
        entity = self.get_entity(self.op.entity_name)
        return entity is not None and entity.friendly_name == self.op.friendly_name


class SetDescriptionOps(OperationOps):
    op: d.SetDescription

    def __init__(self, op: d.SetDescription, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.db_backend.set_description(self.op.entity_name, self.op.description)

    def down(self) -> d.SetDescription:
        sim_db = self.simulate(self.meta_backend.get_previous_operations())
        sim_entity = self.get_entity(self.op.entity_name, sim_db)

        if sim_entity is not None:
            return d.SetDescription(entity_name=self.op.entity_name, description=sim_entity.description)
        else:
            raise ValueError(f'Entity {self.op.entity_name} not found in simulated database')

    def is_up(self) -> bool:
        entity = self.get_entity(self.op.entity_name)
        return entity is not None and entity.description == self.op.description


class SetLabelsOps(OperationOps):
    op: d.SetLabels

    def __init__(self, op: d.SetLabels, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.db_backend.set_labels(self.op.entity_name, self.op.labels)

    def down(self) -> d.SetLabels:
        sim_db = self.simulate(self.meta_backend.get_previous_operations())
        sim_entity = self.get_entity(self.op.entity_name, sim_db)

        if sim_entity is not None:
            return d.SetLabels(entity_name=self.op.entity_name, labels=sim_entity.labels)
        else:
            raise ValueError(f'Entity {self.op.entity_name} not found in simulated database')

    def is_up(self) -> bool:
        entity = self.get_entity(self.op.entity_name)
        return entity is not None and entity.labels == self.op.labels


class SetTagsOps(OperationOps):
    op: d.SetTags

    def __init__(self, op: d.SetTags, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.db_backend.set_tags(self.op.entity_name, self.op.tags)

    def down(self) -> d.SetTags:
        sim_db = self.simulate(self.meta_backend.get_previous_operations())
        sim_entity = self.get_entity(self.op.entity_name, sim_db)

        if sim_entity is not None:
            return d.SetTags(entity_name=self.op.entity_name, tags=sim_entity.tags)
        else:
            raise ValueError(f'Entity {self.op.entity_name} not found in simulated database')

    def is_up(self) -> bool:
        entity = self.get_entity(self.op.entity_name)
        return entity is not None and entity.tags == self.op.tags


class SetExpirationTimestampOps(OperationOps):
    op: d.SetExpirationTimestamp

    def __init__(self, op: d.SetExpirationTimestamp, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.db_backend.set_expiration_timestamp(self.op.entity_name, self.op.expiration_timestamp)

    def down(self) -> d.SetExpirationTimestamp:
        sim_db = self.simulate(self.meta_backend.get_previous_operations())
        sim_entity = self.get_entity(self.op.entity_name, sim_db)

        if sim_entity is not None:
            return d.SetExpirationTimestamp(
                entity_name=self.op.entity_name,
                expiration_timestamp=sim_entity.expiration_timestamp,
            )
        else:
            raise ValueError(f'Entity {self.op.entity_name} not found in simulated database')

    def is_up(self) -> bool:
        entity = self.get_entity(self.op.entity_name)
        return entity is not None and entity.expiration_timestamp == self.op.expiration_timestamp


class SetDefaultRoundingModeOps(OperationOps):
    op: d.SetDefaultRoundingMode

    def __init__(self, op: d.SetDefaultRoundingMode, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.db_backend.set_default_rounding_mode(self.op.entity_name, self.op.rounding_mode)

    def down(self) -> d.SetDefaultRoundingMode:
        sim_db = self.simulate(self.meta_backend.get_previous_operations())
        sim_entity = self.get_entity(self.op.entity_name, sim_db)

        if sim_entity is not None:
            return d.SetDefaultRoundingMode(
                entity_name=self.op.entity_name,
                rounding_mode=sim_entity.default_rounding_mode,
            )
        else:
            raise ValueError(f'Entity {self.op.entity_name} not found in simulated database')

    def is_up(self) -> bool:
        # circular import
        from liti.core.backend.bigquery import BigQueryDbBackend

        if isinstance(self.db_backend, BigQueryDbBackend):
            # the big query API does not provide the default rounding mode, always apply
            return False
        else:
            entity = self.get_entity(self.op.entity_name)
            return entity is not None and entity.default_rounding_mode == self.op.rounding_mode


class SetMaxStalenessOps(OperationOps):
    op: d.SetMaxStaleness

    def __init__(self, op: d.SetMaxStaleness, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.db_backend.set_max_staleness(self.op.entity_name, self.op.max_staleness)

    def down(self) -> d.SetMaxStaleness:
        sim_db = self.simulate(self.meta_backend.get_previous_operations())
        sim_entity = self.get_entity(self.op.entity_name, sim_db)

        if sim_entity is not None:
            return d.SetMaxStaleness(entity_name=self.op.entity_name, max_staleness=sim_entity.max_staleness)
        else:
            raise ValueError(f'Entity {self.op.entity_name} not found in simulated database')

    def is_up(self) -> bool:
        # circular import
        from liti.core.backend.bigquery import BigQueryDbBackend

        if isinstance(self.db_backend, BigQueryDbBackend):
            # the big query API provides max staleness in a custom string format that needs to be parsed, always apply
            # TODO: parse the big query value and use it here
            return False
        else:
            entity = self.get_entity(self.op.entity_name)
            return entity is not None and entity.max_staleness == self.op.max_staleness


class SetEnableChangeHistoryOps(OperationOps):
    op: d.SetEnableChangeHistory

    def __init__(self, op: d.SetEnableChangeHistory, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.db_backend.set_enable_change_history(self.op.table_name, self.op.enabled)

    def down(self) -> d.SetEnableChangeHistory:
        sim_db = self.simulate(self.meta_backend.get_previous_operations())
        sim_table = sim_db.get_table(self.op.table_name)
        return d.SetEnableChangeHistory(table_name=self.op.table_name, enabled=sim_table.enable_change_history)

    def is_up(self) -> bool:
        # circular import
        from liti.core.backend.bigquery import BigQueryDbBackend

        if isinstance(self.db_backend, BigQueryDbBackend):
            # the big query API does not provide the change history flag, always apply
            return False
        else:
            return self.db_backend.get_table(self.op.table_name).enable_change_history == self.op.enabled


class SetEnableFineGrainedMutationsOps(OperationOps):
    op: d.SetEnableFineGrainedMutations

    def __init__(self, op: d.SetEnableFineGrainedMutations, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.db_backend.set_enable_fine_grained_mutations(self.op.table_name, self.op.enabled)

    def down(self) -> d.SetEnableFineGrainedMutations:
        sim_db = self.simulate(self.meta_backend.get_previous_operations())
        sim_table = sim_db.get_table(self.op.table_name)

        return d.SetEnableFineGrainedMutations(
            table_name=self.op.table_name,
            enabled=sim_table.enable_fine_grained_mutations,
        )

    def is_up(self) -> bool:
        # circular import
        from liti.core.backend.bigquery import BigQueryDbBackend

        if isinstance(self.db_backend, BigQueryDbBackend):
            # the big query API does not provide the fine grained mutations flag, always apply
            return False
        else:
            return self.db_backend.get_table(self.op.table_name).enable_fine_grained_mutations == self.op.enabled


class SetKmsKeyNameOps(OperationOps):
    op: d.SetKmsKeyName

    def __init__(self, op: d.SetKmsKeyName, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.db_backend.set_kms_key_name(self.op.table_name, self.op.key_name)

    def down(self) -> d.SetKmsKeyName:
        sim_db = self.simulate(self.meta_backend.get_previous_operations())
        sim_table = sim_db.get_table(self.op.table_name)
        return d.SetKmsKeyName(table_name=self.op.table_name, key_name=sim_table.kms_key_name)

    def is_up(self) -> bool:
        # circular import
        from liti.core.backend.bigquery import BigQueryDbBackend

        if isinstance(self.db_backend, BigQueryDbBackend):
            # the big query API does not provide the kms key name, always apply
            return False
        else:
            return self.db_backend.get_table(self.op.table_name).kms_key_name == self.op.key_name
