from liti.core.context import Context
from liti.core.model.v1.operation.data.table import AddForeignKey, CreateTable, DropConstraint, DropTable, \
    RenameTable, SetClustering, SetDefaultRoundingMode, SetDescription, SetEnableChangeHistory, \
    SetEnableFineGrainedMutations, SetExpirationTimestamp, SetKmsKeyName, SetLabels, SetMaxStaleness, \
    SetPartitionExpiration, SetPrimaryKey, SetRequirePartitionFilter, SetTags
from liti.core.model.v1.operation.ops.base import OperationOps
from liti.core.model.v1.schema import Identifier


class CreateTableOps(OperationOps):
    op: CreateTable

    def __init__(self, op: CreateTable, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.context.db_backend.create_table(self.op.table)

    def down(self) -> DropTable:
        return DropTable(table_name=self.op.table.name)

    def is_up(self) -> bool:
        return self.context.db_backend.has_table(self.op.table.name)


class DropTableOps(OperationOps):
    op: DropTable

    def __init__(self, op: DropTable, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.context.db_backend.drop_table(self.op.table_name)

    def down(self) -> CreateTable:
        sim_db = self.simulate(self.context.meta_backend.get_previous_operations())
        sim_table = sim_db.get_table(self.op.table_name)
        return CreateTable(table=sim_table)

    def is_up(self) -> bool:
        return not self.context.db_backend.has_table(self.op.table_name)


class RenameTableOps(OperationOps):
    op: RenameTable

    def __init__(self, op: RenameTable, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.context.db_backend.rename_table(self.op.from_name, self.op.to_name)

    def down(self) -> RenameTable:
        return RenameTable(
            from_name=self.op.from_name.with_table_name(self.op.to_name),
            to_name=self.op.from_name.table_name,
        )

    def is_up(self) -> bool:
        return self.context.db_backend.has_table(self.op.from_name.with_table_name(self.op.to_name))


class SetPrimaryKeyOps(OperationOps):
    op: SetPrimaryKey

    def __init__(self, op: SetPrimaryKey, context: Context):
        self.op = op
        self.context = context

    def up(self):
        # TODO: implement removal of the existing primary key if any
        self.context.db_backend.set_primary_key(self.op.table_name, self.op.primary_key)

    def down(self) -> SetPrimaryKey:
        sim_db = self.simulate(self.context.meta_backend.get_previous_operations())
        sim_table = sim_db.get_table(self.op.table_name)
        return SetPrimaryKey(table_name=self.op.table_name, primary_key=sim_table.primary_key)

    def is_up(self) -> bool:
        return self.context.db_backend.get_table(self.op.table_name).primary_key == self.op.primary_key


class AddForeignKeyOps(OperationOps):
    op: AddForeignKey

    def __init__(self, op: AddForeignKey, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.context.db_backend.add_foreign_key(self.op.table_name, self.op.foreign_key)

    def down(self) -> DropConstraint:
        return DropConstraint(table_name=self.op.table_name, constraint_name=Identifier(self.op.foreign_key.name))

    def is_up(self) -> bool:
        fk = self.op.foreign_key
        return self.context.db_backend.get_table(self.op.table_name).foreign_key_map.get(fk.name) == fk


class DropConstraintOps(OperationOps):
    op: DropConstraint

    def __init__(self, op: DropConstraint, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.context.db_backend.drop_constraint(self.op.table_name, self.op.constraint_name)

    def down(self) -> AddForeignKey:
        sim_db = self.simulate(self.context.meta_backend.get_previous_operations())
        sim_table = sim_db.get_table(self.op.table_name)

        return AddForeignKey(
            table_name=self.op.table_name,
            foreign_key=sim_table.foreign_key_map[self.op.constraint_name],
        )

    def is_up(self) -> bool:
        return self.op.constraint_name not in self.context.db_backend.get_table(self.op.table_name).foreign_key_map


class SetPartitionExpirationOps(OperationOps):
    op: SetPartitionExpiration

    def __init__(self, op: SetPartitionExpiration, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.context.db_backend.set_partition_expiration(self.op.table_name, self.op.expiration_days)

    def down(self) -> SetPartitionExpiration:
        sim_db = self.simulate(self.context.meta_backend.get_previous_operations())
        sim_table = sim_db.get_table(self.op.table_name)

        return SetPartitionExpiration(
            table_name=self.op.table_name,
            expiration_days=sim_table.partitioning and sim_table.partitioning.expiration_days,
        )

    def is_up(self) -> bool:
        partitioning = self.context.db_backend.get_table(self.op.table_name).partitioning
        return (partitioning and partitioning.expiration_days) == self.op.expiration_days


class SetRequirePartitionFilterOps(OperationOps):
    op: SetRequirePartitionFilter

    def __init__(self, op: SetRequirePartitionFilter, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.context.db_backend.set_require_partition_filter(self.op.table_name, self.op.require_filter)

    def down(self) -> SetRequirePartitionFilter:
        sim_db = self.simulate(self.context.meta_backend.get_previous_operations())
        sim_table = sim_db.get_table(self.op.table_name)

        return SetRequirePartitionFilter(
            table_name=self.op.table_name,
            require_filter=sim_table.partitioning and sim_table.partitioning.require_filter,
        )

    def is_up(self) -> bool:
        partitioning = self.context.db_backend.get_table(self.op.table_name).partitioning
        return (partitioning and partitioning.require_filter) == self.op.require_filter


class SetClusteringOps(OperationOps):
    op: SetClustering

    def __init__(self, op: SetClustering, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.context.db_backend.set_clustering(self.op.table_name, self.op.column_names)

    def down(self) -> SetClustering:
        sim_db = self.simulate(self.context.meta_backend.get_previous_operations())
        sim_table = sim_db.get_table(self.op.table_name)
        return SetClustering(table_name=self.op.table_name, column_names=sim_table.clustering)

    def is_up(self) -> bool:
        return self.context.db_backend.get_table(self.op.table_name).clustering == self.op.column_names


class SetDescriptionOps(OperationOps):
    op: SetDescription

    def __init__(self, op: SetDescription, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.context.db_backend.set_description(self.op.table_name, self.op.description)

    def down(self) -> SetDescription:
        sim_db = self.simulate(self.context.meta_backend.get_previous_operations())
        sim_table = sim_db.get_table(self.op.table_name)
        return SetDescription(table_name=self.op.table_name, description=sim_table.description)

    def is_up(self) -> bool:
        return self.context.db_backend.get_table(self.op.table_name).description == self.op.description


class SetLabelsOps(OperationOps):
    op: SetLabels

    def __init__(self, op: SetLabels, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.context.db_backend.set_labels(self.op.table_name, self.op.labels)

    def down(self) -> SetLabels:
        sim_db = self.simulate(self.context.meta_backend.get_previous_operations())
        sim_table = sim_db.get_table(self.op.table_name)
        return SetLabels(table_name=self.op.table_name, labels=sim_table.labels)

    def is_up(self) -> bool:
        return self.context.db_backend.get_table(self.op.table_name).labels == self.op.labels


class SetTagsOps(OperationOps):
    op: SetTags

    def __init__(self, op: SetTags, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.context.db_backend.set_tags(self.op.table_name, self.op.tags)

    def down(self) -> SetTags:
        sim_db = self.simulate(self.context.meta_backend.get_previous_operations())
        sim_table = sim_db.get_table(self.op.table_name)
        return SetTags(table_name=self.op.table_name, tags=sim_table.tags)

    def is_up(self) -> bool:
        return self.context.db_backend.get_table(self.op.table_name).tags == self.op.tags


class SetExpirationTimestampOps(OperationOps):
    op: SetExpirationTimestamp

    def __init__(self, op: SetExpirationTimestamp, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.context.db_backend.set_expiration_timestamp(self.op.table_name, self.op.expiration_timestamp)

    def down(self) -> SetExpirationTimestamp:
        sim_db = self.simulate(self.context.meta_backend.get_previous_operations())
        sim_table = sim_db.get_table(self.op.table_name)

        return SetExpirationTimestamp(
            table_name=self.op.table_name,
            expiration_timestamp=sim_table.expiration_timestamp,
        )

    def is_up(self) -> bool:
        table = self.context.db_backend.get_table(self.op.table_name)
        return table.expiration_timestamp == self.op.expiration_timestamp


class SetDefaultRoundingModeOps(OperationOps):
    op: SetDefaultRoundingMode

    def __init__(self, op: SetDefaultRoundingMode, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.context.db_backend.set_default_rounding_mode(self.op.table_name, self.op.rounding_mode)

    def down(self) -> SetDefaultRoundingMode:
        sim_db = self.simulate(self.context.meta_backend.get_previous_operations())
        sim_table = sim_db.get_table(self.op.table_name)
        return SetDefaultRoundingMode(table_name=self.op.table_name, rounding_mode=sim_table.default_rounding_mode)

    def is_up(self) -> bool:
        return self.context.db_backend.get_table(self.op.table_name).default_rounding_mode == self.op.rounding_mode


class SetMaxStalenessOps(OperationOps):
    op: SetMaxStaleness

    def __init__(self, op: SetMaxStaleness, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.context.db_backend.set_max_staleness(self.op.table_name, self.op.max_staleness)

    def down(self) -> SetMaxStaleness:
        sim_db = self.simulate(self.context.meta_backend.get_previous_operations())
        sim_table = sim_db.get_table(self.op.table_name)
        return SetMaxStaleness(table_name=self.op.table_name, max_staleness=sim_table.max_staleness)

    def is_up(self) -> bool:
        return self.context.db_backend.get_table(self.op.table_name).max_staleness == self.op.max_staleness


class SetEnableChangeHistoryOps(OperationOps):
    op: SetEnableChangeHistory

    def __init__(self, op: SetEnableChangeHistory, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.context.db_backend.set_enable_change_history(self.op.table_name, self.op.enabled)

    def down(self) -> SetEnableChangeHistory:
        sim_db = self.simulate(self.context.meta_backend.get_previous_operations())
        sim_table = sim_db.get_table(self.op.table_name)
        return SetEnableChangeHistory(table_name=self.op.table_name, enabled=sim_table.enable_change_history)

    def is_up(self) -> bool:
        return self.context.db_backend.get_table(self.op.table_name).enable_change_history == self.op.enabled


class SetEnableFineGrainedMutationsOps(OperationOps):
    op: SetEnableFineGrainedMutations

    def __init__(self, op: SetEnableFineGrainedMutations, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.context.db_backend.set_enable_fine_grained_mutations(self.op.table_name, self.op.enabled)

    def down(self) -> SetEnableFineGrainedMutations:
        sim_db = self.simulate(self.context.meta_backend.get_previous_operations())
        sim_table = sim_db.get_table(self.op.table_name)

        return SetEnableFineGrainedMutations(
            table_name=self.op.table_name,
            enabled=sim_table.enable_fine_grained_mutations,
        )

    def is_up(self) -> bool:
        return self.context.db_backend.get_table(self.op.table_name).enable_fine_grained_mutations == self.op.enabled


class SetKmsKeyNameOps(OperationOps):
    op: SetKmsKeyName

    def __init__(self, op: SetKmsKeyName, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.context.db_backend.set_kms_key_name(self.op.table_name, self.op.key_name)

    def down(self) -> SetKmsKeyName:
        sim_db = self.simulate(self.context.meta_backend.get_previous_operations())
        sim_table = sim_db.get_table(self.op.table_name)
        return SetKmsKeyName(table_name=self.op.table_name, key_name=sim_table.kms_key_name)

    def is_up(self) -> bool:
        return self.context.db_backend.get_table(self.op.table_name).kms_key_name == self.op.key_name
