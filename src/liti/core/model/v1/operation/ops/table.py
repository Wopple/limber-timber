from pathlib import Path

from liti.core.backend.base import DbBackend, MetaBackend
from liti.core.model.v1.operation.data.table import AddForeignKey, CreateTable, DropConstraint, DropTable, RenameTable, \
    SetClustering, \
    SetDefaultRoundingMode, SetDescription, SetLabels, SetPrimaryKey, SetTags
from liti.core.model.v1.operation.ops.base import OperationOps


class CreateTableOps(OperationOps):
    op: CreateTable

    def __init__(self, op: CreateTable):
        self.op = op

    def up(self, db_backend: DbBackend, meta_backend: MetaBackend, target_dir: Path | None):
        db_backend.create_table(self.op.table)

    def down(self, db_backend: DbBackend, meta_backend: MetaBackend) -> DropTable:
        return DropTable(table_name=self.op.table.name)

    def is_up(self, db_backend: DbBackend, target_dir: Path | None) -> bool:
        return db_backend.has_table(self.op.table.name)


class DropTableOps(OperationOps):
    op: DropTable

    def __init__(self, op: DropTable):
        self.op = op

    def up(self, db_backend: DbBackend, meta_backend: MetaBackend, target_dir: Path | None):
        db_backend.drop_table(self.op.table_name)

    def down(self, db_backend: DbBackend, meta_backend: MetaBackend) -> CreateTable:
        sim_db = self.simulate(meta_backend.get_previous_operations())
        sim_table = sim_db.get_table(self.op.table_name)
        return CreateTable(table=sim_table)

    def is_up(self, db_backend: DbBackend, target_dir: Path | None) -> bool:
        return not db_backend.has_table(self.op.table_name)


class RenameTableOps(OperationOps):
    op: RenameTable

    def __init__(self, op: RenameTable):
        self.op = op

    def up(self, db_backend: DbBackend, meta_backend: MetaBackend, target_dir: Path | None):
        db_backend.rename_table(self.op.from_name, self.op.to_name)

    def down(self, db_backend: DbBackend, meta_backend: MetaBackend) -> RenameTable:
        return RenameTable(
            from_name=self.op.from_name.with_table_name(self.op.to_name),
            to_name=self.op.from_name.table_name,
        )

    def is_up(self, db_backend: DbBackend, target_dir: Path | None) -> bool:
        return db_backend.has_table(self.op.from_name.with_table_name(self.op.to_name))


class SetPrimaryKeyOps(OperationOps):
    op: SetPrimaryKey

    def __init__(self, op: SetPrimaryKey):
        self.op = op

    def up(self, db_backend: DbBackend, meta_backend: MetaBackend, target_dir: Path | None):
        # TODO: implement removal of the existing primary key if any
        db_backend.set_primary_key(self.op.table_name, self.op.primary_key)

    def down(self, db_backend: DbBackend, meta_backend: MetaBackend) -> SetPrimaryKey:
        sim_db = self.simulate(meta_backend.get_previous_operations())
        sim_table = sim_db.get_table(self.op.table_name)
        return SetPrimaryKey(table_name=self.op.table_name, primary_key=sim_table.primary_key)

    def is_up(self, db_backend: DbBackend, target_dir: Path | None) -> bool:
        return db_backend.get_table(self.op.table_name).primary_key == self.op.primary_key


class AddForeignKeyOps(OperationOps):
    op: AddForeignKey

    def __init__(self, op: AddForeignKey):
        self.op = op

    def up(self, db_backend: DbBackend, meta_backend: MetaBackend, target_dir: Path | None):
        db_backend.add_foreign_key(self.op.table_name, self.op.foreign_key)

    def down(self, db_backend: DbBackend, meta_backend: MetaBackend) -> DropConstraint:
        return DropConstraint(table_name=self.op.table_name, constraint_name=self.op.foreign_key.name)

    def is_up(self, db_backend: DbBackend, target_dir: Path | None) -> bool:
        fk = self.op.foreign_key
        return db_backend.get_table(self.op.table_name).foreign_key_map.get(fk.name) == fk


class DropConstraintOps(OperationOps):
    op: DropConstraint

    def __init__(self, op: DropConstraint):
        self.op = op

    def up(self, db_backend: DbBackend, meta_backend: MetaBackend, target_dir: Path | None):
        db_backend.drop_constraint(self.op.table_name, self.op.constraint_name)

    def down(self, db_backend: DbBackend, meta_backend: MetaBackend) -> AddForeignKey:
        sim_db = self.simulate(meta_backend.get_previous_operations())
        sim_table = sim_db.get_table(self.op.table_name)

        return AddForeignKey(
            table_name=self.op.table_name,
            foreign_key=sim_table.foreign_key_map[self.op.constraint_name],
        )

    def is_up(self, db_backend: DbBackend, target_dir: Path | None) -> bool:
        return self.op.constraint_name not in db_backend.get_table(self.op.table_name).foreign_key_map


class SetClusteringOps(OperationOps):
    op: SetClustering

    def __init__(self, op: SetClustering):
        self.op = op

    def up(self, db_backend: DbBackend, meta_backend: MetaBackend, target_dir: Path | None):
        db_backend.set_clustering(self.op.table_name, self.op.column_names)

    def down(self, db_backend: DbBackend, meta_backend: MetaBackend) -> SetClustering:
        sim_db = self.simulate(meta_backend.get_previous_operations())
        sim_table = sim_db.get_table(self.op.table_name)
        return SetClustering(table_name=self.op.table_name, column_names=sim_table.clustering)

    def is_up(self, db_backend: DbBackend, target_dir: Path | None) -> bool:
        return db_backend.get_table(self.op.table_name).clustering == self.op.column_names


class SetDescriptionOps(OperationOps):
    op: SetDescription

    def __init__(self, op: SetDescription):
        self.op = op

    def up(self, db_backend: DbBackend, meta_backend: MetaBackend, target_dir: Path | None):
        db_backend.set_description(self.op.table_name, self.op.description)

    def down(self, db_backend: DbBackend, meta_backend: MetaBackend) -> SetDescription:
        sim_db = self.simulate(meta_backend.get_previous_operations())
        sim_table = sim_db.get_table(self.op.table_name)
        return SetDescription(table_name=self.op.table_name, description=sim_table.description)

    def is_up(self, db_backend: DbBackend, target_dir: Path | None) -> bool:
        return db_backend.get_table(self.op.table_name).description == self.op.description


class SetLabelsOps(OperationOps):
    op: SetLabels

    def __init__(self, op: SetLabels):
        self.op = op

    def up(self, db_backend: DbBackend, meta_backend: MetaBackend, target_dir: Path | None):
        db_backend.set_labels(self.op.table_name, self.op.labels)

    def down(self, db_backend: DbBackend, meta_backend: MetaBackend) -> SetLabels:
        sim_db = self.simulate(meta_backend.get_previous_operations())
        sim_table = sim_db.get_table(self.op.table_name)
        return SetLabels(table_name=self.op.table_name, labels=sim_table.labels)

    def is_up(self, db_backend: DbBackend, target_dir: Path | None) -> bool:
        return db_backend.get_table(self.op.table_name).labels == self.op.labels


class SetTagsOps(OperationOps):
    op: SetTags

    def __init__(self, op: SetTags):
        self.op = op

    def up(self, db_backend: DbBackend, meta_backend: MetaBackend, target_dir: Path | None):
        db_backend.set_tags(self.op.table_name, self.op.tags)

    def down(self, db_backend: DbBackend, meta_backend: MetaBackend) -> SetTags:
        sim_db = self.simulate(meta_backend.get_previous_operations())
        sim_table = sim_db.get_table(self.op.table_name)
        return SetTags(table_name=self.op.table_name, tags=sim_table.tags)

    def is_up(self, db_backend: DbBackend, target_dir: Path | None) -> bool:
        return db_backend.get_table(self.op.table_name).tags == self.op.tags


class SetDefaultRoundingModeOps(OperationOps):
    op: SetDefaultRoundingMode

    def __init__(self, op: SetDefaultRoundingMode):
        self.op = op

    def up(self, db_backend: DbBackend, meta_backend: MetaBackend, target_dir: Path | None):
        db_backend.set_default_rounding_mode(self.op.table_name, self.op.rounding_mode)

    def down(self, db_backend: DbBackend, meta_backend: MetaBackend) -> SetDefaultRoundingMode:
        sim_db = self.simulate(meta_backend.get_previous_operations())
        sim_table = sim_db.get_table(self.op.table_name)
        return SetDefaultRoundingMode(table_name=self.op.table_name, rounding_mode=sim_table.default_rounding_mode)

    def is_up(self, db_backend: DbBackend, target_dir: Path | None) -> bool:
        return db_backend.get_table(self.op.table_name).default_rounding_mode == self.op.rounding_mode
