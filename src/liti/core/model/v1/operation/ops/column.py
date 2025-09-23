from liti.core.context import Context
from liti.core.function import extract_nested_datatype
from liti.core.model.v1.operation.data.column import AddColumn, AddColumnField, DropColumn, DropColumnField, \
    RenameColumn, SetColumnDatatype, SetColumnDescription, SetColumnNullable, SetColumnRoundingMode
from liti.core.model.v1.operation.ops.base import OperationOps


class AddColumnOps(OperationOps):
    op: AddColumn

    def __init__(self, op: AddColumn, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.db_backend.add_column(self.op.table_name, self.op.column)

    def down(self) -> DropColumn:
        return DropColumn(table_name=self.op.table_name, column_name=self.op.column.name)

    def is_up(self) -> bool:
        return self.op.column.name in self.db_backend.get_table(self.op.table_name).column_map


class DropColumnOps(OperationOps):
    op: DropColumn

    def __init__(self, op: DropColumn, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.db_backend.drop_column(self.op.table_name, self.op.column_name)

    def down(self) -> AddColumn:
        sim_db = self.simulate(self.meta_backend.get_previous_operations())
        sim_column = sim_db.get_table(self.op.table_name).column_map[self.op.column_name]
        return AddColumn(table_name=self.op.table_name, column=sim_column)

    def is_up(self) -> bool:
        return self.op.column_name not in self.db_backend.get_table(self.op.table_name).column_map


class RenameColumnOps(OperationOps):
    op: RenameColumn

    def __init__(self, op: RenameColumn, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.db_backend.rename_column(self.op.table_name, self.op.from_name, self.op.to_name)

    def down(self) -> RenameColumn:
        return RenameColumn(
            table_name=self.op.table_name,
            from_name=self.op.to_name,
            to_name=self.op.from_name,
        )

    def is_up(self) -> bool:
        return self.op.to_name in self.db_backend.get_table(self.op.table_name).column_map


class SetColumnDatatypeOps(OperationOps):
    op: SetColumnDatatype

    def __init__(self, op: SetColumnDatatype, context: Context):
        self.op = op
        self.context = context

    def up(self):
        from_datatype = self.db_backend.get_table(self.op.table_name).column_map[self.op.column_name]

        self.db_backend.set_column_datatype(
            table_name=self.op.table_name,
            column_name=self.op.column_name,
            from_datatype=from_datatype,
            to_datatype=self.op.datatype,
        )

    def down(self) -> SetColumnDatatype:
        sim_db = self.simulate(self.meta_backend.get_previous_operations())
        sim_column = sim_db.get_table(self.op.table_name).column_map[self.op.column_name]

        return SetColumnDatatype(
            table_name=self.op.table_name,
            column_name=self.op.column_name,
            datatype=sim_column.datatype,
        )

    def is_up(self) -> bool:
        table = self.db_backend.get_table(self.op.table_name)
        return table.column_map[self.op.column_name].datatype == self.op.datatype


class AddColumnFieldOps(OperationOps):
    op: AddColumnField

    def __init__(self, op: AddColumnField, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.db_backend.add_column_field(
            table_name=self.op.table_name,
            field_path=self.op.field_path,
            datatype=self.op.datatype,
        )

    def down(self) -> DropColumnField:
        return DropColumnField(
            table_name=self.op.table_name,
            field_path=self.op.field_path,
        )

    def is_up(self) -> bool:
        table = self.db_backend.get_table(self.op.table_name)

        try:
            extract_nested_datatype(table, self.op.field_path)
            return True
        except ValueError:
            return False


class DropColumnFieldOps(OperationOps):
    op: DropColumnField

    def __init__(self, op: DropColumnField, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.db_backend.drop_column_field(
            table_name=self.op.table_name,
            field_path=self.op.field_path,
        )

    def down(self) -> AddColumnField:
        sim_db = self.simulate(self.meta_backend.get_previous_operations())
        sim_table = sim_db.get_table(self.op.table_name)
        sim_datatype = extract_nested_datatype(sim_table, self.op.field_path)

        return AddColumnField(
            table_name=self.op.table_name,
            field_path=self.op.field_path,
            datatype=sim_datatype,
        )

    def is_up(self) -> bool:
        table = self.db_backend.get_table(self.op.table_name)

        try:
            extract_nested_datatype(table, self.op.field_path)
            return False
        except ValueError:
            return True


class SetColumnNullableOps(OperationOps):
    op: SetColumnNullable

    def __init__(self, op: SetColumnNullable, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.db_backend.set_column_nullable(self.op.table_name, self.op.column_name, self.op.nullable)

    def down(self) -> SetColumnNullable:
        sim_db = self.simulate(self.meta_backend.get_previous_operations())
        sim_column = sim_db.get_table(self.op.table_name).column_map[self.op.column_name]

        return SetColumnNullable(
            table_name=self.op.table_name,
            column_name=self.op.column_name,
            nullable=sim_column.nullable,
        )

    def is_up(self) -> bool:
        table = self.db_backend.get_table(self.op.table_name)
        return table.column_map[self.op.column_name].nullable == self.op.nullable


class SetColumnDescriptionOps(OperationOps):
    op: SetColumnDescription

    def __init__(self, op: SetColumnDescription, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.db_backend.set_column_description(self.op.table_name, self.op.column_name, self.op.description)

    def down(self) -> SetColumnDescription:
        sim_db = self.simulate(self.meta_backend.get_previous_operations())
        sim_column = sim_db.get_table(self.op.table_name).column_map[self.op.column_name]

        return SetColumnDescription(
            table_name=self.op.table_name,
            column_name=self.op.column_name,
            description=sim_column.description,
        )

    def is_up(self) -> bool:
        table = self.db_backend.get_table(self.op.table_name)
        return table.column_map[self.op.column_name].description == self.op.description


class SetColumnRoundingModeOps(OperationOps):
    op: SetColumnRoundingMode

    def __init__(self, op: SetColumnRoundingMode, context: Context):
        self.op = op
        self.context = context

    def up(self):
        self.db_backend.set_column_rounding_mode(self.op.table_name, self.op.column_name, self.op.rounding_mode)

    def down(self) -> SetColumnRoundingMode:
        sim_db = self.simulate(self.meta_backend.get_previous_operations())
        sim_column = sim_db.get_table(self.op.table_name).column_map[self.op.column_name]

        return SetColumnRoundingMode(
            table_name=self.op.table_name,
            column_name=self.op.column_name,
            rounding_mode=sim_column.rounding_mode,
        )

    def is_up(self) -> bool:
        table = self.db_backend.get_table(self.op.table_name)
        return table.column_map[self.op.column_name].rounding_mode == self.op.rounding_mode
