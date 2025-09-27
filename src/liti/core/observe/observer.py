from liti.core.context import Context

from liti.core.model.v1.datatype import Array, BigNumeric, Bytes, Float, Int, Numeric, String
from liti.core.model.v1.schema import MaterializedView, Partitioning, Schema, Table, View


class Defaulter:
    """ Observer interface for backends to implement to define defaults

    Default methods update None values to their defaults.
    """

    def int_defaults(self, node: Int, context: Context):
        pass

    def float_defaults(self, node: Float, context: Context):
        pass

    def numeric_defaults(self, node: Numeric, context: Context):
        pass

    def big_numeric_defaults(self, node: BigNumeric, context: Context):
        pass

    def partitioning_defaults(self, node: Partitioning, context: Context):
        pass

    def table_defaults(self, node: Table, context: Context):
        pass

    def view_defaults(self, node: View, context: Context):
        if node.select_sql is None and node.select_file is not None:
            with open(context.target_dir / node.select_file) as f:
                node.select_sql = f.read()

    def materialized_view_defaults(self, node: MaterializedView, context: Context):
        if node.select_sql is None and node.select_file is not None:
            with open(context.target_dir / node.select_file) as f:
                node.select_sql = f.read()


class Validator:
    """ Observer interface for backends to implement to validate the model

    Validation methods fix invalid values and raise if still invalid.
    """

    def validate_schema(self, node: Schema, context: Context):
        pass

    def validate_int(self, node: Int, context: Context):
        pass

    def validate_float(self, node: Float, context: Context):
        pass

    def validate_numeric(self, node: Numeric, context: Context):
        pass

    def validate_big_numeric(self, node: BigNumeric, context: Context):
        pass

    def validate_string(self, node: String, context: Context):
        pass

    def validate_bytes(self, node: Bytes, context: Context):
        pass

    def validate_array(self, node: Array, context: Context):
        pass

    def validate_partitioning(self, node: Partitioning, context: Context):
        pass

    def validate_view(self, node: View, context: Context):
        if not node.select_sql:
            raise ValueError(f'View {node.name} has no select SQL')

    def validate_materialized_view(self, node: MaterializedView, context: Context):
        if not node.select_sql:
            raise ValueError(f'Materialized view {node.name} has no select SQL')
