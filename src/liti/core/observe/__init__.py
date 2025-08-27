from pydantic import BaseModel

from liti.core.context import Context
from liti.core.model.v1.datatype import Array, BigNumeric, Float, Int, Numeric
from liti.core.model.v1.schema import MaterializedView, Partitioning, Schema, Table, View
from liti.core.observe.observer import Defaulter, Validator


def set_defaults(model: BaseModel, defaulter: Defaulter, context: Context):
    for field_name in model.__pydantic_fields__.keys():
        field = getattr(model, field_name)

        if isinstance(field, BaseModel):
            set_defaults(field, defaulter, context)

    if isinstance(model, Int):
        defaulter.int_defaults(model, context)
    elif isinstance(model, Float):
        defaulter.float_defaults(model, context)
    elif isinstance(model, Numeric):
        defaulter.numeric_defaults(model, context)
    elif isinstance(model, BigNumeric):
        defaulter.big_numeric_defaults(model, context)
    elif isinstance(model, Partitioning):
        defaulter.partitioning_defaults(model, context)
    elif isinstance(model, Table):
        defaulter.table_defaults(model, context)
    elif isinstance(model, View):
        defaulter.view_defaults(model, context)
    elif isinstance(model, MaterializedView):
        defaulter.materialized_view_defaults(model, context)


def validate_model(model: BaseModel, validator: Validator, context: Context):
    for field_name in model.__pydantic_fields__.keys():
        field = getattr(model, field_name)

        if isinstance(field, BaseModel):
            validate_model(field, validator, context)

    if isinstance(model, Schema):
        validator.validate_schema(model, context)
    elif isinstance(model, Int):
        validator.validate_int(model, context)
    elif isinstance(model, Float):
        validator.validate_float(model, context)
    elif isinstance(model, Numeric):
        validator.validate_numeric(model, context)
    elif isinstance(model, BigNumeric):
        validator.validate_big_numeric(model, context)
    elif isinstance(model, Array):
        validator.validate_array(model, context)
    elif isinstance(model, Partitioning):
        validator.validate_partitioning(model, context)
    elif isinstance(model, View):
        validator.validate_view(model, context)
    elif isinstance(model, MaterializedView):
        validator.validate_materialized_view(model, context)
