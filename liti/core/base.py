from typing import Any, Callable, ClassVar, Self, TYPE_CHECKING

from pydantic import BaseModel

# avoid circular import errors by delaying the import of model types
# which need to use types from this file
if TYPE_CHECKING:
    from liti.core.model.v1.data_type import Array, BigNumeric, Float, Int, Numeric
    from liti.core.model.v1.schema import Partitioning


class Defaulter:
    """ Observer interface for backends to implement to define defaults

    Default methods update None values to their defaults.
    """

    def defaults_noop(self, node: Any):
        pass

    def int_defaults(self, node: 'Int'):
        pass

    def float_defaults(self, node: 'Float'):
        pass

    def numeric_defaults(self, node: 'Numeric'):
        pass

    def big_numeric_defaults(self, node: 'BigNumeric'):
        pass


class Defaultable:
    """ Observable interface for the model to implement """

    DEFAULT_METHOD: ClassVar[str] = 'defaults_noop'

    def set_defaults(self, defaulter: Defaulter):
        """ Updates the object with defaults applied

        This method should call set_defaults on the object's children.
        """

        getattr(defaulter, self.__class__.DEFAULT_METHOD)(self)


class Validator:
    """ Observer interface for backends to implement to validate the model

    Validation methods fix invalid values and raise if still invalid.
    """

    def noop_validate(self, node: Any):
        pass

    def validate_int(self, node: 'Int'):
        pass

    def validate_float(self, node: 'Float'):
        pass

    def validate_numeric(self, node: 'Numeric'):
        pass

    def validate_big_numeric(self, node: 'BigNumeric'):
        pass

    def validate_array(self, node: 'Array'):
        pass

    def validate_partitioning(self, node: 'Partitioning'):
        pass


class Validatable:
    """ Observable interface for the model to implement """

    VALIDATE_METHOD: ClassVar[str] = 'noop_validate'

    def liti_validate(self, validator: Validator):
        """ Raises if not valid

        This method should call liti_validate on the object's children.
        """

        getattr(validator, self.__class__.VALIDATE_METHOD)(self)


class LitiModel(BaseModel, Defaultable, Validatable):
    """ Base class for all Liti model classes """

    def set_defaults(self, defaulter: Defaulter):
        for field_name in self.__pydantic_fields__.keys():
            field = getattr(self, field_name)

            if isinstance(field, Defaultable):
                field.set_defaults(defaulter)

        super().set_defaults(defaulter)

    def liti_validate(self, validator: Validator):
        for field_name in self.__pydantic_fields__.keys():
            field = getattr(self, field_name)

            if isinstance(field, Validatable):
                field.liti_validate(validator)

        super().liti_validate(validator)
