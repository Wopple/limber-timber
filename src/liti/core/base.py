from math import isclose
from typing import Any, Callable, ClassVar, Generator, TYPE_CHECKING

from pydantic import BaseModel

# avoid circular import errors by delaying the import of model types
# which need to use types from this file
if TYPE_CHECKING:
    from liti.core.model.v1.datatype import Array, BigNumeric, Float, Int, Numeric
    from liti.core.model.v1.schema import Partitioning, Table


class Star:
    """ Star is used to match everything """

    def __eq__(self, other):
        return True

    def __getitem__(self, item):
        return self

    def get(self, *args, **kwargs):
        return self

    def items(self):
        return iter(())


STAR = Star()


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

    def partitioning_defaults(self, node: 'Partitioning'):
        pass

    def table_defaults(self, node: 'Table'):
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


def is_match(match: Any, value: Any) -> bool:
    if isinstance(value, LitiModel):
        # dig deeper into the model
        return all(is_match(inner, getattr(value, field)) for field, inner in match.items())
    else:
        # check the leaf value
        if isinstance(match, float) and isinstance(value, float):
            return isclose(match, value)
        else:
            # match must be on the left hand side for STAR comparisons
            return match == value


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

    def get_update_fns(self, path: list[str], match: Any) -> Generator[Callable[[Any], None], None, None]:
        """ Yields functions to replace selected fields with a provided value

        :param path: a list of field names to recursively traverse through to find the fields to update
        :param match: either a dict structure of values to compare to the respective fields (functions are yielded on
                      equivalence of all fields), or Star to always yield a function
        """
        if not path:
            return

        head, *tail = path

        if not hasattr(self, head):
            return

        field = getattr(self, head)
        match_value = match.get(head, STAR)

        # stop if any sibling fields do not match
        if not all(
            hasattr(self, f) and is_match(inner, getattr(self, f))
            for f, inner in match.items()
            if f != head
        ):
            return

        if tail:
            # if there are more segments, dig deeper into the model
            if isinstance(field, tuple | list | set):
                # yield for each item that matches in the collection
                for item in field:
                    if isinstance(item, LitiModel):
                        yield from item.get_update_fns(tail, match_value)
            elif isinstance(field, LitiModel):
                yield from field.get_update_fns(tail, match_value)
        # yield the leaf field if it matches
        elif is_match(match_value, field):
            yield lambda value: setattr(self, head, value)
