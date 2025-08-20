from math import isclose
from typing import Any, Callable, ClassVar, Generator, TYPE_CHECKING

from pydantic import BaseModel

from liti.core.context import Context

# avoid circular import errors by delaying the import of model types
# which need to use types from this file
if TYPE_CHECKING:
    from liti.core.model.v1.datatype import Array, BigNumeric, Float, Int, Numeric
    from liti.core.model.v1.schema import MaterializedView, Partitioning, Table, View


class Defaulter:
    """ Observer interface for backends to implement to define defaults

    Default methods update None values to their defaults.
    """

    def defaults_noop(self, node: Any, context: Context):
        pass

    def int_defaults(self, node: 'Int', context: Context):
        pass

    def float_defaults(self, node: 'Float', context: Context):
        pass

    def numeric_defaults(self, node: 'Numeric', context: Context):
        pass

    def big_numeric_defaults(self, node: 'BigNumeric', context: Context):
        pass

    def partitioning_defaults(self, node: 'Partitioning', context: Context):
        pass

    def table_defaults(self, node: 'Table', context: Context):
        pass

    def view_defaults(self, node: 'View', context: Context):
        if node.select_sql is None and node.select_file is not None:
            with open(context.target_dir / node.select_file) as f:
                node.select_sql = f.read()

    def materialized_view_defaults(self, node: 'MaterializedView', context: Context):
        if node.select_sql is None and node.select_file is not None:
            with open(context.target_dir / node.select_file) as f:
                node.select_sql = f.read()


class Defaultable:
    """ Observable interface for the model to implement """

    DEFAULT_METHOD: ClassVar[str] = 'defaults_noop'

    def set_defaults(self, defaulter: Defaulter, context: Context):
        """ Updates the object with defaults applied

        This method should call set_defaults on the object's children.
        """

        getattr(defaulter, self.__class__.DEFAULT_METHOD)(self, context)


class Validator:
    """ Observer interface for backends to implement to validate the model

    Validation methods fix invalid values and raise if still invalid.
    """

    def noop_validate(self, node: Any, context: Context):
        pass

    def validate_int(self, node: 'Int', context: Context):
        pass

    def validate_float(self, node: 'Float', context: Context):
        pass

    def validate_numeric(self, node: 'Numeric', context: Context):
        pass

    def validate_big_numeric(self, node: 'BigNumeric', context: Context):
        pass

    def validate_array(self, node: 'Array', context: Context):
        pass

    def validate_partitioning(self, node: 'Partitioning', context: Context):
        pass

    def validate_view(self, node: 'View', context: Context):
        if not node.select_sql:
            raise ValueError(f'View {node.name} has no select SQL')

    def validate_materialized_view(self, node: 'MaterializedView', context: Context):
        if not node.select_sql:
            raise ValueError(f'Materialized view {node.name} has no select SQL')


class Validatable:
    """ Observable interface for the model to implement """

    VALIDATE_METHOD: ClassVar[str] = 'noop_validate'

    def liti_validate(self, validator: Validator, context: Context):
        """ Raises if not valid

        This method should call liti_validate on the object's children.
        """

        getattr(validator, self.__class__.VALIDATE_METHOD)(self, context)


def is_match(match: Any, value: Any) -> bool:
    # circular import
    from liti.core.model.v1.schema import ValidatedString

    if isinstance(match, dict) and isinstance(value, list | tuple | set):
        # skip checking collection items
        return True
    elif isinstance(value, LitiModel):
        # dig deeper into the model
        return all(is_match(inner, getattr(value, field)) for field, inner in match.items())
    elif isinstance(match, float) and isinstance(value, float):
        return isclose(match, value)
    elif isinstance(value, ValidatedString):
        # avoids having to specify '.string' in templates
        return match == value.string
    else:
        # match must be on the left hand side for STAR comparisons
        return match == value


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


class LitiModel(BaseModel, Defaultable, Validatable):
    """ Base class for all Liti model classes """

    @classmethod
    def by_name(cls, name: str) -> type['LitiModel']:
        # ensure LitiModel subclasses are imported first
        # noinspection PyUnresolvedReferences
        import liti.core.model.v1.subclasses

        return {
            subclass.__name__: subclass
            for subclass in LitiModel.__subclasses__()
        }[name]

    def set_defaults(self, defaulter: Defaulter, context: Context):
        for field_name in self.__pydantic_fields__.keys():
            field = getattr(self, field_name)

            if isinstance(field, Defaultable):
                field.set_defaults(defaulter, context)

        super().set_defaults(defaulter, context)

    def liti_validate(self, validator: Validator, context: Context):
        for field_name in self.__pydantic_fields__.keys():
            field = getattr(self, field_name)

            if isinstance(field, Validatable):
                field.liti_validate(validator, context)

        super().liti_validate(validator, context)

    def get_roots(self, root: type['LitiModel'], full_match: Any) -> Generator[tuple['LitiModel', Any], None, None]:
        """ Yields all the root nodes of the given type that match the provided `full_match`

        Also yields the remaining local match portion associated with each root in case the template path traverses
        through collection nodes. Those local matches can be used to check each item in the collections.
        """

        # this can be a bit duplicative since each call to `is_match` is recursive,
        # but performance here is not a concern, this is the easiest way to implement
        # the matching logic
        if not is_match(full_match, self):
            return

        if isinstance(self, root):
            yield self, full_match
        else:
            for field_name in self.__pydantic_fields__.keys():
                field = getattr(self, field_name)

                if isinstance(field, list | tuple | set):
                    for item in field:
                        if isinstance(item, LitiModel):
                            yield from item.get_roots(root, full_match[field_name])
                elif isinstance(field, LitiModel):
                    yield from field.get_roots(root, full_match[field_name])

    def get_update_fns(self, path: list[str], matches: list[Any]) -> Generator[Callable[[Any], None], None, None]:
        """ Yields functions to replace selected fields with a provided value

        :param path: a list of field names to recursively traverse through to find the fields to update
        :param matches: a list of either a dict structure of values to compare to the respective fields (functions are
            yielded on equivalence of all fields), or Star to always yield a function, all items in the list must match
        """

        # circular import
        from liti.core.model.v1.schema import ValidatedString

        if not path:
            return

        field_name, *tail = path

        if not hasattr(self, field_name):
            return

        field = getattr(self, field_name)
        field_matches = [m.get(field_name, STAR) for m in matches]

        # stop if any sibling fields do not match
        if not all(
            hasattr(self, f) and is_match(inner, getattr(self, f))
            for m in matches
            for f, inner in m.items()
            if f != field_name
        ):
            return

        if tail:
            # if there are more segments, dig deeper into the model
            if isinstance(field, tuple | list | set):
                # yield for each item that matches in the collection
                for item in field:
                    if isinstance(item, LitiModel):
                        yield from item.get_update_fns(tail, field_matches)
            elif isinstance(field, LitiModel):
                yield from field.get_update_fns(tail, field_matches)
        # yield the leaf field if it matches
        elif all(is_match(fm, field) for fm in field_matches):
            field_type = self.__pydantic_fields__[field_name].annotation

            if issubclass(field_type, ValidatedString):
                # avoids having to specify '.string' in templates
                yield lambda value: setattr(self, field_name, field_type(value))
            else:
                yield lambda value: setattr(self, field_name, value)
