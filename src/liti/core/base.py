from math import isclose
from types import UnionType
from typing import Any, Callable, Generator, get_args, get_origin

from pydantic import BaseModel


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


class LitiModel(BaseModel):
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
            subclass = extract_subclass(field_type, ValidatedString)

            if subclass is not None:
                # avoids having to specify '.string' in templates when the field is a ValidatedString
                yield lambda value: setattr(self, field_name, subclass(value))
            else:
                yield lambda value: setattr(self, field_name, value)


def extract_subclass(ty: type, parent: type) -> type | None:
    """ Returns the first subclass of parent if any exist, supports direct subclasses and union types """

    if issubclass(ty, parent):
        return ty

    if get_origin(ty) is UnionType:
        for t in get_args(ty):
            if issubclass(t, parent):
                return t

    return None
