from typing import Generator, TypeVar

T = TypeVar('T')

def recursive_subclasses(ty: type[T]) -> Generator[type[T], None, None]:
    for subclass in ty.__subclasses__():
        yield subclass
        yield from recursive_subclasses(subclass)
