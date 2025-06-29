from abc import ABC, abstractmethod
from typing import ClassVar

from pydantic.main import BaseModel

from liti.core.backend.base import DbBackend


class Operation(BaseModel, ABC):
    KIND: ClassVar[str]

    @abstractmethod
    def up(self, backend: DbBackend):
        """ Apply the operation """
        pass

    @abstractmethod
    def down(self, backend: DbBackend):
        """ Unapply the operation """
        pass

    @abstractmethod
    def is_up(self, backend: DbBackend) -> bool:
        """ True if the operation is applied

        Assumes that, if applied, this is the most recent operation (otherwise the behavior is not defined).
        Can return True even if the migrations metadata is not up to date.
        Useful for recovering from failures that left the migrations in an inconsistent state.
        """
        pass
