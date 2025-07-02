from abc import ABC, abstractmethod

from liti.core.backend.base import DbBackend, MetaBackend


class OperationOps(ABC):
    @abstractmethod
    def up(self, db_backend: DbBackend):
        """ Apply the operation """
        pass

    @abstractmethod
    def down(self, db_backend: DbBackend, meta_backend: MetaBackend):
        """ Unapply the operation """
        pass

    @abstractmethod
    def is_up(self, db_backend: DbBackend) -> bool:
        """ True if the operation is applied

        Assumes that, if applied, this is the most recent operation (otherwise the behavior is not defined).
        Can return True even if the migrations metadata is not up to date.
        Useful for recovering from failures that left the migrations in an inconsistent state.
        """
        pass
