from abc import ABC, abstractmethod

from liti.core.backend.base import DbBackend, MetaBackend
from liti.core.model.v1.operation.data.base import Operation


class OperationOps(ABC):
    op: Operation

    @staticmethod
    def simulate(operations: list[Operation]) -> DbBackend:
        # circular imports
        from liti.core.backend.memory import MemoryDbBackend, MemoryMetaBackend
        from liti.core.runner import MigrateRunner

        sim_db = MemoryDbBackend()
        sim_meta = MemoryMetaBackend()
        sim_runner = MigrateRunner(db_backend=sim_db, meta_backend=sim_meta, target=operations)
        sim_runner.run(wet_run=True, silent=True)
        return sim_db

    @classmethod
    def get_attachment(cls, op: Operation) -> type:
        return {
            getattr(subclass, '__annotations__')['op']: subclass
            for subclass in OperationOps.__subclasses__()
        }[type(op)]

    @abstractmethod
    def up(self, db_backend: DbBackend):
        """ Apply the operation """
        pass

    @abstractmethod
    def down(self, db_backend: DbBackend, meta_backend: MetaBackend) -> Operation:
        """ Build the inverse operation """
        pass

    @abstractmethod
    def is_up(self, db_backend: DbBackend) -> bool:
        """ True if the operation is applied

        Assumes that, if applied, this is the most recent operation (otherwise the behavior is not defined).
        Can return True even if the migrations metadata is not up to date.
        Useful for recovering from failures that left the migrations in an inconsistent state.
        """
        pass
