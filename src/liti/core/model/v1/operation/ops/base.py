from abc import ABC, abstractmethod
from pathlib import Path

from liti.core.backend.base import DbBackend, MetaBackend
from liti.core.context import Context
from liti.core.model.v1.operation.data.base import Operation
from liti.core.model.v1.schema import MaterializedView, QualifiedName, Schema, Table, View
from liti.core.reflect import recursive_subclasses


class OperationOps(ABC):
    op: Operation
    context: Context

    @property
    def db_backend(self) -> DbBackend:
        return self.context.db_backend

    @property
    def meta_backend(self) -> MetaBackend:
        return self.context.meta_backend

    @property
    def target_dir(self) -> Path | None:
        return self.context.target_dir

    @staticmethod
    def simulate(operations: list[Operation]) -> DbBackend:
        # circular imports
        from liti.core.backend.memory import MemoryDbBackend, MemoryMetaBackend
        from liti.core.runner import MigrateRunner

        sim_context = Context(
            db_backend=MemoryDbBackend(),
            meta_backend=MemoryMetaBackend(),
            target_operations=operations,
            silent=True,
        )

        MigrateRunner(context=sim_context).run(wet_run=True)
        return sim_context.db_backend

    @classmethod
    def get_attachment(cls, op: Operation) -> type['OperationOps']:
        # ensure OperationOps subclasses are imported first
        # noinspection PyUnresolvedReferences
        import liti.core.model.v1.operation.ops.subclasses

        return {
            getattr(subclass, '__annotations__')['op']: subclass
            for subclass in recursive_subclasses(OperationOps)
        }[type(op)]

    @abstractmethod
    def up(self):
        """ Apply the operation """
        pass

    @abstractmethod
    def down(self) -> Operation:
        """ Build the inverse operation """
        pass

    @abstractmethod
    def is_up(self) -> bool:
        """ True if the operation is applied

        If the operation is applied when `is_up` is called, it assumes this is the most recently applied operation.
        Otherwise, the behavior is undefined.
        Can return True even if the metadata is not up to date.
        Useful for recovering from failures that left the migrations in an inconsistent state.
        """
        pass

    def get_entity(
        self,
        name: QualifiedName,
        db_backend: DbBackend | None = None,
    ) -> Schema | Table | View | MaterializedView | None:
        """ Return the named entity among the supported entity kinds or None if it does not exist """

        db_backend = db_backend or self.db_backend
        entity = db_backend.get_entity(name)

        # only return the entity if its kind is supported by this operation
        for entity_kind in self.op.supported_entity_kinds:
            if entity_kind == 'SCHEMA' and isinstance(entity, Schema):
                return entity
            elif entity_kind == 'TABLE' and isinstance(entity, Table):
                return entity
            elif entity_kind == 'VIEW' and isinstance(entity, View):
                return entity
            elif entity_kind == 'MATERIALIZED_VIEW' and isinstance(entity, MaterializedView):
                return entity

        return None
