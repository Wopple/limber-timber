from pathlib import Path
from typing import Any, Optional, TYPE_CHECKING

from pydantic import BaseModel

if TYPE_CHECKING:
    from liti.core.model.v1.manifest import Manifest
    from liti.core.model.v1.operation.data.base import Operation
    from liti.core.model.v1.template import Template
    from liti.core.backend.base import DbBackend, MetaBackend


# I was unable to instantiate the Context in some cases when using the types normally, so
# I am using `Any` for the private type, and using setters and getters with the real type.
# Naming with a leading underscore was causing pydantic to treat them as class variables.
class Context(BaseModel):
    db_backend_: Any = None
    meta_backend_: Any = None
    target_dir: Path | None = None
    silent: bool = False
    manifest_: Any = None
    template_files: list[Path] | None = None
    templates_: Any | None
    target_operations_: Any = None

    def __init__(
        self,
        *,  # forcing named args since static analysis cannot catch mistakes due to usage of Any
        db_backend: Any = None,
        meta_backend: Any = None,
        target_dir: Path | None = None,
        silent: bool = False,
        manifest: Any = None,
        template_files: list[Path] | None = None,
        templates: Any = None,
        target_operations: Any = None,
    ):
        """ Allows instantiation with expected field names without trailing underscores """

        super().__init__(
            db_backend_=db_backend,
            meta_backend_=meta_backend,
            target_dir=target_dir,
            silent=silent,
            manifest_=manifest,
            template_files=template_files,
            templates_=templates,
            target_operations_=target_operations,
        )

    @property
    def db_backend(self) -> Optional['DbBackend']:
        return self.db_backend_

    @db_backend.setter
    def db_backend(self, value: Optional['DbBackend']):
        self.db_backend_ = value

    @property
    def meta_backend(self) -> Optional['MetaBackend']:
        return self.meta_backend_

    @meta_backend.setter
    def meta_backend(self, value: Optional['MetaBackend']):
        self.meta_backend_ = value

    @property
    def manifest(self) -> Optional['Manifest']:
        return self.manifest_

    @manifest.setter
    def manifest(self, value: Optional['Manifest']):
        self.manifest_ = value

    @property
    def templates(self) -> list['Template'] | None:
        return self.templates_

    @templates.setter
    def templates(self, value: list['Template'] | None):
        self.templates_ = value

    @property
    def target_operations(self) -> list['Operation'] | None:
        return self.target_operations_

    @target_operations.setter
    def target_operations(self, value: list['Operation'] | None):
        self.target_operations_ = value
