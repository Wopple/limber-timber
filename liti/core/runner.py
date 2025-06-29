from liti.core.backend.base import DbBackend, MetaBackend


class MigrationsRunner:
    def __init__(self, db_backend: DbBackend, meta_backend: MetaBackend):
        self.db_backend = db_backend
        self.meta_backend = meta_backend

    def run(
        self,
        target_dir: str,
        dry_run: bool = True,
        allow_down: bool = False,
    ):
        pass  # TODO
