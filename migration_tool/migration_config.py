from migration_tool.db_types import DBType
from migration_tool.settings import Settings


class MigrationConfig:
    def __init__(self, db_name: str, db_type: str, settings: Settings):
        self.db_type = DBType(db_type)
        self.db_name = db_name
        self.db_settings = settings
