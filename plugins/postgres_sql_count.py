import logging

from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook

log = logging.getLogger(__name__)


class PostgresSQLCountRows(BaseOperator):

    @apply_defaults
    def __init__(self, db_schema, table_name, *args, **kwargs):
        """
        :param table_name: table name
        """
        self.table_name = table_name
        self.hook = PostgresHook(schema=db_schema)
        super(PostgresSQLCountRows, self).__init__(*args, **kwargs)

    def execute(self, context):

        result = self.hook.get_first(
            sql="select count(*) from {};".format(self.table_name))
        log.info("Result: {}".format(result))
        return result


class PostgresSQLCustomOperatorsPlugin(AirflowPlugin):
    name = "postgres_custom"
    operators = [PostgresSQLCountRows]

    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []

