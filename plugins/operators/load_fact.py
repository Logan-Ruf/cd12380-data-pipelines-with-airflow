from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries


class LoadFactOperator(BaseOperator):
    ui_color = "#F98866"

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        table="",
        sql_query="",
        truncate=False,
        *args,
        **kwargs,
    ):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.truncate = truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate:
            self.log.info(f"Truncating fact table {self.table}")
            redshift.run(SqlQueries.truncate.format(table=self.table))
            self.log.info(f"{self.table} truncated successfully")

        self.log.info(f"Inserting into fact table {self.table}")
        redshift.run(
            SqlQueries.insert_template.format(
                table=self.table,
                query=self.sql_query,
            )
        )
        self.log.info(f"{self.table} inserted successfully")
