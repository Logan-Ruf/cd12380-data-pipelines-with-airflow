from airflow.hooks.base import BaseHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries


class StageToRedshiftOperator(BaseOperator):
    ui_color = "#358140"
    template_fields = ("s3_key",)

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        aws_credentials_id="",
        table="",
        s3_bucket="",
        s3_key="",
        json="auto",
        *args,
        **kwargs,
    ):

        super(StageToRedshiftOperator, self).__init__(
            *args,
            **kwargs,
        )
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.json = json

    def execute(self, context):
        aws_hook = BaseHook.get_connection(self.aws_credentials_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table: " + self.table)
        redshift.run(SqlQueries.truncate.format(table=self.table))

        self.log.info("Copying data from S3 to Redshift")
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)

        session_token = aws_hook.extra_dejson.get("aws_session_token")
        session_token_clause = (
            "SESSION_TOKEN '{}'".format(session_token) if session_token else ""
        )

        formatted_sql = SqlQueries.stage_redshift_sql.format(
            self.table,
            s3_path,
            aws_hook.login,
            aws_hook.password,
            session_token_clause,
            self.json,
        )
        self.log.info(formatted_sql)
        redshift.run(formatted_sql)
