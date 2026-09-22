from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
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
        aws_hook = AwsBaseHook(aws_conn_id=self.aws_credentials_id, client_type="s3")
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table: " + self.table)
        redshift.run(SqlQueries.truncate.format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        formatted_sql = SqlQueries.stage_redshift_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json,
        )
        redshift.run(formatted_sql)
