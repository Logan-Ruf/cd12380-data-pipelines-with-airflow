import tempfile

import pendulum
from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from udacity import sql_statements


@dag(start_date=pendulum.now())
def load_data_to_redshift():

    @task
    def load_task():
        bucket = Variable.get("s3_bucket")
        prefix = Variable.get("s3_prefix")
        key = f"{prefix}/divvy/unpartitioned/divvy_trips_2018.csv"

        # Postgres has no S3 COPY, so download the CSV and stream it in.
        s3_hook = S3Hook(aws_conn_id="aws_credentials")
        redshift_hook = PostgresHook("redshift")
        with tempfile.TemporaryDirectory() as tmp_dir:
            local_path = s3_hook.download_file(
                key=key, bucket_name=bucket, local_path=tmp_dir
            )
            redshift_hook.run("TRUNCATE TABLE trips")
            redshift_hook.copy_expert(sql_statements.COPY_TRIPS_SQL, local_path)

    create_table_task = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="redshift",
        sql=sql_statements.CREATE_TRIPS_TABLE_SQL,
    )

    location_traffic_task = PostgresOperator(
        task_id="calculate_location_traffic",
        postgres_conn_id="redshift",
        sql=[
            sql_statements.LOCATION_TRAFFIC_SQL_DROP,
            sql_statements.LOCATION_TRAFFIC_SQL_CREATE,
        ],
    )

    load_data = load_task()
    create_table_task >> load_data
    load_data >> location_traffic_task


s3_to_redshift_dag = load_data_to_redshift()
