import logging

import pendulum
from airflow.decorators import dag, task
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable


@dag(start_date=pendulum.now())
def list_keys():

    @task
    def list_keys():
        hook = S3Hook(aws_conn_id="aws_credentials")
        bucket = Variable.get("s3_bucket")
        prefix = Variable.get("s3_prefix")
        logging.info(f"Listing Keys from {bucket}/{prefix}")
        keys = hook.list_keys(bucket, prefix=prefix)
        for key in keys:
            logging.info(f"- s3://{bucket}/{key}")

    list_keys()


list_keys_dag = list_keys()
