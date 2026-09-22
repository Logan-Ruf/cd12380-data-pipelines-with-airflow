from datetime import timedelta

import pendulum
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from operators import (
    DataQualityOperator,
    LoadDimensionOperator,
    LoadFactOperator,
    StageToRedshiftOperator,
)

from plugins.helpers.sql_queries import SqlQueries

default_args = {
    "owner": "chris-ruf",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_retry": False,
}


@dag(
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="0 * * * *",
    catchup=False,
    start_date=pendulum.datetime(year=2018, month=11, day=1, tz="UTC"),
)
def final_project():

    s3_bucket = Variable.get("s3_bucket")

    start_operator = DummyOperator(task_id="Begin_execution")
    stop_operator = DummyOperator(task_id="Stop_execution")

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="Stage_events",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_events",
        s3_bucket=s3_bucket,
        s3_key="log_data/{{ execution_date.year }}/{{ '%02d'|format(execution_date.month) }}/{{ execution_date.year }}-{{ '%02d'|format(execution_date.month) }}-{{ '%02d'|format(execution_date.day) }}-events.json",
        json=f"s3://{s3_bucket}/log_json_path.json",
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="Stage_songs",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_songs",
        s3_bucket=s3_bucket,
        s3_key="song_data",
        json="auto",
    )

    load_songplays_table = LoadFactOperator(
        task_id="Load_songplays_fact_table",
        redshift_conn_id="redshift",
        table="songplays",
        sql_query=SqlQueries.songplay_table_insert,
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id="Load_user_dim_table",
        redshift_conn_id="redshift",
        table="users",
        sql_query=SqlQueries.user_table_insert,
        truncate=True,
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id="Load_song_dim_table",
        redshift_conn_id="redshift",
        table="songs",
        sql_query=SqlQueries.user_table_insert,
        truncate=True,
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id="Load_artist_dim_table",
        redshift_conn_id="redshift",
        table="artists",
        sql_query=SqlQueries.artist_table_insert,
        truncate=True,
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id="Load_time_dim_table",
        redshift_conn_id="redshift",
        table="time",
        sql_query=SqlQueries.time_table_insert,
        truncate=True,
    )

    run_quality_checks = DataQualityOperator(
        task_id="Run_data_quality_checks",
    )

    start_operator >> stage_events_to_redshift >> load_songplays_table
    start_operator >> stage_songs_to_redshift >> load_songplays_table
    load_songplays_table >> load_artist_dimension_table >> run_quality_checks
    load_songplays_table >> load_song_dimension_table >> run_quality_checks
    load_songplays_table >> load_time_dimension_table >> run_quality_checks
    load_songplays_table >> load_user_dimension_table >> run_quality_checks
    run_quality_checks >> stop_operator


final_project_dag = final_project()
