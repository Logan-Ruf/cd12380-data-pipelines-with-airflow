from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries


class DataQualityOperator(BaseOperator):
    ui_color = "#89DA59"

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        qc_queries=None,
        *args,
        **kwargs,
    ):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.qc_queries = qc_queries or [
            {
                "query": SqlQueries.qc_rows_exist,
                "table": "songs",
                "expected": 1,
            }
        ]

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        failures = []

        for i, qc in enumerate(self.qc_queries, start=1):
            description = f"check {i} of {len(self.qc_queries)} on '{qc['table']}'"
            query = qc["query"].format(table=qc["table"])

            self.log.info(f"Running data quality {description}: {query.strip()}")
            records = redshift.get_records(query)

            if not records or not records[0]:
                failures.append(
                    f"{description}: query returned no results ({query.strip()})"
                )
                continue

            actual = records[0][0]
            expected = qc["expected"]

            if actual != expected:
                failures.append(f"{description}: expected {expected}, got {actual}")
                continue

        if failures:
            failure_list = "\n".join(failures)
            raise ValueError(
                f"Data quality check(s) failed "
                f"({len(failures)} of {len(self.qc_queries)}):\n{failure_list}"
            )

        self.log.info("All data quality checks passed")
