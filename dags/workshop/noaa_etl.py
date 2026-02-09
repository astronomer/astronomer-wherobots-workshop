from airflow.sdk import dag, task, chain, Asset
from include.custom_wherobots_provider.operators.operators_run_custom import (
    WherobotsRunOperator,
)
from include.custom_wherobots_provider.operators.operators_sql_custom import (
    WherobotsSqlOperator,
)
from wherobots.db.runtime import Runtime
from wherobots.db.region import Region
import os

_WHEROBOTS_CONN_ID = os.getenv("WHEROBOTS_CONN_ID", "wherobots_default")
_RUNTIME = Runtime.TINY
_REGION = Region.AWS_US_WEST_2
_CATALOG = os.getenv("CATALOG", "org_catalog")
_DATABASE = os.getenv("DATABASE", "astronomer_workshop")
_S3_URI = os.getenv("S3_URI").rstrip("/")
_END_DATE = os.getenv("END_DATE", "2026-01-01")
_LOOKBACK_MONTHS = os.getenv("LOOKBACK_MONTHS", 3)


@dag(
    schedule=[Asset("start_noaa_etl")],
    template_searchpath=[f"{os.getenv('AIRFLOW_HOME')}/include/sql"],
)
def noaa_etl():

    @task
    def get_today_date(**context):
        from datetime import datetime
        from dateutil.relativedelta import relativedelta

        run_after = context["dag_run"].run_after
        run_date = datetime.strptime(run_after.strftime("%Y-%m-%d"), "%Y-%m-%d")

        return run_date.strftime("%Y-%m-%d")

    _get_today_date = get_today_date()

    _load_hail_data = WherobotsRunOperator(
        task_id="load_hail_data",
        wherobots_conn_id=_WHEROBOTS_CONN_ID,
        name="load_hail_data_{{ dag_run.run_after.strftime('%Y%m%dT%H%M%S') }}",
        runtime=_RUNTIME,
        region=_REGION,
        run_python={
            "uri": f"{_S3_URI}/load_hail_data.py",
            "args": [
                "--catalog",
                _CATALOG,
                "--database",
                _DATABASE,
                "--end-date",
                _get_today_date,
                "--lookback-months",
                str(_LOOKBACK_MONTHS),
            ],
        },
        poll_logs=True,
    )

    _verify_loaded_hail_data = WherobotsSqlOperator(
        task_id="verify_loaded_hail_data",
        wherobots_conn_id=_WHEROBOTS_CONN_ID,
        runtime=_RUNTIME,
        region=_REGION,
        sql="workshop/noaa_etl/verify_loaded_hail_data.sql",
        parameters={"catalog": _CATALOG, "database": _DATABASE},
        do_xcom_push=True,
    )

    @task(outlets=[Asset("noaa_data_ready")])
    def print_verify_loaded_hail_data(hail_raw_summary_table):
        print(hail_raw_summary_table.T.to_string())

    _print_verify_loaded_hail_data = print_verify_loaded_hail_data(
        hail_raw_summary_table=_verify_loaded_hail_data.output
    )  # Airflow automatically infers the dependencies between tasks when using @task and providing input arguments from upstream tasks

    chain(_load_hail_data, _verify_loaded_hail_data)


noaa_etl()
