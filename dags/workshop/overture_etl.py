from airflow.sdk import dag, task, chain, Asset
from include.custom_wherobots_provider.operators.operators_sql_custom import (
    WherobotsSqlOperator,
)
from include.custom_wherobots_provider.operators.operators_run_custom import (
    WherobotsRunOperator,
)
from wherobots.db.runtime import Runtime
from wherobots.db.region import Region
import os

_WHEROBOTS_CONN_ID = os.getenv("WHEROBOTS_CONN_ID", "wherobots_default")
_RUNTIME = Runtime.TINY
_REGION = Region.AWS_US_WEST_2
_CATALOG = os.getenv("CATALOG", "org_catalog")
_DATABASE = os.getenv("DATABASE", "astronomer_workshop")
_COUNTRY = os.getenv("COUNTRY", "US")
_S3_URI = os.getenv("S3_URI").rstrip("/")


@dag(
    schedule=[Asset("start_overture_etl")],
    template_searchpath=[f"{os.getenv('AIRFLOW_HOME')}/include/sql"],
)
def overture_etl():

    _create_state_boundary = WherobotsSqlOperator(
        task_id="create_state_boundary",
        wherobots_conn_id=_WHEROBOTS_CONN_ID,
        runtime=_RUNTIME,
        region=_REGION,
        sql="workshop/overture_etl/create_state_boundary.sql",
        parameters={
            "catalog": _CATALOG,
            "database": _DATABASE,
            "country": _COUNTRY,
            "us_postcode": "{{ var.value.us_postcode }}",
        },
    )

    _check_state_boundary_loaded = WherobotsSqlOperator(
        task_id="check_state_boundary_loaded",
        wherobots_conn_id=_WHEROBOTS_CONN_ID,
        runtime=_RUNTIME,
        region=_REGION,
        sql="workshop/overture_etl/check_state_boundary_loaded.sql",
        parameters={"catalog": _CATALOG, "database": _DATABASE},
    )

    _create_county_area = WherobotsRunOperator(
        task_id="create_county_area",
        name="create_county_area_{{ dag_run.run_after.strftime('%Y%m%dT%H%M%S') }}",
        wherobots_conn_id=_WHEROBOTS_CONN_ID,
        runtime=_RUNTIME,
        region=_REGION,
        run_python={
            "uri": f"{_S3_URI}/create_county_area.py",
            "args": [
                "--catalog",
                _CATALOG,
                "--database",
                _DATABASE,
                "--postcode",
                "{{ var.value.us_postcode }}",
                "--country",
                _COUNTRY,
            ],
        },
        poll_logs=True,
    )

    _check_county_area_created = WherobotsSqlOperator(
        task_id="check_county_area_created",
        wherobots_conn_id=_WHEROBOTS_CONN_ID,
        runtime=_RUNTIME,
        region=_REGION,
        sql="workshop/overture_etl/check_county_area_created.sql",
        parameters={
            "catalog": _CATALOG,
            "database": _DATABASE,
            "country": _COUNTRY,
            "us_postcode": "{{ var.value.us_postcode }}",
        },
    )

    @task(outlets=[Asset("overture_data_ready")])
    def overture_data_ready():
        print("Overture data ready")

    _overture_data_ready = overture_data_ready()

    chain(
        _create_state_boundary,
        _check_state_boundary_loaded,
        _overture_data_ready,
    )

    chain(
        _create_county_area,
        _check_county_area_created,
        _overture_data_ready,
    )


overture_etl()
