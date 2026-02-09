from airflow.sdk import dag
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
_DATABASE = os.getenv("DATABASE", "workshoptestnotebook")
_S3_URI = os.getenv(
    "S3_URI", "s3://wbts-wbc-wayuylkmvf/sm3tlot7a0/data/customer-3i2is57clci3n7/testing"
)


@dag
def visualization():

    _create_visualization = WherobotsRunOperator(
        task_id="create_visualization",
        name="create_visualization_{{ dag_run.run_after.strftime('%Y%m%dT%H%M%S') }}",
        wherobots_conn_id=_WHEROBOTS_CONN_ID,
        runtime=_RUNTIME,
        region=_REGION,
        run_python={
            "uri": f"{_S3_URI}/visualization.py",
            "args": [
                "--catalog", _CATALOG,
                "--database", _DATABASE,
                "--postcode", "{{ var.value.us_postcode }}",
                "--s3-uri", _S3_URI,
            ],
        },
        environment={
            "dependencies": [
                {"libraryName": "keplergl", "libraryVersion": "0.3.2"},
                {"libraryName": "geopandas", "libraryVersion": "0.14.0"},
            ],
        },
        poll_logs=True,
    )


visualization()