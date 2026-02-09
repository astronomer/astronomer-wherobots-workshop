"""
Helper Dag that tests the connection to Wherobots for
the WherobotsSQLHook, WherobotsRunOperator, and WherobotsSqlOperator.
"""

from airflow.sdk import dag, task
from wherobots.db.runtime import Runtime
from wherobots.db.region import Region
from include.custom_wherobots_provider.operators.operators_sql_custom import (
    WherobotsSqlOperator,
)
from include.custom_wherobots_provider.operators.operators_run_custom import (
    WherobotsRunOperator,
)
import os

_WHEROBOTS_CONN_ID = os.getenv("WHEROBOTS_CONN_ID", "wherobots_default")
_URI_PYTHON_SCRIPT = f"{os.getenv('S3_URI')}/testing_script.py"
_RUNTIME = Runtime.MICRO
_REGION = Region.AWS_US_WEST_2


@dag(tags=["helper"])
def test_wherobots_connection():

    @task
    def test_sql_hook():
        from include.custom_wherobots_provider.hooks.hooks_sql_custom import (
            WherobotsSqlHook,
        )

        hook = WherobotsSqlHook(
            wherobots_conn_id=_WHEROBOTS_CONN_ID,
            runtime=_RUNTIME,
            region=_REGION,
        )

        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("SELECT 1")

        return cursor.fetchall()

    test_sql_hook()

    WherobotsSqlOperator(
        task_id="test_sql_operator",
        wherobots_conn_id=_WHEROBOTS_CONN_ID,
        runtime=_RUNTIME,
        region=_REGION,
        sql="SELECT 1",
    )

    WherobotsRunOperator(
        task_id="test_run_operator",
        name="test_run_operator_{{ dag_run.run_after.strftime('%Y%m%dT%H%M%S') }}",
        wherobots_conn_id=_WHEROBOTS_CONN_ID,
        runtime=_RUNTIME,
        region=_REGION,
        run_python={"uri": _URI_PYTHON_SCRIPT},
        poll_logs=True,
    )


test_wherobots_connection()
