from airflow.sdk import dag, Param, task, chain
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

_TABLES_TO_EMPTY = [
    "hail_state",
    "hail_county",
    "postcode_areas",
    "state_boundary",
    "risk_comparison",
]

_ALL_TABLES = _TABLES_TO_EMPTY + ["hail_raw"]


@dag(
    params={
        "empty_tables": Param(
            default=False,
            type="boolean",
            description="Empties the following tables: hail_state, hail_county, postcode_areas, state_boundary, risk_comparison",
        ),
        "empty_hail_raw": Param(
            default=False,
            type="boolean",
            description="Empties the hail_raw table.",
        ),
        "drop_all_tables": Param(
            default=False,
            type="boolean",
            description="Drops all tables in the workshop database.",
        ),
    },
    tags=["helper", "Danger zone!"],
)
def clean_up_dag():

    @task.branch
    def fetch_user_input(**context):
        empty_tables = context["params"]["empty_tables"]
        empty_hail_raw = context["params"]["empty_hail_raw"]
        drop_all_tables = context["params"]["drop_all_tables"]

        action_to_take = []

        if drop_all_tables:
            return "drop_all_tables"

        if empty_tables:
            action_to_take.append("empty_tables")
        if empty_hail_raw:
            action_to_take.append("empty_hail_raw")

        return action_to_take

    _fetch_user_input = fetch_user_input()

    _empty_hail_raw = WherobotsSqlOperator(
        task_id="empty_hail_raw",
        wherobots_conn_id=_WHEROBOTS_CONN_ID,
        runtime=_RUNTIME,
        region=_REGION,
        sql="DELETE FROM %(catalog)s.%(database)s.hail_raw;",
        parameters={
            "catalog": _CATALOG,
            "database": _DATABASE,
        },
    )

    @task
    def empty_tables():
        from include.custom_wherobots_provider.hooks.hooks_sql_custom import (
            WherobotsSqlHook,
        )

        for table in _TABLES_TO_EMPTY:
            hook = WherobotsSqlHook(
                wherobots_conn_id=_WHEROBOTS_CONN_ID, runtime=_RUNTIME, region=_REGION
            )
            hook.run(
                f"DELETE FROM %(catalog)s.%(database)s.{table}",
                parameters={
                    "catalog": _CATALOG,
                    "database": _DATABASE,
                },
            )

    _empty_tables = empty_tables()

    @task
    def drop_all_tables():
        from include.custom_wherobots_provider.hooks.hooks_sql_custom import (
            WherobotsSqlHook,
        )

        for table in _ALL_TABLES:
            hook = WherobotsSqlHook(
                wherobots_conn_id=_WHEROBOTS_CONN_ID, runtime=_RUNTIME, region=_REGION
            )
            hook.run(
                f"DROP TABLE IF EXISTS %(catalog)s.%(database)s.{table}",
                parameters={
                    "catalog": _CATALOG,
                    "database": _DATABASE,
                },
            )

    _drop_all_tables = drop_all_tables()

    chain(
        _fetch_user_input,
        [_empty_hail_raw, _empty_tables, _drop_all_tables],
    )


clean_up_dag()
