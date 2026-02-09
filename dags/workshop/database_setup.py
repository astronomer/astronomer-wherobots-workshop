from airflow.sdk import dag, chain
from include.custom_wherobots_provider.operators.operators_sql_custom import (
    WherobotsSqlOperator,
)
from wherobots.db.runtime import Runtime
from wherobots.db.region import Region
import os

_WHEROBOTS_CONN_ID = os.getenv("WHEROBOTS_CONN_ID", "wherobots_default")
_CATALOG = os.getenv("CATALOG", "org_catalog")
_DATABASE = os.getenv("DATABASE", "workshoptestnotebook")
_RUNTIME = Runtime.TINY
_REGION = Region.AWS_US_WEST_2


_TABLES_TO_CREATE = [
    "hail_raw",
    "hail_state",
    "hail_county",
    "state_boundary",
    "postcode_areas",
    "risk_comparison",
]


@dag(
    template_searchpath=[
        f"{os.getenv('AIRFLOW_HOME')}/include/sql"
    ],  # point to the folder where SQL scripts are stored
)
def database_setup():

    create_database_if_not_exists = WherobotsSqlOperator(
        task_id="create_database_if_not_exists",
        wherobots_conn_id=_WHEROBOTS_CONN_ID,
        runtime=_RUNTIME,
        region=_REGION,
        sql="""CREATE DATABASE IF NOT EXISTS %(catalog)s.%(database)s;""",  # You can can run SQL directly or store it in a script.
        parameters={
            "catalog": _CATALOG,
            "database": _DATABASE,
        },  # Parameters are passed to the SQL at runtime.
        return_last=False,
    )

    _list_of_table_creation_tasks = []

    # you can create Airflow tasks using Python code like loops
    for table in _TABLES_TO_CREATE:
        create_table_if_not_exists = WherobotsSqlOperator(
            task_id=f"create_{table}_table_if_not_exists",
            wherobots_conn_id=_WHEROBOTS_CONN_ID,
            runtime=_RUNTIME,
            region=_REGION,
            sql=f"workshop/database_setup/create_{table}_table_if_not_exists.sql",  # run SQL stored in a script in a folder supplied to template_searchpath
            parameters={
                "catalog": _CATALOG,
                "database": _DATABASE,
            },  # Parameters are passed to the SQL at runtime.
            return_last=False,
        )

        _list_of_table_creation_tasks.append(create_table_if_not_exists)

    # the database needs to be created before the tables can be created
    chain(
        create_database_if_not_exists,
        _list_of_table_creation_tasks,
    )


database_setup()
