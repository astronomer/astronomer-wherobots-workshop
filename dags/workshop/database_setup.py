from airflow.sdk import dag, chain, Asset, task
from include.custom_wherobots_provider.operators.operators_sql_custom import (
    WherobotsSqlOperator,
)
from wherobots.db.runtime import Runtime
from wherobots.db.region import Region
import os

_WHEROBOTS_CONN_ID = os.getenv("WHEROBOTS_CONN_ID", "wherobots_default")
_CATALOG = os.getenv("CATALOG", "org_catalog")
_DATABASE = os.getenv("DATABASE", "astronomer_workshop")
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
    schedule=[Asset("start_database_setup")],
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

    @task
    def load_sql_scripts():
        sql_dir = f"{os.getenv('AIRFLOW_HOME')}/include/sql/workshop/database_setup"
        sql_scripts = []
        for table in _TABLES_TO_CREATE:
            file_path = f"{sql_dir}/create_{table}_table_if_not_exists.sql"
            with open(file_path, "r") as f:
                sql_scripts.append(f.read())
        return sql_scripts

    _sql_scripts = load_sql_scripts()

    create_tables_if_not_exists = WherobotsSqlOperator.partial(
        task_id="create_tables_if_not_exists",
        wherobots_conn_id=_WHEROBOTS_CONN_ID,
        runtime=_RUNTIME,
        region=_REGION,
        parameters={
            "catalog": _CATALOG,
            "database": _DATABASE,
        },  # Parameters are passed to the SQL at runtime.
        return_last=False,
    ).expand(sql=_sql_scripts)

    @task()#outlets=[Asset("start_noaa_etl"), Asset("start_overture_etl")])
    def database_setup_complete():
        print("Database setup complete")

    _database_setup_complete = database_setup_complete()

    # the database needs to be created before the tables can be created
    chain(
        create_database_if_not_exists,
        create_tables_if_not_exists,
        _database_setup_complete,
    )


database_setup()
