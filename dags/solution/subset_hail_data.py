from airflow.sdk import dag, task, chain, Asset
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
_DATABASE = os.getenv("DATABASE", "workshoptestnotebook")


@dag(
    schedule=(Asset("noaa_data_ready") & Asset("overture_data_ready")),
    template_searchpath=[f"{os.getenv('AIRFLOW_HOME')}/include/sql"],
)
def subset_hail_data():

    _subset_hail_data_state = WherobotsSqlOperator(
        task_id="subset_hail_data_state",
        wherobots_conn_id=_WHEROBOTS_CONN_ID,
        runtime=_RUNTIME,
        region=_REGION,
        sql="workshop/subset_hail_data/subset_hail_data_state.sql",
        parameters={
            "catalog": _CATALOG,
            "database": _DATABASE,
            "us_postcode": "{{ var.value.us_postcode }}",
        },
    )

    _check_subset_hail_data_state = WherobotsSqlOperator(
        task_id="check_subset_hail_data_state",
        wherobots_conn_id=_WHEROBOTS_CONN_ID,
        runtime=_RUNTIME,
        region=_REGION,
        sql="workshop/subset_hail_data/check_subset_hail_data_state.sql",
        parameters={
            "catalog": _CATALOG,
            "database": _DATABASE,
            "us_postcode": "{{ var.value.us_postcode }}",
        },
    )

    @task
    def print_subset_hail_data_state(subset_hail_data_state):
        print(subset_hail_data_state.T.to_string())

    _print_subset_hail_data_state = print_subset_hail_data_state(
        subset_hail_data_state=_check_subset_hail_data_state.output
    )

    _subset_hail_data_county = WherobotsSqlOperator(
        task_id="subset_hail_data_county",
        wherobots_conn_id=_WHEROBOTS_CONN_ID,
        runtime=_RUNTIME,
        region=_REGION,
        sql="workshop/subset_hail_data/subset_hail_data_county.sql",
        parameters={
            "catalog": _CATALOG,
            "database": _DATABASE,
            "us_postcode": "{{ var.value.us_postcode }}",
        },
    )

    _check_subset_hail_data_county = WherobotsSqlOperator(
        task_id="check_subset_hail_data_county",
        wherobots_conn_id=_WHEROBOTS_CONN_ID,
        runtime=_RUNTIME,
        region=_REGION,
        sql="workshop/subset_hail_data/check_subset_hail_data_county.sql",
        parameters={
            "catalog": _CATALOG,
            "database": _DATABASE,
            "us_postcode": "{{ var.value.us_postcode }}",
        },
    )

    @task
    def print_subset_hail_data_county(subset_hail_data_county):
        print(subset_hail_data_county.T.to_string())

    _print_subset_hail_data_county = print_subset_hail_data_county(
        subset_hail_data_county=_check_subset_hail_data_county.output
    )

    _calculate_comparison_hail_state_country = WherobotsSqlOperator(
        task_id="calculate_comparison_hail_state_country",
        wherobots_conn_id=_WHEROBOTS_CONN_ID,
        runtime=_RUNTIME,
        region=_REGION,
        sql="workshop/subset_hail_data/calculate_comparison_hail_state_country.sql",
        parameters={
            "catalog": _CATALOG,
            "database": _DATABASE,
            "us_postcode": "{{ var.value.us_postcode }}",
        },
    )

    _check_calculate_comparison_hail_state_country = WherobotsSqlOperator(
        task_id="check_calculate_comparison_hail_state_country",
        wherobots_conn_id=_WHEROBOTS_CONN_ID,
        runtime=_RUNTIME,
        region=_REGION,
        sql="workshop/subset_hail_data/check_calculate_comparison_hail_state_country.sql",
        parameters={
            "catalog": _CATALOG,
            "database": _DATABASE,
            "us_postcode": "{{ var.value.us_postcode }}",
        },
    )

    @task(outlets=[Asset("hail_comparison_ready")])
    def print_calculate_comparison_hail_state_country(
        calculate_comparison_hail_state_country,
    ):
        for _, row in calculate_comparison_hail_state_country.iterrows():
            print(f"\n=== {row['area_type']}: {row['area_id']} ===")
            for col, val in row.items():
                if col not in ["area_type", "area_id"]:
                    print(f"  {col}: {val}")

    _print_calculate_comparison_hail_state_country = print_calculate_comparison_hail_state_country(
        calculate_comparison_hail_state_country=_check_calculate_comparison_hail_state_country.output
    )

    chain(
        _subset_hail_data_county,
        _check_subset_hail_data_county,
    )

    chain(
        _subset_hail_data_state,
        _check_subset_hail_data_state,
    )

    chain(
        _subset_hail_data_state,
        _subset_hail_data_county,
        _calculate_comparison_hail_state_country,
    )


subset_hail_data()
