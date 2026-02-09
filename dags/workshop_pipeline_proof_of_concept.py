from airflow.sdk import dag, task, chain, task_group, Param
from pendulum import datetime
from wherobots.db.runtime import Runtime
from wherobots.db.region import Region
from include.custom_wherobots_provider.operators.operators_sql_custom import (
    WherobotsSqlOperator,
)
import os


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    params={
        "start_date": Param(
            "2023-01-01",
            type="string",
            description="Enter the start date for the data processing.",
        ),
        "end_date": Param(
            "2023-01-10",
            type="string",
            description="Enter the end date for the data processing.",
        ),
        "subset_date": Param(
            "2023-01-02",
            type="string",
            description="Enter the subset date for the data processing.",
        ),
        "catalog": Param(
            "org_catalog",
            type="string",
            description="Enter your catalog name.",
        ),
        "database": Param(
            "workshop",
            type="string",
            description="Enter your database name.",
        ),
        "output_path": Param(
            "/usr/local/airflow/include/output/my_map.html",
            type="string",
            description="Enter the output path for the visualization.",
        ),
    },
    template_searchpath=[f"{os.getenv('AIRFLOW_HOME')}/include/sql"],
)
def workshop_pipeline_proof_of_concept():

    @task_group
    def setup_wherobots():

        create_database_if_not_exists = WherobotsSqlOperator(
            task_id="create_database_if_not_exists",
            wherobots_conn_id="wherobots_default",
            runtime=Runtime.TINY,
            region=Region.AWS_US_WEST_2,
            sql="proof_of_concept/create_db_if_not_exists.sql",
            parameters={
                "catalog": "{{ params.catalog }}",
                "database": "{{ params.database }}",
            },
            return_last=False,
        )

        create_table_if_not_exists = WherobotsSqlOperator(
            task_id="create_table_if_not_exists",
            wherobots_conn_id="wherobots_default",
            runtime=Runtime.TINY,
            region=Region.AWS_US_WEST_2,
            sql="proof_of_concept/create_table_if_not_exists.sql",
            parameters={
                "catalog": "{{ params.catalog }}",
                "database": "{{ params.database }}",
            },
            return_last=False,
        )

        chain(
            create_database_if_not_exists,
            create_table_if_not_exists,
        )

    _setup_wherobots = setup_wherobots()

    @task_group
    def load_data():

        @task
        def get_date_ranges(**context):
            from include.helpers.datetime_helpers import generate_daily_dates

            start_date = context["params"]["start_date"]
            end_date = context["params"]["end_date"]

            return generate_daily_dates(start_date, end_date)

        _get_date_ranges = get_date_ranges()

        @task(max_active_tis_per_dag=2)
        def load_data_s3_to_wherobots(date: str, **context):
            from include.custom_wherobots_provider.hooks.hooks_sql_custom import (
                WherobotsSqlHook,
            )
            from include.helpers.sedona_helpers import (
                fetch_noaa_data_from_s3,
                create_merge_sql,
            )
            import logging

            logger = logging.getLogger(__name__)

            year = date[:4]

            chunks = fetch_noaa_data_from_s3(
                date=date, s3_uri=f"s3a://noaa-swdi-pds/hail-{year}.csv"
            )

            hook = CUSTOMWherobotsSqlHook(
                wherobots_conn_id="wherobots_default",
                runtime=Runtime.TINY,
                region=Region.AWS_US_WEST_2,
            )
            conn = hook.get_conn()
            cursor = conn.cursor()

            total_processed = 0
            batch_size = 1000
            catalog = context["params"]["catalog"]
            database = context["params"]["database"]
            

            for chunk_idx, chunk in enumerate(chunks):
                for i in range(0, len(chunk), batch_size):
                    batch = chunk[i : i + batch_size]

                    merge_sql = create_merge_sql(batch, catalog, database)

                    logger.debug(
                        f"Merging batch {chunk_idx * len(chunk) // batch_size + i // batch_size + 1}: {len(batch)} rows..."
                    )
                    cursor.execute(merge_sql)
                    cursor.fetchall()

                    total_processed += len(batch)

            conn.close()

            logger.info(
                f"Successfully merged {total_processed} rows for ({date})"
            )


        _load_data_s3_to_wherobots = load_data_s3_to_wherobots.expand(
            date=_get_date_ranges
        )

        chain(_get_date_ranges, _load_data_s3_to_wherobots)

    _load_data = load_data()

    _verify_data_loaded = WherobotsSqlOperator(
        task_id="verify_data_loaded",
        wherobots_conn_id="wherobots_default",
        runtime=Runtime.TINY,
        region=Region.AWS_US_WEST_2,
        sql="proof_of_concept/count_rows_hail_table.sql",
        parameters={
            "catalog": "{{ params.catalog }}",
            "database": "{{ params.database }}",
        },
        return_last=False,
    )

    chain(_load_data, _verify_data_loaded)

    @task_group
    def transform_data():

        _create_texas_hail_table = WherobotsSqlOperator(
            task_id="create_texas_hail_table_for_subset_date",
            wherobots_conn_id="wherobots_default",
            runtime=Runtime.TINY,
            region=Region.AWS_US_WEST_2,
            sql="proof_of_concept/create_texas_table_for_subset_date.sql",
            parameters={
                "catalog": "{{ params.catalog }}",
                "database": "{{ params.database }}",
                "table_name": "texas_hail_subset",
                "subset_date": "{{ params.subset_date }}",
            },
            return_last=False,
        )

        _verify_texas_data = WherobotsSqlOperator(
            task_id="verify_texas_data_for_subset_date",
            wherobots_conn_id="wherobots_default",
            runtime=Runtime.TINY,
            region=Region.AWS_US_WEST_2,
            sql="proof_of_concept/count_rows_texas_table.sql",
            parameters={
                "catalog": "{{ params.catalog }}",
                "database": "{{ params.database }}",
                "table_name": "texas_hail_subset",
            },
            return_last=False,
        )

        chain(
            _create_texas_hail_table,
            _verify_texas_data,
        )

    _transform_data = transform_data()

    @task
    def create_visualization(**context):
        from include.custom_wherobots_provider.hooks.hooks_sql_custom import (
            WherobotsSqlHook,
        )
        from include.helpers.sedona_helpers import create_sedona_context
        from include.helpers.visualization_helpers import create_kepler_map
        import logging

        logger = logging.getLogger(__name__)

        hook = WherobotsSqlHook(
            wherobots_conn_id="wherobots_default",
            runtime=Runtime.TINY,
            region=Region.AWS_US_WEST_2,
        )
        conn = hook.get_conn()
        cursor = conn.cursor()

        org_catalog = context["params"]["catalog"]
        database = context["params"]["database"]
        output_path = context["params"]["output_path"]

        cursor.execute(
            f"SELECT * FROM {org_catalog}.{database}.texas_hail_subset"
        )
        hail_results = cursor.fetchall()

        hail_columns = [desc[0] for desc in cursor.description]

        cursor.execute(
            """
            SELECT id, geometry
            FROM wherobots_open_data.overture_maps_foundation.divisions_division_area
            WHERE subtype = 'county' AND region = 'US-TX'
        """
        )
        counties_results = cursor.fetchall()
        counties_columns = [desc[0] for desc in cursor.description]

        conn.close()

        sedona = create_sedona_context("NOAA-Visualization")

        create_kepler_map(
            sedona=sedona,
            hail_data=hail_results,
            hail_columns=hail_columns,
            counties_data=counties_results,
            counties_columns=counties_columns,
            output_path=output_path,
        )

        sedona.stop()

    _create_visualization = create_visualization()

    chain(_load_data, _verify_data_loaded)

    chain(
        _setup_wherobots,
        _load_data,
        _transform_data,
        _create_visualization,
    )


workshop_pipeline_proof_of_concept()
