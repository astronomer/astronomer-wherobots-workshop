from airflow.sdk import dag, task
from wherobots.db.runtime import Runtime
from wherobots.db.region import Region
from include.custom_wherobots_provider.operators.operators_sql_custom import CUSTOMWherobotsSqlOperator
from include.custom_wherobots_provider.operators.operators_run_custom import CUSTOMWherobotsRunOperator
import logging

logger = logging.getLogger(__name__)

@dag
def test_wherobots_connection():

    @task
    def test_pyspark_session():
        from sedona.spark import SedonaContext
        import pyspark

        sedona_jars = (
            "org.apache.sedona:sedona-spark-4.0_2.13:1.8.0,"
            "org.datasyslab:geotools-wrapper:1.8.0-33.1,"
            "org.apache.hadoop:hadoop-aws:3.4.1,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.777"
        )

        config = (
            SedonaContext.builder()
            .appName("NOAA-Local-Test")
            .config("spark.jars.packages", sedona_jars)
            .config(
                "spark.jars.repositories",
                "https://artifacts.unidata.ucar.edu/repository/unidata-all",
            )
            .config(
                "spark.hadoop.fs.s3a.bucket.noaa-swdi-pds.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider",
            )
            .getOrCreate()
        )
        sedona = SedonaContext.create(config)

        dataset = "hail"
        year = "2023"
        s3_uri = f"s3a://noaa-swdi-pds/{dataset}-{year}.csv"
        column_names = [
            "ZTIME",
            "LON",
            "LAT",
            "WSR_ID",
            "CELL_ID",
            "RANGE",
            "AZIMUTH",
            "SEVPROB",
            "PROB",
            "MAXSIZE",
        ]


        df = sedona.read.option("comment", "#").csv(s3_uri).toDF(*column_names)

        count = df.count()
        sample = df.limit(5).collect()

        for row in sample:
            logger.info(row)

        sedona.stop()

    test_pyspark_session()

    @task
    def test_sql_hook():
        from include.custom_wherobots_provider.hooks.hooks_sql_custom import CUSTOMWherobotsSqlHook

        hook = CUSTOMWherobotsSqlHook(
            wherobots_conn_id="wherobots_default",
            runtime=Runtime.MICRO,
            region=Region.AWS_US_WEST_2,
        )

        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("SELECT 1")

        return cursor.fetchall()

    test_sql_hook()

    CUSTOMWherobotsSqlOperator(
        task_id="test_sql_operator",
        wherobots_conn_id="wherobots_default",
        runtime=Runtime.MICRO,
        region=Region.AWS_US_WEST_2,
        sql="SELECT 1",
    )

    # TODO: Update once access
    CUSTOMWherobotsRunOperator(
        task_id="test_run_operator",
        wherobots_conn_id="wherobots_default",
        runtime=Runtime.MICRO,
        region=Region.AWS_US_WEST_2,
        run_python={"uri": "S3-PATH-TO-YOUR-FILE"},
    )


test_wherobots_connection()
