from airflow.sdk import dag, task, Param, Asset, chain


@dag(params={"us_postcode": Param(type="string", default="73034")}, tags=["Start me!"])
def start_pipeline():

    @task
    def write_postcode_to_airflow_variable(**context):
        from airflow.sdk import Variable

        Variable.set("us_postcode", context["params"]["us_postcode"])

    _write_postcode_to_airflow_variable = write_postcode_to_airflow_variable()

    @task(outlets=[Asset("start_noaa_etl")])
    def start_noaa_etl():
        print("NOAA ETL started using an Airflow Asset")

    _start_noaa_etl = start_noaa_etl()

    @task(outlets=[Asset("start_overture_etl")])
    def start_overture_etl():
        print("Overture ETL started using an Airflow Asset")

    _start_overture_etl = start_overture_etl()

    chain(_write_postcode_to_airflow_variable, [_start_noaa_etl, _start_overture_etl])


start_pipeline()
