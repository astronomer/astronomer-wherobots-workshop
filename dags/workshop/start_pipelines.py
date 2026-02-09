from airflow.sdk import dag, task, Param, Asset, chain


@dag(params={"us_postcode": Param(type="string", default="73034")}, tags=["Start me!"])
def start_pipeline():

    @task
    def write_postcode_to_airflow_variable(**context):
        from airflow.sdk import Variable

        Variable.set("us_postcode", context["params"]["us_postcode"])

    _write_postcode_to_airflow_variable = write_postcode_to_airflow_variable()

    @task(outlets=[Asset("start_database_setup")])
    def start_database_setup():
        print("Database setup started using an Airflow Asset")

    _start_database_setup = start_database_setup()

    chain(_write_postcode_to_airflow_variable, _start_database_setup)


start_pipeline()
