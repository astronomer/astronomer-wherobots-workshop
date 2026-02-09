from airflow.sdk import dag, Asset, task


@dag(tags=["syntax"])
def my_etl_dag():

    @task(outlets=[Asset("data_ready")])
    def load():
        print("Loading data...")

    load()


my_etl_dag()


@dag(schedule=[Asset("data_ready")], tags=["syntax"])
def my_ml_dag():

    @task
    def empty_task():
        print("Empty task")

    empty_task()


my_ml_dag()
