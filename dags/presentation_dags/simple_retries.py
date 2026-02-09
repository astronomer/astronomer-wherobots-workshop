from airflow.sdk import dag, task 
from datetime import timedelta

def on_failure_callback(context):
    print("HELLO FROM ON FAILURE CALLBACK")
    print(f"Task {context['task_instance'].task_id} failed")
    print(f"Error: {context['exception']}")


@dag(
    schedule=None,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=10),
        # "retry_exponential_backoff": True,
        "on_failure_callback": on_failure_callback,
        "execution_timeout": timedelta(minutes=30),
    },
    max_consecutive_failed_dag_runs=10,
    dagrun_timeout=timedelta(hours=4),
    tags=["syntax"],
)
def simple_retries():

    @task
    def failing_task():
        raise Exception("This task always fails")

    failing_task()

simple_retries()