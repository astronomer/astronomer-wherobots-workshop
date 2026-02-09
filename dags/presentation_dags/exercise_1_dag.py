from airflow.sdk import dag, task, chain, Param
from airflow.providers.standard.operators.bash import BashOperator
from pendulum import datetime


@dag(
    start_date=datetime(2026, 1, 1),
    schedule=None,
    params={"my_favorite_number": Param(type="integer", default=42)},
    tags=["Exercises", "syntax"]
)
def exercise_1_dag():

    @task
    def fetch_favorite_number(**context):
        my_favorite_number = context["params"]["my_favorite_number"]
        return my_favorite_number

    _fetch_favorite_number = fetch_favorite_number()

    @task
    def modify_favorite_number(my_favorite_number):
        return my_favorite_number * 2

    _modify_favorite_number = modify_favorite_number(_fetch_favorite_number)

    _print_modified_favorite_number = BashOperator(
        task_id="print_modified_favorite_number",
        bash_command=f"echo {_modify_favorite_number}",
    )

    ### EXERCISE 2 ### 
    # Add an additional task that squares the favorite number and prints it to the logs 
    # Make this task dependent on the fetch_favorite_number task

    chain(
        _fetch_favorite_number, _modify_favorite_number, _print_modified_favorite_number
    )


exercise_1_dag()
