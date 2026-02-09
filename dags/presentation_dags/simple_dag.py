from airflow.sdk import dag, task, chain
from airflow.providers.standard.operators.bash import BashOperator
from pendulum import datetime


@dag(
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
)
def simple_dag():

    @task
    def get_manatee_joke():
        import requests

        url = "https://manateejokesapi.herokuapp.com/manatees/random"
        response = requests.get(url)
        data = response.json()
        return f"{data['setup']} - {data['punchline']}"

    _get_manatee_joke = get_manatee_joke()

    _print_joke = BashOperator(
        task_id="print_joke",
        bash_command=f"echo {_get_manatee_joke}",
    )

    chain(_get_manatee_joke, _print_joke)


simple_dag()


