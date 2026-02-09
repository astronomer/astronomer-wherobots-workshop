from airflow.sdk import dag, task, chain, Asset
from airflow.providers.standard.operators.bash import BashOperator
@dag(schedule=[Asset("test_asset")])
def test():


    test_jinja = BashOperator(
        task_id="test_jinja",
        bash_command="echo {{ dag_run.run_after.strftime('%Y%m%dT%H%M%S') }}",
    )

test()