from airflow.providers.standard.operators.hitl import HITLOperator
from airflow.sdk import dag, task, chain, Param
from datetime import timedelta

@dag(tags=["syntax"])
def simple_hitl():

    @task
    def upstream_task():
        return "Choose wisely."

    _upstream_task = upstream_task()

    _hitl_task = HITLOperator(
        task_id="hitl_task",
        subject="Choose your starter pokemon",  # templatable
        body="{{ ti.xcom_pull(task_ids='upstream_task') }}", # templatable
        options=["Bulbasaur", "Charmander", "Squirtle"],  # cannot be empty!
        defaults=["Charmander"],
        params={
            "Reason": Param(type="string", default="..."),
        },
    )

    @task 
    def print_result(hitl_output):
        print(f"You chose: {hitl_output['chosen_options']}")
        print(f"Reason: {hitl_output['params_input']['Reason']}")

    _print_result = print_result(_hitl_task.output)

    chain(_upstream_task, _hitl_task, _print_result)

simple_hitl()
