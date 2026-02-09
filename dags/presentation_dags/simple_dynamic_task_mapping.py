from airflow.sdk import dag, task


@dag(tags=["syntax"])
def simple_dynamic_task_mapping():

    @task
    def get_numbers():
        import random

        return random.sample(range(1, 100), random.randint(1, 10))

    @task
    def process_number(num, exponent):
        return num**exponent

    _get_numbers = get_numbers()
    process_number.partial(exponent=2).expand(num=_get_numbers)


simple_dynamic_task_mapping()
