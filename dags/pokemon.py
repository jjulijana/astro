from airflow.decorators import dag, task
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

from datetime import datetime
import requests
import random

with DAG(
    dag_id="poke_dag",
    start_date=datetime(2024, 4, 6),
    schedule_interval="@daily",
    catchup=False,
    default_args={"owner": "Astro", "retries": 0},
    tags=["pokeapi"],
    doc_md=__doc__,
    render_template_as_native_obj=True,
    params={
        "n" : 5
    }
) as poke_dag:

    @task
    def get_all_pokemons() -> list:
        count = requests.get("https://pokeapi.co/api/v2/pokemon").json()["count"]

        response = requests.get(f'https://pokeapi.co/api/v2/pokemon?limit={count}')
        results = response.json()["results"]

        print(f"Total number of pokemons: {count}")
        print("List of pokemons:")
        for result in results:
            print(result['name'])

        return [result['name'] for result in results]


    @task
    def get_pokemon_sample(pokemons: list[str], n: int = 5) -> list[str]:
        return random.sample(pokemons, n)
    

    pokemons = get_all_pokemons()
    pokemon_sample = get_pokemon_sample(pokemons, "{{ params.n }}")


    @task
    def get_poke_info(name: str) -> dict:
        poke = {}
        response = requests.get(f'https://pokeapi.co/api/v2/pokemon/{name}').json()

        poke['name'] = name
        poke['id'] = response['id']
        abilities_data = response['abilities']
        abilities = [ability_info['ability']['name'] for ability_info in abilities_data]
        poke['abilities'] = abilities
        types_data = response['types']
        types = [type['type']['name'] for type in types_data]
        poke['types'] = types
        poke['xp'] = response['base_experience']
        stats_data = response['stats']
        stats = {stat['stat']['name']: stat['base_stat'] for stat in stats_data}
        poke['stats'] = stats

        print(f"Information for Pokémon {name}:")
        print(poke)

        return poke


    poke_info = get_poke_info.expand(name=pokemon_sample)


    # save_to_file_command = """
    # echo '{{ task_instance.xcom_pull(task_ids="get_all_pokemons") }}' > $AIRFLOW_HOME/output/pokemon_data.json
    # if [ $? -eq 0 ]; then
    #     echo "Pokémon data saved to file."
    # else
    #     echo "Error saving Pokémon data to file."
    # fi
    # """


    # get_pokemons_task = PythonOperator(
    #     task_id="get_all_pokemons",
    #     python_callable=get_all_pokemons
    # )

    # with TaskGroup("parallel_pokemon_tasks") as parallel_pokemon_tasks:
    #     for i in range(5):
    #         random_pokemon_task_id = f"get_random_pokemon_{i+1}"
    #         get_random_pokemon_task = PythonOperator(
    #             task_id=random_pokemon_task_id,
    #             python_callable=lambda: random.choice(get_all_pokemons())
    #         )
        
    #         get_pokemon_info_task_id = f"get_pokemon_info_{i+1}"
    #         get_pokemon_info_task = PythonOperator(
    #             task_id=get_pokemon_info_task_id,
    #             python_callable=get_poke_info,
    #             op_kwargs={'name': "{{ task_instance.xcom_pull(task_ids='" + random_pokemon_task_id + "') }}"},
    #             provide_context=True,
    #         )
            
    #         get_random_pokemon_task >> get_pokemon_info_task


    # save_to_file_task = BashOperator(
    #     task_id="save_to_file",
    #     bash_command=save_to_file_command
    # )
    

    # get_pokemons_task >> parallel_pokemon_tasks >> save_to_file_task