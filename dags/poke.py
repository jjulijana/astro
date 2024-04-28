from airflow.decorators import dag, task
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime
import requests
import random

with DAG(
    dag_id="poke",
    start_date=datetime(2024, 4, 6),
    schedule_interval="@daily",
    catchup=False,
    default_args={"owner": "Astro", "retries": 3},
    tags=["poke"],
    doc_md=__doc__,
) as poke_dag:

    def get_all_pokemons() -> list:
        count = requests.get("https://pokeapi.co/api/v2/pokemon").json()["count"]

        response = requests.get(f'https://pokeapi.co/api/v2/pokemon?limit={count}')
        results = response.json()["results"]

        print(f"Total number of pokemons: {count}")
        print("List of pokemons:")
        for result in results:
            print(result['name'])

        return [result['name'] for result in results]

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

    save_to_file_command = """
    echo '{{ task_instance.xcom_pull(task_ids="get_pokemon_info") }}' > $AIRFLOW_HOME/output/pokemon_data.json
    if [ $? -eq 0 ]; then
        echo "Pokémon data saved to file."
    else
        echo "Error saving Pokémon data to file."
    fi
    """


    get_pokemons_task = PythonOperator(
        task_id="get_all_pokemons",
        python_callable=get_all_pokemons
    )

    get_random_pokemon_task = PythonOperator(
        task_id="get_random_pokemon",
        python_callable=lambda: random.choice(get_all_pokemons())
    )

    get_pokemon_info_task = PythonOperator(
        task_id="get_pokemon_info",
        python_callable=get_poke_info,
        op_kwargs={'name': "{{ task_instance.xcom_pull(task_ids='get_random_pokemon') }}"}
    )

    save_to_file_task = BashOperator(
        task_id="save_to_file",
        bash_command=save_to_file_command
    )
    

    get_pokemons_task >> get_random_pokemon_task >> get_pokemon_info_task >> save_to_file_task