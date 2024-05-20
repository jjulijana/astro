from airflow.decorators import dag, task, task_group
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

@dag(
    dag_id="movie_data_pipeline",
    default_args={"owner": "Astro", "retries": 0},
    description="A DAG to process movie data",
    schedule_interval="@daily",
    start_date=datetime(2024, 4, 6),
    tags=["movie_data"],
    catchup=False
)
def movie_data_pipeline():

    @task
    def load_movie_data(csv = "movie_metadata.csv", table_name = "movie_metadata", postgres_conn_id = "postgres"):
        hook = PostgresHook.get_hook(postgres_conn_id)
        # engine = create_engine(hook.get_uri())
        engine = hook.get_sqlalchemy_engine()
        df = pd.read_csv(f"/usr/local/airflow/include/seed/{csv}", sep=",", index_col=False)
        print("<<<<<<<<<<start migrating data>>>>>>>>>>>>>>")
        connection = engine.connect()
        df.to_sql(table_name, con=engine, schema="imdb", if_exists='replace', index=False)
        connection.close()
        # with engine.connect() as conn:
        #     df.to_sql(table_name, con=conn, schema="imdb", if_exists='replace', index=False)
        print("<<<<<<<<<<<<<<<<<<<completed>>>>>>>>>>>>>>>>")
        return table_name
            
    movie_metadata = load_movie_data()

movie_data_pipeline_dag = movie_data_pipeline()
