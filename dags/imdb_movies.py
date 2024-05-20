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
    template_searchpath="/usr/local/airflow/include/sql",
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
            
    @task_group(group_id='data_cleaning', tooltip='Data Cleaning Tasks')
    def data_cleaning_group(source_table: str, target_table: str, schema_name: str):

        drop_duplicates = PostgresOperator(
            task_id='drop_duplicates',
            postgres_conn_id='postgres',
            sql='drop_duplicates.sql',
            params={ 'schema': schema_name,
                'target_table': target_table,
                'source_table': source_table }
        )

        drop_columns = PostgresOperator(
            task_id='drop_columns',
            postgres_conn_id='postgres',
            sql='drop_columns.sql',
            params={ 'schema': schema_name,
                'table_name': target_table, 
                'columns_to_drop' : ['facenumber_in_poster'] }
        )

        remove_null_terminating_char = PostgresOperator(
            task_id='remove_null_terminating_char',
            postgres_conn_id='postgres',
            sql='remove_null_terminating_char.sql',
            params={ 'schema': schema_name,
                'table_name': target_table }
        )
        
        drop_duplicates >> drop_columns >> remove_null_terminating_char

        return target_table

    cleaned_movies = data_cleaning_group("movie_metadata", "cleaned_movies", "imdb")

movie_data_pipeline_dag = movie_data_pipeline()
