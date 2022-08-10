""""
Три локации сериала "Рик и Морти" с наибольшим количеством резидентов.
"""

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator

import requests

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'k-povarov-11',
    'poke_interval': 600,
    'retries': 3,
    'retry_delay': 10,
    'priority_weight': 2
}

with DAG("dag_povarov_ram_location",
          schedule_interval=None,
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['povarov_dag_ram_location']
          ) as dag:

    start = DummyOperator(
        task_id='start'
    )

    def get_location_func():
        res = []
        r = requests.get('https://rickandmortyapi.com/api/location/')

        for page in range(1, r.json().get('info').get('pages') + 1):
            r = requests.get('https://rickandmortyapi.com/api/location/?page={}'.format(page))
            for i in range(len(r.json().get('results'))):
                id = r.json().get('results')[i].get('id')
                name = r.json().get('results')[i].get('name')
                type_ = r.json().get('results')[i].get('type')
                dimension = r.json().get('results')[i].get('dimension')
                resident_cnt = len(r.json().get('results')[i].get('residents'))
                res.append((id, name, type_, dimension, resident_cnt))

        top = sorted(res, key=lambda x: x[-1], reverse=True)[:3]
        result = ','.join(map(str, top))
        return result

    get_location = PythonOperator(
        task_id='get_location',
        python_callable=get_location_func,
        provide_context=True
    )

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='conn_greenplum',
        sql="""
            CREATE TABLE IF NOT EXISTS public.ram_top3_location
            (
                id integer PRIMARY KEY,
                name varchar,
                type varchar,
                dimension varchar,
                resident_cnt integer
            )
            DISTRIBUTED BY (id);
        """,
        autocommit=True
    )


    insert_values = PostgresOperator(
        task_id='insert_values',
        postgres_conn_id='conn_greenplum',
        sql = [
            'TRUNCATE TABLE  ram_top3_location',
            'INSERT INTO ram_top3_location VALUES {{ ti.xcom_pull(task_ids="get_location") }}',
        ],
        autocommit=True
    )

    end = DummyOperator(
        task_id='end'
    )

    start >> get_location >> create_table >> insert_values >> end

