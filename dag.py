from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
import datetime as dt

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

default_args = {
    'owner': 'k-povarov-7',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 6, 13),
}

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator'
}

test_connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': '656e2b0c9c',
    'user': 'student-rw',
    'database': 'test'
}

schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_sim_example():

    @task()
    def extract_feed():
        query = '''select
                            toDate(time) as event_day,
                            user_id,
                            countIf(action = 'like') likes,
                            countIf(action = 'view') views,
                            case when gender = 1 then 'male' else 'female' end gender,
                            os,
                            case 
                                when age < 18 then '<18'
                                when age between 18 and 25 then '18-25'
                                when age between 25 and 35 then '25-35'
                                when age between 35 and 45 then '35-45'
                                else '>45'
                                end age
                        from simulator_20220520.feed_actions
                        where toDate(time) = yesterday()
                        group by user_id, 
                                 gender,
                                 age,
                                 os,
                                 event_day'''
        df_feed = ph.read_clickhouse(query, connection=connection)
        return df_feed
        
    @task()
    def extract_message():
        query = '''select * from 
                        (select 
                            user_id,
                            count(user_id) as messages_sent,
                            count(distinct user_id) as users_sent,
                            case when gender = 1 then 'male' else 'female' end gender,
                            os,
                            case 
                                when age < 18 then '<18'
                                when age between 18 and 25 then '18-25'
                                when age between 25 and 35 then '25-35'
                                when age between 35 and 45 then '35-45'
                                else '>45'
                                end age
                        from simulator_20220520.message_actions 
                        where toDate(time) = yesterday()
                        group by user_id,gender, os, age
                        ) t1

                        full join

                        (select 
                            reciever_id as user_id,
                            count(reciever_id) as messages_received,
                            count(distinct reciever_id) as users_received,
                            case when gender = 1 then 'male' else 'female' end gender,
                            os,
                            case 
                                when age < 18 then '<18'
                                when age between 18 and 25 then '18-25'
                                when age between 25 and 35 then '25-35'
                                when age between 35 and 45 then '35-45'
                                else '>45'
                                end age
                        from simulator_20220520.message_actions 
                        where toDate(time) = yesterday()
                        group by reciever_id,gender, os, age
                        ) t2 
                    using(user_id, age,os, gender)'''
        df_message = ph.read_clickhouse(query, connection=connection)
        return df_message
    
    @task()
    def merge_tabels(df_feed, df_message):
        df = pd.merge(df_feed, df_message, how='outer', on=['user_id','age','os', 'gender'])
        return df
    
    @task()
    def total_metrics(df, list_metric=['gender','age','os']):
        
        t = pd.DataFrame()

        for metric in list_metric:
            res = df.groupby(metric).agg({'messages_sent': 'sum',
                               'users_sent': 'sum',
                               'messages_received': 'sum',
                               'users_received': 'sum',
                               'views': 'sum',
                               'likes': 'sum'}).reset_index().rename(columns={metric:'metric_value'})
            res['event_date'] = dt.datetime.today().strftime("%Y/%m/%d")
            t = pd.concat([t, res])
        return t
    
    @task()
    def final_data(t):
        df = t[['event_date', 'metric_value', 'views', 'likes', 'messages_received', 
                'messages_sent', 'users_received', 'users_sent'
               ]]
    
        df = df.astype({'views':'int64',
                        'likes':'int64',
                        'messages_received':'int64',
                        'messages_sent':'int64',
                        'users_received':'int64',
                        'users_sent':'int64'})
        return df
    
    @task
    def create_table(df):
        query = '''CREATE TABLE IF NOT EXISTS test.airflow_povarov
        (event_date Date,
         metric_value String,
         views Int64,
         likes Int64,
         messages_received Int64,
         messages_sent Int64,
         users_received Int64,
         users_sent Int64
         ) ENGINE = MergeTree'''
        ph.execute(connection=test_connection, query = query)
        ph.to_clickhouse(df=df, table='airflow_povarov', index=False, connection=test_connection)
    
    
    df_feed = extract_feed()
    df_message = extract_message()
    df = merge_tabels(df_feed, df_message)
    t = total_metrics(df)
    df = final_data(t)
    create_table(df)

dag_sim_example = dag_sim_example()