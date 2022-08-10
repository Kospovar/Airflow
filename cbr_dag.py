from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

import csv
import xml.etree.ElementTree as ET
from datetime import datetime
from airflow.utils.dates import days_ago
import logging

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'k-povarov-11',
    'poke_interval': 600,
    'retries': 3,
    'retry_delay': 10,
    'priority_weight': 2
}

with DAG("dag_povarov_cbr",
          schedule_interval='0 18 * * 1-6',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['povarov_dag']
          ) as dag:

    def get_url_func(**kwargs):
        date = datetime.strptime(str(kwargs['ds']), "%Y-%m-%d").strftime("%d/%m/%Y/")
        url = 'https://www.cbr.ru/scripts/XML_daily.asp?date_req={}'.format(date)
        bash_command ='curl {} | iconv -f Windows-1251 -t UTF-8 > /tmp/cbr_povarov.xml'.format(url)
        return bash_command

    get_url = PythonOperator(
        task_id='get_url',
        python_callable=get_url_func,
        provide_context=True
    )

    script='''
    {{ ti.xcom_pull(task_ids='get_url') }}
    '''

    load_cbr_xml = BashOperator(
        task_id='load_cbr_xml',
        bash_command=script
    )

    def export_xml_to_csv_func(**kwargs):

        parser = ET.XMLParser(encoding="UTF-8")
        tree = ET.parse('/tmp/cbr_povarov.xml', parser=parser)
        root = tree.getroot()

        with open('/tmp/cbr_povarov.csv', 'w') as csv_file:
            writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for Valute in root.findall('Valute'):
                NumCode = Valute.find('NumCode').text
                CharCode = Valute.find('CharCode').text
                Nominal = Valute.find('Nominal').text
                Name = Valute.find('Name').text
                Value = Valute.find('Value').text.replace(',', '.')
                Date = root.attrib['Date']
                ID = Valute.attrib['ID']
                writer.writerow([Date] + [ID] + [NumCode] + [CharCode] + [Nominal] + [Name] + [Value])

                logging.info([Date] + [ID] + [NumCode] + [CharCode] + [Nominal] + [Name] + [Value])

        return root.attrib['Date']

    export_xml_to_csv = PythonOperator(
        task_id='export_xml_to_csv',
        python_callable=export_xml_to_csv_func,
        provide_context=True
    )

    text_info = DummyOperator(
        task_id='text_info'
    )

    def chek_non_working_day_func(**kwargs):

        ds_to_csv = str(kwargs['templates_dict']['implicit'])
        ds = str(kwargs['ds'])

        logging.info(ds_to_csv)
        logging.info(ds)

        execution_date = datetime.strptime(ds, "%Y-%m-%d").strftime("%d/%m/%Y/")
        date = datetime.strptime(ds_to_csv, "%d.%m.%Y").strftime("%d/%m/%Y/")

        if date == execution_date:
            return ['truncate_table', 'load_csv_to_gp']
        else:
            logging.info('----------------------------')
            logging.info('Сегодня нет данных от ЦБР')
            logging.info('----------------------------')
            return 'text_info'

    chek_non_working_day = PythonOperator(
        task_id='chek_non_working_day',
        python_callable=chek_non_working_day_func,
        templates_dict={'implicit': '{{ ti.xcom_pull(task_ids="export_xml_to_csv") }}'},
        provide_context=True
    )

    def truncate_table_func():
        pg_hook = PostgresHook(
            postgres_conn_id='conn_greenplum'
        )
        pg_hook.run('TRUNCATE TABLE  k_pov_cbr')


    truncate_table = PythonOperator(
        task_id='truncate_table',
        python_callable=truncate_table_func
    )

    def load_csv_to_gp_func():
        pg_hook = PostgresHook(
            postgres_conn_id='conn_greenplum'
        )
        pg_hook.copy_expert("COPY k_pov_cbr  FROM STDIN DELIMITER ','", '/tmp/cbr_povarov.csv')

    load_csv_to_gp = PythonOperator(
        task_id='load_csv_to_gp',
        python_callable=load_csv_to_gp_func
    )

    get_url >> load_cbr_xml >> export_xml_to_csv >> chek_non_working_day
    chek_non_working_day >> truncate_table >> load_csv_to_gp
    chek_non_working_day >> text_info