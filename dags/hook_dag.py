from datetime import datetime, timedelta

from Postgres2File import Postgres2File
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator, ShortCircuitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.sql import SqlSensor

default_args = {
    'owner': 'airflow',
    'depend_on_post': False,
    'start_date': datetime(2022, 4, 5, 10, 00, 00),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


def get_activated_sources():
    request = "SELECT name, owner FROM pet"
    pg_hook = PostgresHook(postgre_conn_id="postgres_default", schema="airflow")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    sources = cursor.fetchall()
    for source in sources:
        print("Source: {0} - activated: {1}".format(source[0], source[1]))
    return sources


def check_date():
    return datetime.now().day == 20


with DAG('hook_dag',
         default_args=default_args,
         schedule_interval='@once',
         catchup=False) as dag:
    start_task = DummyOperator(task_id='start_task')
    hook_task = PythonOperator(task_id='hook_task', python_callable=get_activated_sources)

    [log_sensor_1, log_sensor_2] = [SqlSensor(
        task_id=f"log_sensor_{i}",
        conn_id="postgres_default",
        sql='sql/log_sensor.sql',
        params={'object_flow': 1}
    ) for i in [1, 2]]

    [log_start_1, log_start_2] = [PostgresOperator(
        task_id=f"log_start_{i}",
        postgres_conn_id="postgres_default",
        sql='./sql/log_start.sql',
        params={'object_flow': 1}
    ) for i in [1, 2]]


    def f_getid(**context):
        sql = f'''select id from list_flow_log
                WHERE object_flow = 1 and status = 1; '''
        hook = PostgresHook(postgres_conn_id="postgres_default", schema='airflow')
        records = hook.get_records(sql, parameters=None)
        row_id = records[0][0]
        context['task_instance'].xcom_push(key='row_id', value=row_id)


    [getid_1, getid_2] = [PythonOperator(
        task_id=f"getid_{i}",
        python_callable=f_getid
    ) for i in [1, 2]]

    [log_end_no_data_1, log_end_no_data_2] = [PostgresOperator(
        task_id=f'log_end_no_data_{i}',
        postgres_conn_id="postgres_default",
        sql='./sql/log_end_no_status.sql',
        params={'comment': 'not_new_data'}
    ) for i in [1, 2]]


    # объявление дага с журналированием
    def task_success_log(context):
        log_end = PostgresOperator(
            task_id='log_end',
            postgres_conn_id="postgres_default",
            sql=f'''update list_flow_log
                set t_end=NOW(), status=2, comment = '*Task*: {context['task_instance'].task_id} success'
                where id = {context['task_instance'].xcom_pull(key='row_id')};'''
        )
        return log_end.execute(context=context)


    transfer_suit_pa = Postgres2File(
        task_id='transfer_suit_pa',
        postgres_conn_id="postgres_default",
        sql=f'''SELECT name, owner FROM pet''',
        headings=['name', 'owner'],
        file_name='pet',
        file_format='xlsx',
        on_success_callback=task_success_log,  # журналирование
    )

    transfer_xml = Postgres2File(
        task_id='transfer_xml',
        postgres_conn_id="postgres_default",
        sql=f'''SELECT name, owner FROM pet''',
        template_xml="./template/orgunit_new.xml",
        file_name='orgunit_new',
        file_format='xml',
        on_success_callback=task_success_log,  # журналирование
    )


    def f_branching(**context):
        id_s = context['task_instance'].xcom_pull(key='dest_ids')
        qrows = 2
        if qrows >= 1:
            return 'transfer_suit_pa'
        else:
            return 'log_end_no_data_2'


    def f_branching_xml(**context):
        id_s = context['task_instance'].xcom_pull(key='dest_ids')
        qrows = 2
        if qrows >= 1:
            return 'transfer_xml'
        else:
            return 'log_end_no_data_1'

    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=f_branching,
    )

    branching_xml = BranchPythonOperator(
        task_id='branching_xml',
        python_callable=f_branching_xml,
    )

    cond_true = ShortCircuitOperator(
        task_id='cond_true',
        python_callable=check_date
    )

    [start_task, hook_task, log_sensor_1, cond_true]

    log_sensor_1 >> log_start_1 >> getid_1 >> branching_xml >> [transfer_xml, log_end_no_data_1]
    cond_true >> log_sensor_2 >> log_start_2 >> getid_2 >> branching >> [transfer_suit_pa, log_end_no_data_2]
