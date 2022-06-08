import os
import socket
from datetime import datetime

from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.edgemodifier import Label

# name_settings = os.path.basename(__file__).replace('.py','')
# settings = Variable.get(name_settings, deserialize_json=True)
# --Set Variable Dag
from Postgres2SMB_decrypt import Postgres2SMB_decrypt

deployment = Variable.get("DEPLOYMENT")
TAG_DAG = ['6611', 'cos', 'ipo']
OBJ_FLOW = 53
if deployment == "DEV":
    CONN_IPO = 'IPO'
    # CONN_source = 'IPO'  # Заменяем на имя коннектора системы источника
    CONN_source = 'COS_db'  # Заменяем на имя коннектора системы источника
    SCHEMA_source = 'public'
    CONN_target_smb = 'COS_smb'  # Заменяем на имя коннектора системы приемника
    ST = datetime(2022, 4, 18)  # Заменяем на дату внедрения интеграции
    SI = None  # Заменяем на необходимый шедулинг
elif deployment == "PROD":  # Ниже заменяем на значения для прода
    CONN_IPO = 'IPO'
    CONN_source = 'IPO'
    SCHEMA_source = 'asup'
    CONN_target_smb = 'COS_smb'
    ST = datetime(2022, 6, 15)  # Заменяем на дату внедрения интеграции
    SI = '0 0 15 Dec *'


# объявление дага с журналированием
def task_success_log(context):
    log_end = PostgresOperator(
        task_id='log_end',
        postgres_conn_id=CONN_IPO,
        sql=f'''UPDATE list_flow_log
                        SET q_source={context['task_instance'].xcom_pull(key='q_rows')},
                        q_target={context['task_instance'].xcom_pull(key='q_rows')}, t_end=NOW(), status=2,
                        optional_key='{context['task_instance'].task_id}'
                        where id = {context['task_instance'].xcom_pull(key='row_id')};'''
    )
    return log_end.execute(context=context)


def task_failure_log(context):
    log_err_dag = PostgresOperator(
        task_id='log_err_dag',
        postgres_conn_id=CONN_IPO,
        sql=f'''update list_flow_log
                set t_end=NOW(), status=3, code_error=3, comment = '*Task*: {context['task_instance'].task_id}'
                where id = {context['task_instance'].xcom_pull(key='row_id')};'''
    )
    return log_err_dag.execute(context=context)


with DAG(
        os.path.basename(__file__),
        default_args={
            'owner': Variable.get("OWNER"),
            'retries': 0,
            'provide_context': True,  # permission to transmit xcom metadata
            'on_failure_callback': task_failure_log
        },
        start_date=ST,
        schedule_interval=SI,
        template_searchpath=[str(Variable.get("TMP_SP"))],
        tags=TAG_DAG,
        catchup=False  # run with start_date, меняем на true только в крайней необходимости
) as dag:

    log_sensor = SqlSensor(
        task_id='log_sensor',
        conn_id=CONN_IPO,
        sql='./sql/log_sensor.sql',
        params={'object_flow': OBJ_FLOW}
    )
    log_start = PostgresOperator(
        task_id='log_start',
        postgres_conn_id=CONN_IPO,
        sql='./sql/log_start.sql',
        params={'object_flow': OBJ_FLOW}
    )
    log_err_access_source = PostgresOperator(
        task_id='log_err_access_source',
        postgres_conn_id=CONN_IPO,
        sql='./sql/log_err_access_source.sql'
    )
    log_err_access_target = PostgresOperator(
        task_id='log_err_access_target',
        postgres_conn_id=CONN_IPO,
        sql='./sql/log_err_access_target.sql'
    )

    # log_end_no_data = PostgresOperator(
    #     task_id='log_end_no_data',
    #     postgres_conn_id=CONN_IPO,
    #     sql='./sql/log_end_no_status.sql',
    #     params={'comment': 'not_new_data'}
    # )
    #
    # def f_branching():
    #     sql = f'''select count(*) from {SCHEMA_source}.personal_section_inf_on_request_v;'''
    #     hook = PostgresHook(postgres_conn_id=CONN_IPO)
    #     records = hook.get_records(sql, parameters=None)
    #     qrows = records[0][0]
    #     if qrows >= 1:
    #         return 'transfer_cos_smb'
    #     else:
    #         return 'log_end_no_data'
    #
    # branching = BranchPythonOperator(
    #     task_id='branching',
    #     python_callable=f_branching)


    # проверка сетевого доступа
    def f_check_nwa_s():
        try:
            conn_s = BaseHook.get_connection(CONN_source)
            conn_host = conn_s.host
            conn_port = conn_s.port
            clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            clientsocket.connect((conn_host, conn_port))
            clientsocket.send(b'\n')
            return f'check_system_target'
        except Exception:
            return f'log_err_access_source'


    check_system_source = BranchPythonOperator(
        task_id='check_system_source',
        python_callable=f_check_nwa_s
    )


    def f_check_nwa_t():
        try:
            # conn_s = BaseHook.get_connection(CONN_target_smb)
            # check_connect(conn_s)
            return 'transfer_cos_smb'
        except Exception:
            return 'log_err_access_target'


    def check_connect(conn_s):
        conn_host = conn_s.host
        conn_port = conn_s.port
        clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        clientsocket.connect((conn_host, conn_port))
        clientsocket.send(b'\n')


    check_system_target = BranchPythonOperator(
        task_id='check_system_target',
        python_callable=f_check_nwa_t,
    )


    # Инструкция к интеграции
    def f_getid(**context):
        sql = f'''select id from list_flow_log
                        WHERE object_flow = {OBJ_FLOW} and status = 1; '''
        hook = PostgresHook(postgres_conn_id=CONN_IPO)
        records = hook.get_records(sql, parameters=None)
        row_id = records[0][0]
        context['task_instance'].xcom_push(key='row_id', value=row_id)


    getid = PythonOperator(
        task_id='getid',
        python_callable=f_getid
    )

    transfer_cos_smb = Postgres2SMB_decrypt(
        task_id="transfer_cos_smb",
        postgres_conn_id=CONN_source,
        sql=f'''select MOFID, DATE, HOLIDAY, TXT_LONG from {SCHEMA_source}.calendar_on_request_v;''',
        # samba_conn_id=CONN_target_smb,
        headings=['MOFID', 'DATE', 'HOLIDAY', 'TXT_LONG'],
        # share='',  # todo добавить шару
        dir_samba='/opt/airflow/result',  # todo добавить название папки в самбе
        file_name='calendar',
        file_format='xls',
        encoding='utf-8',
        decr_col=['MOFID', 'TXT_LONG'],
        on_success_callback=task_success_log
    )
    # log_sensor >> log_start >> getid >> check_system_source >> check_system_target >> branching >> transfer_cos_smb  # идеальный сценарий
    log_sensor >> log_start >> getid >> check_system_source >> check_system_target >> transfer_cos_smb  # идеальный сценарий
    check_system_source >> Label(
        "no network access") >> log_err_access_source  # нет сетевого доступа к системе источника
    check_system_target >> Label(
        "no network access") >> log_err_access_target  # нет сетевого доступа к системе приемника
    # branching >> Label("rows = 0") >> log_end_no_data #нет данных
