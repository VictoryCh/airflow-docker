import logging
import os
import socket
from datetime import datetime

# name_settings = os.path.basename(__file__).replace('.py','')
# settings = Variable.get(name_settings, deserialize_json=True)
# --Set Variable Dag
from operators.biq_6611.Postgres2Postgres_decrypt import Postgres2Postgres_decrypt
from Postgres2SMB_decrypt import Postgres2SMB_decrypt
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.edgemodifier import Label

deployment = Variable.get("DEPLOYMENT")
TAG_DAG = ['6611', 'cos', 'ipo']
OBJ_FLOW = 55
EXEC_DATE = '{{ yesterday_ds }}'
if deployment == "DEV":
    CONN_IPO = 'IPO'
    CONN_source = 'IPO'  # Заменяем на имя коннектора системы источника
    CONN_source2 = 'COS_DB'  # Заменяем на имя коннектора системы источника
    SCHEMA_source = 'public'
    CONN_target = 'COS_DB_dev'  # Заменяем на имя коннектора системы приемника
    SCHEMA_target = 'public'
    ST = datetime(2021, 6, 16)  # Заменяем на дату внедрения интеграции
    SI = '28 12 16 Jun *'  # Заменяем на необходимый шедулинг
elif deployment == "PROD":  # Ниже заменяем на значения для прода
    CONN_IPO = 'IPO'
    CONN_source = 'IPO'
    SCHEMA_source = 'asup'
    CONN_target = 'COS_DB'
    SCHEMA_target = 'public'
    ST = datetime(2022, 6, 15)  # Заменяем на дату внедрения интеграции
    SI = '0 0 * * *'

SQL = f'''select FDATE, TIME, PERNR, NACHN, VORNA, MIDNM, ORGEH, PLANS, PLTXT, CHPER, WERKS, BTRTL, GBDAT
                from {SCHEMA_source}.personal_data_actual'''

# объявление дага с журналированием
def task_success_log(context):
    log_end = PostgresOperator(
        task_id='log_end',
        postgres_conn_id=CONN_IPO,
        sql=f'''UPDATE list_flow_log
                        SET q_source={context['task_instance'].xcom_pull(key='full_qr')},
                        q_target={context['task_instance'].xcom_pull(key='full_qr')}, t_end=NOW(), status=2,
                        comment='{context['task_instance'].xcom_pull(key='full_comment')}'
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


    # проверка сетевого доступа
    def f_check_nwa_s():
        try:
            conn_s = BaseHook.get_connection(CONN_source)
            check_connect(conn_s)
            return f'check_system_target'
        except Exception:
            return f'log_err_access_source'


    check_system_source = BranchPythonOperator(
        task_id='check_system_source',
        python_callable=f_check_nwa_s
    )


    def f_check_nwa_t():
        try:
            conn_s = BaseHook.get_connection(CONN_target)
            check_connect(conn_s)
            return 'transfer_sensor'
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

    transfer_sensor = SqlSensor(
        task_id='transfer_sensor',
        conn_id=CONN_target,
        sql='./sql/biq_6611/transfer_sensor.sql',
        params={
            'object_flow': OBJ_FLOW,
            'schema': SCHEMA_target
        },
        poke_interval=20,
        timeout=60 * 5,
        mode='reschedule'
    )

    transfer_start = PostgresOperator(
        task_id='transfer_start',
        postgres_conn_id=CONN_target,
        sql='./sql/biq_6611/transfer_start.sql',
        params={
            'object_flow': OBJ_FLOW,
            'schema': SCHEMA_target
        }
    )

    def f_get_semaphore_id(**context):
        sql = f'''select id from {SCHEMA_target}.sap_hr_semaphore WHERE object_flow = {OBJ_FLOW} and status = 1; '''
        hook = PostgresHook(postgres_conn_id=CONN_target)
        records = hook.get_records(sql, parameters=None)
        row_id = records[0][0]
        context['task_instance'].xcom_push(key='cos_semaphore_id', value=row_id)


    get_semaphore_id = PythonOperator(
        task_id='get_semaphore_id',
        python_callable=f_get_semaphore_id
    )

    transfer_cos = Postgres2Postgres_decrypt(
        task_id="transfer_cos",
        postgres_source_conn_id=CONN_source,
        sql=f'''select objid, delflag, orgname, orgbegda, orgendda, orglow, shortname, objid_par1, lname, werks, btrtl 
                from {SCHEMA_source}.organizational_unit_actual_f''',
        postgres_dest_conn_id=CONN_target,
        dest_schema=SCHEMA_target,
        dest_table='sap_hr_organizational_unit_actual',
        dest_cols=['objid', 'delflag', 'orgname', 'orgbegda', 'orgendda', 'orglow', 'shortname', 'objid_par1', 'lname', 'werks', 'btrtl'],
        trunc=f'''TRUNCATE TABLE {SCHEMA_target}.sap_hr_organizational_unit_actual;'''
    )

    transfer_cos_smb_xlsx = Postgres2SMB_decrypt(
        task_id="transfer_cos_smb_xlsx",
        postgres_conn_id=CONN_source2,
        sql=SQL,
        # samba_conn_id=CONN_target_smb,
        headings=['Дата', 'Время', 'Табельный', 'Фамилия', 'VORNA', 'MIDNM', 'ORGEH', 'PLANS', 'PLTXT', 'CHPER', 'WERKS', 'BTRTL', 'Дата рождения'],
        # share='/opt',  # todo добавить шару
        dir_samba='/opt/airflow/result',  # todo добавить название папки в самбе
        file_name='user_new',
        file_format='xlsx',
        encoding='utf-8',
        decrypt_col=['NACHN', 'VORNA', 'MIDNM', 'GBDAT'],
    )

    def f_transfer_status_update(**context):
        error = context['task_instance'].xcom_pull(key='transferFailure')
        if error:
            sql = f'''update {SCHEMA_target}.sap_hr_semaphore
                set t_end=NOW(), status=3, comment = 'Error message: {context['task_instance'].xcom_pull(key='exception')}'
                where id = {context['task_instance'].xcom_pull(key='cos_semaphore_id')};'''
        else:
            sql = f'''UPDATE {SCHEMA_target}.sap_hr_semaphore
                        SET q_source={context['task_instance'].xcom_pull(key='full_qr')},
                        q_target={context['task_instance'].xcom_pull(key='full_qr')}, t_end=NOW(), status=2,
                        comment='*Success*: {context['task_instance'].xcom_pull(key='full_comment')}'
                        where id = {context['task_instance'].xcom_pull(key='cos_semaphore_id')};'''
        hook = PostgresHook(postgres_conn_id=CONN_target)
        hook.run(sql, parameters=None)


    update_cos_status_transfer_finish = PythonOperator(
        task_id='update_cos_status_transfer_finish',
        python_callable=f_transfer_status_update,
        trigger_rule='none_skipped',
        on_success_callback=task_success_log
    )

    log_sensor >> log_start >> getid >> check_system_source >> check_system_target >> transfer_sensor >> transfer_start >> \
    get_semaphore_id >> transfer_cos >> transfer_cos_smb_xlsx >> update_cos_status_transfer_finish  # идеальный сценарий
    check_system_source >> Label(
        "no network access") >> log_err_access_source  # нет сетевого доступа к системе источника
    check_system_target >> Label(
        "no network access") >> log_err_access_target  # нет сетевого доступа к системе приемника
