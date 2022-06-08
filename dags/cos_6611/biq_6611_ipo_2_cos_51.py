import os
import socket
from datetime import datetime

# name_settings = os.path.basename(__file__).replace('.py','')
# settings = Variable.get(name_settings, deserialize_json=True)
# --Set Variable Dag
from operators.biq_6611.Postgres2SMB_cos import Postgres2SMB_cos
from operators.biq_6611.Postgres2Postgres_decrypt import Postgres2Postgres_decrypt
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.edgemodifier import Label

deployment = Variable.get("DEPLOYMENT")
TAG_DAG = ['6611', 'cos', 'ipo']
OBJ_FLOW = 51
EXEC_DATE = '{{ yesterday_ds }}'
if deployment == "DEV":
    CONN_IPO = 'IPO'
    CONN_source = 'IPO'  # Заменяем на имя коннектора системы источника
    SCHEMA_source = 'asup'
    CONN_target_smb = 'COS_SMB_dev'  # Заменяем на имя коннектора системы приемника
    SMB_share = 'Dev-fs01'  # Заменяем на адрес шары
    DIR_SMB = '/cos_bell'
    CONN_target_db = 'COS_DB_dev'
    SCHEMA_target = 'public'
    ST = datetime(2022, 5, 18)  # Заменяем на дату внедрения интеграции
    SI = None  # Заменяем на необходимый шедулинг
elif deployment == "PROD":  # Ниже заменяем на значения для прода
    CONN_IPO = 'IPO'
    CONN_source = 'IPO'
    SCHEMA_source = 'asup'
    CONN_target_smb = 'COS_SMB'
    SMB_share = '10.7.200.11'  # Заменяем на адрес шары
    DIR_SMB = '/cos'
    CONN_target_db = 'COS_DB'
    SCHEMA_target = 'public'
    ST = datetime(2022, 6, 15)  # Заменяем на дату внедрения интеграции
    SI = '0 0 * * *'

# объявление дага с журналированием
def task_success_log(context):
    log_end = PostgresOperator(
        task_id='log_end',
        postgres_conn_id=CONN_IPO,
        sql=f'''UPDATE ipo.list_flow_log
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
        sql=f'''update ipo.list_flow_log
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
    sleep = BashOperator(
        task_id='sleep',
        bash_command='sleep 5'
    )

    i = 1
    while i <= 2:

        log_sensor = SqlSensor(
            task_id='log_sensor' + str(i),
            conn_id=CONN_IPO,
            sql='./sql/log_sensor.sql',
            params={'object_flow': OBJ_FLOW}
        )
        log_start = PostgresOperator(
            task_id='log_start' + str(i),
            postgres_conn_id=CONN_IPO,
            sql='./sql/log_start.sql',
            params={'object_flow': OBJ_FLOW}
        )
        log_err_access_source = PostgresOperator(
            task_id='log_err_access_source' + str(i),
            postgres_conn_id=CONN_IPO,
            sql='./sql/log_err_access_source.sql'
        )
        log_err_access_target = PostgresOperator(
            task_id='log_err_access_target' + str(i),
            postgres_conn_id=CONN_IPO,
            sql='./sql/log_err_access_target.sql'
        )


        # проверка сетевого доступа
        def f_check_nwa_s(num):
            try:
                conn_s = BaseHook.get_connection(CONN_source)
                conn_host = conn_s.host
                conn_port = conn_s.port
                clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                clientsocket.connect((conn_host, conn_port))
                clientsocket.send(b'\n')
                return f'check_system_target{num}'
            except Exception:
                return f'log_err_access_source{num}'


        check_system_source = BranchPythonOperator(
            task_id='check_system_source' + str(i),
            python_callable=f_check_nwa_s,
            op_kwargs={'num': str(i)}
        )


        def f_check_nwa_t(num):
            try:
                if num == '1':
                    conn_s = BaseHook.get_connection(CONN_target_smb)
                    check_connect(conn_s)
                    return 'transfer_cos_smb'
                if num == '2':
                    conn_s = BaseHook.get_connection(CONN_target_db)
                    check_connect(conn_s)
                    return 'transfer_cos_db'
            except Exception:
                return f'log_err_access_target{num}'


        def check_connect(conn_s):
            conn_host = conn_s.host
            conn_port = conn_s.port
            clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            clientsocket.connect((conn_host, conn_port))
            clientsocket.send(b'\n')


        check_system_target = BranchPythonOperator(
            task_id='check_system_target' + str(i),
            python_callable=f_check_nwa_t,
            op_kwargs={'num': str(i)}
        )


        # Инструкция к интеграции
        def f_getid(**context):
            sql = f'''select id from ipo.list_flow_log
                                WHERE object_flow = {OBJ_FLOW} and status = 1; '''
            hook = PostgresHook(postgres_conn_id=CONN_IPO)
            records = hook.get_records(sql, parameters=None)
            row_id = records[0][0]
            context['task_instance'].xcom_push(key='row_id', value=row_id)


        getid = PythonOperator(
            task_id='getid' + str(i),
            python_callable=f_getid
        )

        if i == 1:
            transfer_cos_smb = Postgres2SMB_cos(
                task_id="transfer_cos_smb",
                postgres_conn_id=CONN_source,
                sql=f'''select to_char(now(), 'yyyymmdd') as "date", to_char(now(), 'hh24:mi:ss') as "time", PERNR, NACHN, VORNA, MIDNM, ORGEH, PLANS, PLTXT, CHPER, WERKS, BTRTL, GBDAT, PERSG, PERSK, FDATE, HDATE 
                        from {SCHEMA_source}.biq_6611_user_new_f('{EXEC_DATE}')''',
                headings=['DATE', 'TIME', 'PERNR', 'NACHN', 'VORNA', 'MIDNM', 'ORGEH', 'PLANS', 'PLTXT', 'CHPER', 'WERKS', 'BTRTL', 'GBDAT', 'PERSG', 'PERSK', 'FDATE', 'HDATE'],
                samba_conn_id=CONN_target_smb,
                share=SMB_share,
                dir_samba=DIR_SMB,
                file_name='user_new',
                file_format='xml',
                template_excel_xml='./template/xml/biq_6611/user_new.xml',
                encoding='utf-8',
                decrypt_col=['NACHN', 'VORNA', 'MIDNM', 'HDATE'],
                on_success_callback=task_success_log
            )

            log_sensor >> log_start >> getid >> check_system_source >> check_system_target >> transfer_cos_smb  # идеальный сценарий
            check_system_source >> Label(
                "no network access") >> log_err_access_source  # нет сетевого доступа к системе источника
            check_system_target >> Label(
                "no network access") >> log_err_access_target  # нет сетевого доступа к системе приемника

        if i == 2:
            transfer_cos_db = Postgres2Postgres_decrypt(
                task_id="transfer_cos_db",
                postgres_source_conn_id=CONN_source,
                sql=f'''select to_char(now(), 'yyyymmdd') as "date", to_char(now(), 'hh24:mi:ss') as "time", PERNR, NACHN, VORNA, MIDNM, ORGEH, PLANS, PLTXT, CHPER, WERKS, BTRTL, GBDAT, PERSG, PERSK, FDATE, HDATE 
                                        from {SCHEMA_source}.biq_6611_user_new_f('{EXEC_DATE}')''',
                postgres_dest_conn_id=CONN_target_db,
                dest_schema=SCHEMA_target,
                dest_table='sap_hr_user_new',
                dest_cols=['DATE', 'TIME', 'PERNR', 'NACHN', 'VORNA', 'MIDNM', 'ORGEH', 'PLANS', 'PLTXT', 'CHPER', 'WERKS', 'BTRTL', 'GBDAT', 'PERSG', 'PERSK', 'FDATE', 'HDATE'],
                decrypt_column=['NACHN', 'VORNA', 'MIDNM', 'HDATE'],
                on_success_callback=task_success_log,
                trunc=f'''TRUNCATE TABLE {SCHEMA_target}.sap_hr_user_new;'''
            )

            sleep >> log_sensor >> log_start >> getid >> check_system_source >> check_system_target >> transfer_cos_db  # идеальный сценарий
            check_system_source >> Label(
                "no network access") >> log_err_access_source  # нет сетевого доступа к системе источника
            check_system_target >> Label(
                "no network access") >> log_err_access_target  # нет сетевого доступа к системе приемника

        i = i + 1