from airflow import DAG
from datetime import date, datetime, timedelta
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.utils.edgemodifier import Label
from airflow.hooks.base_hook import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.sensors.sql import SqlSensor
import os
import socket

from Postgres2Postgres import Postgres2Postgres

# name_settings = os.path.basename(__file__).replace('.py','')
# settings = Variable.get(name_settings, deserialize_json=True)
# --Set Variable Dag

# deployment = Variable.get("DEPLOYMENT")
TAG_DAG = ['2295', 'cso', 'ipo']
OBJ_FLOW = 0
CONN_IPO = 'postgres_default'
CONN_source = 'postgres_default'
CONN_target = 'postgres_host'
SCHEMA_target = 'public'
ST = datetime(2022, 4, 20)
SI = '41 15 * * *'


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
        # необходима доработка: заменить null на значения
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
            # 'owner': Variable.get("OWNER"),
            # 'retries': 0,
            # 'provide_context': True,  # permission to transmit xcom metadata
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
    while i <= 1:

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


        # log_end_no_data = PostgresOperator(
        #     task_id='log_end_no_data' + str(i),
        #     postgres_conn_id=CONN_IPO,
        #     sql='./sql/log_end_no_status.sql',
        #     params={'comment': 'not_new_data'}
        # )

        # проверка сетевого доступа
        def f_check_nwa_s(num):
            try:
                # conn_s = BaseHook.get_connection(CONN_source)
                # conn_host = conn_s.host
                # conn_port = conn_s.port
                # clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                # clientsocket.connect((conn_host, conn_port))
                # clientsocket.send(b'\n')
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
                # conn_s = BaseHook.get_connection(CONN_target)
                # conn_host = conn_s.host
                # conn_port = conn_s.port
                # clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                # clientsocket.connect((conn_host, conn_port))
                # clientsocket.send(b'\n')
                if num == '1':
                    return 'transfer_cso_org'
                # if num == '2':
                #     return 'transfer_cso_pers'
            except Exception:
                return f'log_err_access_target{num}'


        check_system_target = BranchPythonOperator(
            task_id='check_system_target' + str(i),
            python_callable=f_check_nwa_t,
            op_kwargs={'num': str(i)}
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
            task_id='getid' + str(i),
            python_callable=f_getid
        )

        if i == 1:
            transfer_cso_org = Postgres2Postgres(
                task_id="transfer_cso_org",
                postgres_source_conn_id = CONN_source,
                sql=f'''select to_char(now(), 'yyyymmdd') as "date",to_char(now(), 'hh24:mi:ss') as "time", WERKS, 
                BTRTL, objid as ORGEH, LNAME, orgname as SNAME, shortname as ABBRV, OPRNT, chperoe as CHPER, 
                orglow as ZTYPE, orgendda as SENDD from organizational_unit_actual_f;''',
                # todo поправить таблицу с данными
                postgres_dest_conn_id = CONN_target,
                dest_schema=SCHEMA_target,
                dest_table='organizational_unit_actual_f',
                dest_cols=['date', 'time', 'werks', 'btrtl', 'orgeh',
                           'lname', 'sname', 'abbrv', 'oprnt', 'chper', 'ztype', 'sendd'],
                on_success_callback=task_success_log,
                trunc=f'''TRUNCATE TABLE {SCHEMA_target}.organizational_unit_actual_f;'''
            )
            log_sensor >> log_start >> getid >> check_system_source >> check_system_target >> transfer_cso_org  # идеальный сценарий
            check_system_source >> Label(
                "no network access") >> log_err_access_source  # нет сетевого доступа к системе источника
            check_system_target >> Label(
                "no network access") >> log_err_access_target  # нет сетевого доступа к системе приемника
        # if i == 2:
        #     transfer_cso_pers = Postgres2Postgres(
        #         task_id="transfer_cso_pers",
        #         postgres_conn_id=CONN_source,
        #         sql='./sql/transfer_cos_pers.sql',
        #         # todo поправить таблицу с данными
        #         file_name='user_new',
        #         file_format='xml',
        #         template_xml='./template/user_new.xml',
        #         on_success_callback=task_success_log
        #     )
        #     sleep >> log_sensor >> log_start >> getid >> check_system_source >> check_system_target >> transfer_cso_pers  # идеальный сценарий
        #     check_system_source >> Label(
        #         "no network access") >> log_err_access_source  # нет сетевого доступа к системе источника
        #     check_system_target >> Label(
        #         "no network access") >> log_err_access_target  # нет сетевого доступа к системе приемника

        i = i + 1
