import logging
import base64

from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from cryptography.fernet import Fernet
from rsa import encrypt


class Postgres2Postgres(BaseOperator):
    template_fields = ('sql',)
    template_ext = ('.sql',)  # file format
    ui_color = '#e08c8c'
    dest_cols = []

    @apply_defaults
    def __init__(self, sql, postgres_dest_conn_id=None, parameters=None, autocommit=False, rows_chunk=5000,
                 postgres_source_conn_id=None, dest_schema=None, dest_table=None, dest_cols=None, trunc=False, *args,
                 **kwargs):
        super(Postgres2Postgres, self).__init__(*args, **kwargs)
        if parameters is None:
            parameters = {}
        self.sql = sql  # sql query on source system (here, if necessary, transformations are performed)
        self.postgres_dest_conn_id = postgres_dest_conn_id
        self.parameters = parameters
        self.autocommit = autocommit
        self.rows_chunk = rows_chunk
        self.postgres_source_conn_id = postgres_source_conn_id
        self.dest_schema = dest_schema
        self.dest_table = dest_table
        self.dest_cols = dest_cols
        self.trunc = trunc

    def _execute(self, src_hook, dest_hook, context):
        with src_hook.get_conn() as src_conn:
            cursor = src_conn.cursor()
            logging.info("Source id connection: %s", self.postgres_dest_conn_id)
            logging.info("Executing sql = %s", self.sql)
            cursor.execute(self.sql, self.parameters)
            if self.trunc:
                sql_trunc = self.trunc
                logging.info("Truncating {0}.{1}".format(self.dest_schema, self.dest_table))
                dest_hook.run(sql_trunc)
            rows_total = 0
            target_rows = cursor.fetchmany(self.rows_chunk)

            key = Variable.get("fernet_secret_key_asup")
            f = Fernet(key)

            while len(target_rows) > 0:
                rows_total = rows_total + len(target_rows)
                tr_list = []
                for rowno, row in enumerate(target_rows, start=1):
                    list = []
                    for colno, cell_value in enumerate(row, start=1):
                        # logging.info("Data cell_value: %s ", cell_value)
                        if colno in (2, 3):
                            list.append(cell_value)
                        else:
                            list.append(f.encrypt(str(cell_value).encode('utf-8')).decode('utf-8'))
                    # logging.info("Data list: %s ", list)
                    tr_list.append(tuple(list))

                logging.info("Data target_rows: %s ", target_rows)
                logging.info("Data target_list %s ", tr_list)

                # print(self.dest_table, target_rows, self.dest_cols)
                dest_hook.insert_rows(self.dest_schema + '.' + self.dest_table, tr_list,
                                      target_fields=self.dest_cols,
                                      commit_every=self.rows_chunk)
                target_rows = cursor.fetchmany(self.rows_chunk)
                logging.info("Data transfer: %s rows", rows_total)

            logging.info("Finished data transfer")
            q_rows = rows_total
            context['task_instance'].xcom_push(key='q_rows', value=q_rows)

            cursor.close()

    def execute(self, context):

        try:
            dest_hook = PostgresHook(postgres_conn_id=self.postgres_dest_conn_id)
            src_hook = PostgresHook(postgres_conn_id=self.postgres_source_conn_id)
            self._execute(src_hook, dest_hook, context)
            context['task_instance'].xcom_push(key='transferFailure', value=False)
        except Exception as exc:
            context['task_instance'].xcom_push(key='transferFailure', value=True)
            raise exc
