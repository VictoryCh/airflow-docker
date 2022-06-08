import logging

from airflow.models import Variable
from cryptography.fernet import Fernet
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class Postgres2Postgres_decrypt(BaseOperator):
    template_fields = ('sql',)
    template_ext = ('.sql',)  # file format
    ui_color = '#e08c8c'
    dest_cols = []

    @apply_defaults
    def __init__(self, sql, postgres_dest_conn_id=None, parameters=None, autocommit=False, rows_chunk=5000,
                 postgres_source_conn_id=None, dest_schema=None, dest_table=None, dest_cols=None, decrypt_column=None,
                 trunc=False, *args,
                 **kwargs):
        super(Postgres2Postgres_decrypt, self).__init__(*args, **kwargs)
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
        self.decrypt_column = decrypt_column
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

            f = None
            decrypt_col_num = []
            if self.decrypt_column is not None:
                logging.info("Decrypt_col: %s ", self.decrypt_column)
                key = Variable.get("fernet_secret_key_asup")
                f = Fernet(key)
                for colno, heading in enumerate(self.headings, start=0):
                    if heading in self.decrypt_column:
                        decrypt_col_num.append(colno)

            while len(target_rows) > 0:
                rows_total = rows_total + len(target_rows)
                tr_list = []
                for rowno, row in enumerate(target_rows, start=1):
                    list = []
                    for colno, cell_value in enumerate(row, start=1):
                        if f is None and colno not in decrypt_col_num:
                            list.append(cell_value)
                        else:
                            list.append(f.decrypt(str(cell_value).encode('utf-8')).decode('utf-8'))
                    tr_list.append(tuple(list))
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
            rows_count = context['task_instance'].xcom_pull(key='full_qr')
            full_comment = context['task_instance'].xcom_pull(key='full_comment')
            if rows_count is None and full_comment is None:
                rows_count, full_comment = 0, ''
            dest_hook = PostgresHook(postgres_conn_id=self.postgres_dest_conn_id)
            src_hook = PostgresHook(postgres_conn_id=self.postgres_source_conn_id)
            self._execute(src_hook, dest_hook, context)
            context['task_instance'].xcom_push(key='transferFailure', value=False)

            q_rows = context['task_instance'].xcom_pull(key='q_rows')
            rows_count = q_rows + rows_count
            full_comment = full_comment + self.dest_table + f': {q_rows}, '
            context['task_instance'].xcom_push(key='full_comment', value=full_comment)
            context['task_instance'].xcom_push(key='full_qr', value=rows_count)
        except Exception as exc:
            context['task_instance'].xcom_push(key='transferFailure', value=True)
            raise exc

