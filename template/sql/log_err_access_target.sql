UPDATE list_flow_log
SET t_end=NOW(), status=3, code_error=2
where id = {{ ti.xcom_pull(key='row_id') }};
