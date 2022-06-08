UPDATE list_flow_log
SET q_source=null, q_target=null, t_end=NOW(), status=2, optional_key=null, comment = '{{ params.comment }}'
where id = {{ ti.xcom_pull(key='row_id') }};
