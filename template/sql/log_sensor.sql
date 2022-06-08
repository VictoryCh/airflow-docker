select case when q = 'true' then 1 else 0 end as q from  (
	select case when count(id)=0 then 'true' else 'false' end as q
	from list_flow_log
	where object_flow = {{ params.object_flow }} and status = 1
) t1;
