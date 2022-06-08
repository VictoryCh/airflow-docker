do $$
declare
	q_stream integer;
begin  
	select 1 into q_stream
	from list_flow_log
	where object_flow = {{ params.object_flow }} and status = 1;
  
	if q_stream > 0 then
		raise exception 'IP-offline - The stream is blocked';
	else 
  		INSERT INTO list_flow_log (object_flow, t_start, status) VALUES({{ params.object_flow }}, NOW(), 1);
	end if;

end $$
