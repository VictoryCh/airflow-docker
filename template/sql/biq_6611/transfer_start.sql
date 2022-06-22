do $$
declare
	q_stream integer;
begin  
	select 1 into q_stream
	from {{ params.schema }}.sap_hr_semaphore
	where object_flow = {{ params.object_flow }} and status = 1;
  
	if q_stream > 0 then
		raise exception 'COS WEB - The stream is blocked';
	else 
  		INSERT INTO {{ params.schema }}.sap_hr_semaphore (object_flow, t_start, status) VALUES({{ params.object_flow }}, NOW(), 1);
	end if;

end $$
