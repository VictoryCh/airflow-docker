do $$
declare
	q_stream integer;
begin  
	select 1 into q_stream
	from {{ params.schema }}.sap_hr_semaphore
	where object_flow = {{ params.object_flow }} and status = 1 and (DATE_PART('day', now() - t_start) * 24 + DATE_PART('hour', now() - t_start)) >= 3;
  
	if q_stream > 0 then
	    update {{ params.schema }}.sap_hr_semaphore
	    set status = 3, comment = 'Error: Hanging'
	    where object_flow = {{ params.object_flow }} and status = 1 and (DATE_PART('day', now() - t_start) * 24 + DATE_PART('hour', now() - t_start)) >= 3;
	end if;

    INSERT INTO {{ params.schema }}.sap_hr_semaphore (object_flow, t_start, status) VALUES({{ params.object_flow }}, NOW(), 1);
end $$
