select case when count(id) = 0 then 1 else 0 end
from {{ params.schema }}.sap_hr_semaphore
where object_flow = {{ params.object_flow }} and status = 1 and (DATE_PART('day', now() - t_start) * 24 + DATE_PART('hour', now() - t_start)) < 3;
