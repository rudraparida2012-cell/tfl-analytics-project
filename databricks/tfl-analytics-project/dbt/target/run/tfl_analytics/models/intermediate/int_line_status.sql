
  
  
  create or replace view `rudxdatabricks`.`default`.`int_line_status`
  
  as (
    select
    line_id,
    line_name,
    mode_name,
    line_status_id,
    status_line_id,
    status_severity,
    status_severity_description,
    status_reason,
    status_created_at,
    line_modified_at
from `rudxdatabricks`.`default`.`stg_line_status`
  )
