
  
  
  create or replace view `rudxdatabricks`.`default`.`stg_line_status`
  
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
    line_created_at,
    line_modified_at,
    source_file_name,
    source_system,
    dataset_name,
    bronze_loaded_at,
    silver_loaded_at
from `rudxdatabricks`.`default`.`line_status`
  )
