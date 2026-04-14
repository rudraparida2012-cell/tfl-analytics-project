
  
    
        create or replace table `rudxdatabricks`.`default`.`fact_line_status`
      
      
  using delta
      
      
      
      
      
      
      
      as
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
from `rudxdatabricks`.`default`.`int_line_status`
  