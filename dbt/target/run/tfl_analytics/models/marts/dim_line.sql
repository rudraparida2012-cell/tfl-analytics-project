
  
    
        create or replace table `rudxdatabricks`.`default`.`dim_line`
      
      
  using delta
      
      
      
      
      
      
      
      as
      select distinct
    line_id,
    line_name,
    mode_name
from `rudxdatabricks`.`default`.`int_line_status`
  