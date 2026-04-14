
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select status_severity
from `rudxdatabricks`.`default`.`fact_line_status`
where status_severity is null



  
  
      
    ) dbt_internal_test