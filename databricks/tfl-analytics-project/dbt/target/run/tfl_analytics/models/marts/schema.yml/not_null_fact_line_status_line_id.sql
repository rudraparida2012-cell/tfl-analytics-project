
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select line_id
from `rudxdatabricks`.`default`.`fact_line_status`
where line_id is null



  
  
      
    ) dbt_internal_test