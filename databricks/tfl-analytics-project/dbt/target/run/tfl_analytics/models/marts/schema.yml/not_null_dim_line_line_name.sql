
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select line_name
from `rudxdatabricks`.`default`.`dim_line`
where line_name is null



  
  
      
    ) dbt_internal_test