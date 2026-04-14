
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select line_id as from_field
    from `rudxdatabricks`.`default`.`fact_line_status`
    where line_id is not null
),

parent as (
    select line_id as to_field
    from `rudxdatabricks`.`default`.`dim_line`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test