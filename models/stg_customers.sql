with sales as (
    select * from {{ref('stg_sales')}}
),

customers as (
    select 
        date_trunc(month, day) as sale_month,
        day as sale_date, 
        customer_name,
        customer_id, 
        net_sales as order_value
    from sales
    where customer_name IS NOT NULL and net_sales <> 0
) 

select * from customers