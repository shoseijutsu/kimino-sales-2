with sales_by_product as (
    select * from {{ ref('stg_salesbyproduct') }}
),

monthly as (
    select month, concat(product_title, variant_title) as product, net quantity
    from sales_by_product
    order by month, product 
)

select * from monthly