with sales_by_product as (
    select product_title, product_type, IFNULL(variant_title, ' ') as variant_title, month, net_quantity

    from kimino.public.sales_by_product
)

select * from sales_by_product