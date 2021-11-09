WITH sales AS (
    SELECT * FROM {{ref('stg_sales')}}
),
customers AS (
    SELECT 
        date_trunc(month, day) AS sale_month,
        day AS sale_date, 
        customer_name,
        customer_id, 
        net_sales AS order_value,
        customer_type,
        DENSE_RANK() OVER (PARTITION BY customer_name, sale_month ORDER BY sale_date ASC) as monthly_rank
    FROM sales
    WHERE 
        customer_name IS NOT NULL
        AND net_sales <> 0
        AND order_value > 0
),
customers_rank AS (
    SELECT 
        MAX(sale_month) as sale_month,
        sale_date,
        customer_name,
        MAX(customer_id) as customer_id,
        SUM(order_value) as order_value,
        MAX(monthly_rank) as monthly_rank
    FROM customers
    GROUP BY customer_name, sale_date
),
customers_pivot AS (
    SELECT
        *,
        CASE
            WHEN monthly_rank = 1 THEN 1
            ELSE NULL
        END AS unique_cnt,
        CASE
            WHEN monthly_rank = 2 THEN 1
            ELSE NULL
        END as repeat_cnt
    FROM customers_rank
)

SELECT 
    sale_month,
    COUNT(unique_cnt),
    COUNT(repeat_cnt),
    COUNT(repeat_cnt) / COUNT(unique_cnt) * 100 AS retention_rate
FROM customers_pivot
GROUP BY sale_month
ORDER BY sale_month DESC