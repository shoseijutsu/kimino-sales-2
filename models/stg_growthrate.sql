with customers as (
    select * from {{ ref('stg_customers') }}
),

growth as (
    select date_trunc('month', day) as date, count(*) as count, lag(count(*), 1) over (order by date_trunc('month', day)) as prevmonth
    from customers
    group by 1
    order by 1
),

growth_rate as(
    select date, count, round(100*(count-prevmonth)/prevmonth,1) as growthrate
    from growth
    where date NOT IN ('2021-11-01')
)

select * from growth_rate