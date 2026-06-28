with sales as (
    select *
    from {{ ref('int_sales_enriched') }}
),

rates as (
    select *
    from {{ ref('exchange_rates') }}
),

converted as (
    select
        sales.transaction_id,
        sales.store_id,
        sales.sku,
        sales.raw_amount,
        sales.currency,
        sales.was_refunded,
        sales.conversion_time_hours,
        sales.raw_amount / rates.exchange_rate as amount_usd
    from sales
    left join rates
        on sales.currency = rates.currency
)

select
    store_id,
    sum(amount_usd) as total_sales_usd,
    avg(conversion_time_hours) as avg_conversion_time,
    sum(was_refunded)::float / count(*) as refund_rate,
    avg(amount_usd) as ticket_promedio_usd
from converted
group by store_id