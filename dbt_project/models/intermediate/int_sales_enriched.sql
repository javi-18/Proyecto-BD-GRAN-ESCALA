select
    transaction_id,
    store_id,
    sku,
    raw_amount,
    currency,
    status_history,
    processed_at,
    batch_id,

    case
        when status_history like '%REFUNDED%' then 1
        else 0
    end as was_refunded,

    case
        when status_history like '%COMPLETED%' then 1.0
        else null
    end as conversion_time_hours

from {{ ref('stg_sales') }}