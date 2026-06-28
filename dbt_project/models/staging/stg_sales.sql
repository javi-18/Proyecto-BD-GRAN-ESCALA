select
    transaction_id,
    store_id,
    sku,
    raw_amount,
    currency,
    status_history::text as status_history,
    processed_at,
    batch_id
from raw_sales_silver