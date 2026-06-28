select *
from {{ ref('fct_sales_daily') }}
where total_sales_usd <= 0