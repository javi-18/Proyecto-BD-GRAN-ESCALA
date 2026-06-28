import json
import pandas as pd
from sqlalchemy import create_engine,text 


PARQUET_PATH = "data/silver/sales_silver.parquet"

DB_USER = "postgres"
DB_PASSWORD = "javi1234"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "supermarket"


engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)


df = pd.read_parquet(PARQUET_PATH)

rows = []

for _, row in df.iterrows():
    rows.append({
        "transaction_id": row["id"],
        "store_id": row["store"],
        "sku": row["sku"],
        "raw_amount": row["financials"]["raw_amount"],
        "currency": row["financials"]["currency"],
        "status_history": json.dumps(row["status_history"], default=str),
        "processed_at": row["metadata"]["processed_at"],
        "batch_id": row["metadata"]["batch_id"],
    })


df_flat = pd.DataFrame(rows)

with engine.begin() as conn:
    conn.execute(text("DROP TABLE IF EXISTS raw_sales_silver CASCADE"))

df_flat.to_sql(
    "raw_sales_silver",
    engine,
    if_exists="append",
    index=False
)

print("Tabla raw_sales_silver cargada correctamente en PostgreSQL.")