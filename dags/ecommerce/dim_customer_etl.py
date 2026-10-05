import pandas as pd
from sqlalchemy import create_engine, text
from datetime import date
from pathlib import Path

dw = create_engine("postgresql+psycopg2://airflow:airflow@localhost:5432/dw_db")
BASE_DIR = Path(__file__).resolve().parent.parent.parent
CSV_PATH = BASE_DIR / "data" / "Sample - Superstore.csv"

df = pd.read_csv(CSV_PATH, encoding="windows-1252")

df_cust = df[["Customer ID", "Customer Name", "Segment"]].drop_duplicates("Customer ID")
df_cust.columns = ["customer_id", "customer_name", "segment"]

today = date.today()

with dw.begin() as conn:
    current = pd.read_sql("SELECT * FROM ecommerce.dim_customer WHERE is_current=TRUE", dw)

    for _, r in df_cust.iterrows():
        old = current[current["customer_id"].eq(r["customer_id"])]

        if old.empty:
            changed = True
        else:
            o = old.iloc[0]
            changed = (o["customer_name"] != r["customer_name"]) or (o["segment"] != r["segment"])

        if not changed:
            continue

        if not old.empty:
            conn.execute(text("""
                UPDATE ecommerce.dim_customer
                SET effective_to = :today, is_current = FALSE
                WHERE customer_id = :customer_id AND is_current = TRUE
            """), {"today": today, "customer_id": r["customer_id"]})

        conn.execute(text("""
            INSERT INTO ecommerce.dim_customer (customer_id, customer_name, segment, effective_from, effective_to, is_current)
            VALUES (:customer_id, :customer_name, :segment, :effective_from, NULL, TRUE)
        """), {**r.to_dict(), "effective_from": today})

print(f"Loaded Customer: {len(df_cust)}")