import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path

dw = create_engine("postgresql+psycopg2://airflow:airflow@localhost:5432/dw_db")
BASE_DIR = Path(__file__).resolve().parent.parent.parent
CSV_PATH = BASE_DIR / "data" / "Sample - Superstore.csv"

df = pd.read_csv(CSV_PATH, encoding="windows-1252")

# 1. Clean Postal Code และแปลงวันที่
df["Postal Code"] = df["Postal Code"].fillna("00000").astype(str).str.split(".").str[0].str.zfill(5)
df["order_full_date"] = pd.to_datetime(df["Order Date"]).dt.date
df["ship_full_date"] = pd.to_datetime(df["Ship Date"]).dt.date

# 2. ดึง Surrogate Keys จาก Dimensions
dim_date = pd.read_sql("SELECT date_key, full_date FROM ecommerce.dim_date", dw)
dim_cust = pd.read_sql("SELECT customer_key, customer_id FROM ecommerce.dim_customer WHERE is_current=TRUE", dw)
dim_prod = pd.read_sql("SELECT product_key, product_id, product_name FROM ecommerce.dim_product", dw)
dim_geo  = pd.read_sql("SELECT geo_key, country, state, city, postal_code FROM ecommerce.dim_geography", dw)

# 3. Merge หา Surrogate Keys
df_fact = df.merge(
    dim_date, left_on="order_full_date", right_on="full_date", how="left"
).rename(columns={"date_key": "order_date_key"}).drop(columns=["full_date"])

df_fact = df_fact.merge(
    dim_date, left_on="ship_full_date", right_on="full_date", how="left"
).rename(columns={"date_key": "ship_date_key"}).drop(columns=["full_date"])

df_fact = df_fact.merge(
    dim_cust, left_on="Customer ID", right_on="customer_id", how="left"
)

df_fact = df_fact.merge(
    dim_prod, left_on=["Product ID", "Product Name"], right_on=["product_id", "product_name"], how="left"
)

df_fact = df_fact.merge(
    dim_geo, left_on=["Country", "State", "City", "Postal Code"], right_on=["country", "state", "city", "postal_code"], how="left"
)

# 4. Insert เข้า Fact Table
with dw.begin() as conn:
    for _, r in df_fact.iterrows():
        conn.execute(text("""
            INSERT INTO ecommerce.fact_sales (
                row_id, order_id, order_date_key, ship_date_key,
                customer_key, product_key, geo_key,
                ship_mode, sales, quantity, discount, profit
            )
            VALUES (
                :row_id, :order_id, :order_date_key, :ship_date_key,
                :customer_key, :product_key, :geo_key,
                :ship_mode, :sales, :quantity, :discount, :profit
            )
            ON CONFLICT (order_id, row_id) DO NOTHING
        """), {
            "row_id": r["Row ID"],
            "order_id": r["Order ID"],
            "order_date_key": r["order_date_key"],
            "ship_date_key": r["ship_date_key"],
            "customer_key": r["customer_key"],
            "product_key": r["product_key"],
            "geo_key": r["geo_key"],
            "ship_mode": r["Ship Mode"],
            "sales": r["Sales"],
            "quantity": r["Quantity"],
            "discount": r["Discount"],
            "profit": r["Profit"]
        })

print(f"Fact Sales Loaded: {len(df_fact)}")