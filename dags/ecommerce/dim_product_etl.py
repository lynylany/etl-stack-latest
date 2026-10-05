import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path

dw = create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/dw_db")
BASE_DIR = Path(__file__).resolve().parent.parent.parent
CSV_PATH = BASE_DIR / "data" / "Sample - Superstore.csv"

df = pd.read_csv(CSV_PATH, encoding="windows-1252")

df_prod = df[["Product ID", "Product Name", "Category", "Sub-Category"]].drop_duplicates(["Product ID", "Product Name"])
df_prod.columns = ["product_id", "product_name", "category", "sub_category"]

with dw.begin() as conn:
    for _, r in df_prod.iterrows():
        conn.execute(text("""
            INSERT INTO ecommerce.dim_product (product_id, product_name, category, sub_category)
            VALUES (:product_id, :product_name, :category, :sub_category)
            ON CONFLICT (product_id, product_name) DO NOTHING
        """), r.to_dict())

print(f"Loaded Product: {len(df_prod)}")