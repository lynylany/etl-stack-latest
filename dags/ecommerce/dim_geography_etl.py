import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path

dw = create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/dw_db")
BASE_DIR = Path(__file__).resolve().parent.parent.parent
CSV_PATH = BASE_DIR / "data" / "Sample - Superstore.csv"

df = pd.read_csv(CSV_PATH, encoding="windows-1252")

# คลีนข้อมูล Postal Code
df["Postal Code"] = df["Postal Code"].fillna("00000").astype(str).str.split(".").str[0].str.zfill(5)

df_geo = df[["Country", "Region", "State", "City", "Postal Code"]].drop_duplicates()
df_geo.columns = ["country", "region", "state", "city", "postal_code"]

with dw.begin() as conn:
    for _, r in df_geo.iterrows():
        conn.execute(text("""
            INSERT INTO ecommerce.dim_geography (country, region, state, city, postal_code)
            VALUES (:country, :region, :state, :city, :postal_code)
            ON CONFLICT (country, state, city, postal_code) DO NOTHING
        """), r.to_dict())

print(f"Loaded Geography: {len(df_geo)}")