import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path

dw = create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/dw_db")
BASE_DIR = Path(__file__).resolve().parent.parent.parent
CSV_PATH = BASE_DIR / "data" / "Sample - Superstore.csv"

df = pd.read_csv(CSV_PATH, encoding="windows-1252")

# รวมวันที่ทั้งหมดทั้งฝั่ง Order และ Ship
dates_order = pd.to_datetime(df["Order Date"])
dates_ship = pd.to_datetime(df["Ship Date"])
all_dates = pd.concat([dates_order, dates_ship]).drop_duplicates().dt.date

df_date = pd.DataFrame({"full_date": all_dates})
df_date["date_key"] = df_date["full_date"].apply(lambda d: int(d.strftime("%Y%m%d")))
df_date["year"] = pd.to_datetime(df_date["full_date"]).dt.year
df_date["quarter"] = pd.to_datetime(df_date["full_date"]).dt.quarter
df_date["month"] = pd.to_datetime(df_date["full_date"]).dt.month
df_date["day"] = pd.to_datetime(df_date["full_date"]).dt.day
df_date["day_name"] = pd.to_datetime(df_date["full_date"]).dt.day_name()

with dw.begin() as conn:
    for _, r in df_date.iterrows():
        conn.execute(text("""
            INSERT INTO ecommerce.dim_date (date_key, full_date, year, quarter, month, day, day_name)
            VALUES (:date_key, :full_date, :year, :quarter, :month, :day, :day_name)
            ON CONFLICT (date_key) DO NOTHING
        """), r.to_dict())

print(f"Loaded Date: {len(df_date)}")