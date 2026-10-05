import pandas as pd
from sqlalchemy import create_engine,text

src=create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/etl_db")
dw=create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/dw_db")

df=pd.read_sql("SELECT * FROM raw_area",src)

lookup=df.dropna(subset=["DistrictID","DistrictThai"]).drop_duplicates("DistrictID").set_index("DistrictID")["DistrictThai"]
df["DistrictThai"]=df["DistrictThai"].fillna(df["DistrictID"].map(lookup))

df["subdistrict_name"]=df["TambonThai"].str.replace(r"^(ต\.|ตำบล|แขวง)\s*","",regex=True).str.strip()
df["district_name"]=df["DistrictThai"].str.replace(r"^(อ\.|อำเภอ|เขต)\s*","",regex=True).str.strip()
df["province_name"]=df["ProvinceThai"].str.replace(r"^(จ\.|จังหวัด)\s*","",regex=True).str.strip()

df_area=df[["ProvinceID","province_name","DistrictID","district_name","TambonID","subdistrict_name"]].drop_duplicates("TambonID")
df_area.columns=["province_code","province_name","district_code","district_name","subdistrict_code","subdistrict_name"]

with dw.begin() as conn:
    for _,r in df_area.iterrows():
        conn.execute(text("""
            INSERT INTO dim_area
            (province_code,province_name,district_code,district_name,subdistrict_code,subdistrict_name)
            VALUES (:province_code,:province_name,:district_code,:district_name,:subdistrict_code,:subdistrict_name)
            ON CONFLICT (subdistrict_code) DO NOTHING
        """),r.to_dict())

print("Area:",len(df_area))