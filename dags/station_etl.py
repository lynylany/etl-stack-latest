import re,requests,pandas as pd
from sqlalchemy import create_engine,text
from datetime import date
from pathlib import Path

dw=create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/dw_db")
API_KEY="bd4e93003fa947c6bdcbd19f4df4682d"
BASE_DIR = Path(__file__).resolve().parent.parent

df = pd.read_csv(
    BASE_DIR / "data" / "air4thai_stations.csv"
)

def parse_area(x):
    if pd.isna(x): return pd.Series([None,None,None])
    x=str(x).strip()
    s=re.search(r"(?:ต\.|ตำบล|แขวง)\s*([^,]+?)(?=\s+(?:อ\.|อำเภอ|เขต)|,|$)",x)
    d=re.search(r"(?:อ\.|อำเภอ|เขต)\s*([^,]+?)(?=,|$)",x)
    p=re.sub(r"^(จ\.|จังหวัด)\s*","",x.split(",")[-1].strip()) if "," in x else None
    return pd.Series([s.group(1).strip() if s else None,d.group(1).strip() if d else None,p])

df[["subdistrict_name","district_name","province_name"]]=df["areaTH"].apply(parse_area)

df["district_name"]=df["district_name"].str.replace(
    r"^(อ\.|อำเภอ|เขต)\s*","",regex=True).str.strip()

df["province_name"]=(df["province_name"]
    .str.replace("กรุงเทพฯ","กรุงเทพมหานคร",regex=False)
    .str.replace("กทม.","กรุงเทพมหานคร",regex=False)
    .str.replace(r"^(จ\.|จังหวัด)\s*","",regex=True)
    .str.strip())

df.loc[df["district_name"].eq("เมือง"),"district_name"]="เมือง"+df["province_name"]
df.loc[df["province_name"].isna()&df["stationType"].eq("BKK"),"province_name"]="กรุงเทพมหานคร"

def reverse_geocode(lat,lon):
    try:
        r=requests.get(
            "https://api.geoapify.com/v1/geocode/reverse",
            params={
                "lat":lat,"lon":lon,
                "format":"json","lang":"th",
                "apiKey":API_KEY
            },
            timeout=10
        )
        r.raise_for_status()
        results=r.json().get("results",[])
        return results[0].get("quarter") if results else None
    except requests.RequestException:
        print(f"Reverse geocode failed: {lat},{lon}")
        return None

m=df["subdistrict_name"].isna()
df.loc[m,"subdistrict_name"]=df.loc[m].apply(
    lambda r:reverse_geocode(r["lat"],r["long"]),axis=1)

df["subdistrict_name"]=df["subdistrict_name"].str.replace(
    r"^(ต\.|ตำบล|แขวง)\s*","",regex=True).str.strip()

area=pd.read_sql(
    "SELECT area_key,province_name,district_name,subdistrict_name FROM dim_area",
    dw)

df=df.merge(
    area,
    on=["province_name","district_name","subdistrict_name"],
    how="left")

df_station=df[
    ["stationID","nameTH","stationType","lat","long","area_key"]
].copy()

df_station.columns=[
    "station_id","station_name_th","station_type",
    "latitude","longitude","area_key"]

print("Station:",len(df_station))
print("Not matched:",df_station["area_key"].isna().sum())

df_station = df_station.where(pd.notna(df_station), None)

today=date.today()

with dw.begin() as conn:
    current=pd.read_sql(
        "SELECT * FROM dim_station WHERE is_current=TRUE",
        dw)

    for _,r in df_station.iterrows():
        old=current[current["station_id"].eq(r["station_id"])]

        if old.empty:
            changed=True
        else:
            o=old.iloc[0]
            changed=any(
                not (
                    pd.isna(o[c]) and pd.isna(r[c])
                ) and o[c]!=r[c]
                for c in [
                    "station_name_th",
                    "station_type",
                    "latitude",
                    "longitude",
                    "area_key"
                ]
            )

        if not changed:
            continue

        if not old.empty:
            conn.execute(text("""
                UPDATE dim_station
                SET effective_to=:today,
                    is_current=FALSE
                WHERE station_id=:station_id
                  AND is_current=TRUE
            """),{
                "today":today,
                "station_id":r["station_id"]
            })

        conn.execute(text("""
            INSERT INTO dim_station (
                station_id,
                station_name_th,
                station_type,
                latitude,
                longitude,
                area_key,
                effective_from,
                effective_to,
                is_current
            )
            VALUES (
                :station_id,
                :station_name_th,
                :station_type,
                :latitude,
                :longitude,
                :area_key,
                :effective_from,
                NULL,
                TRUE
            )
        """),{
            **r.to_dict(),
            "effective_from":today
        })

print("Loaded")