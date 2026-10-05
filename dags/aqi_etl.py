import requests,numpy as np,pandas as pd
from sqlalchemy import create_engine,text

dw=create_engine("postgresql+psycopg2://airflow:airflow@localhost:5432/dw_db")

url="https://air4thai.pcd.go.th/services/getNewAQI_JSON.php"
data=requests.get(url,verify=False,timeout=30).json()

df=pd.DataFrame(data["stations"])

df_aqi=pd.concat([
    df[["stationID"]].reset_index(drop=True),
    pd.json_normalize(df["AQILast"])
],axis=1)

numeric_cols=[
    "PM25.aqi","PM25.value",
    "PM10.aqi","PM10.value",
    "O3.aqi","O3.value",
    "CO.aqi","CO.value",
    "NO2.aqi","NO2.value",
    "SO2.aqi","SO2.value",
    "AQI.aqi"
]

df_aqi[numeric_cols]=df_aqi[numeric_cols].apply(
    pd.to_numeric,errors="coerce"
)

df_aqi[numeric_cols]=df_aqi[numeric_cols].replace(
    [-1,-999],np.nan
)

df_aqi["full_date"]=pd.to_datetime(df_aqi["date"]).dt.date
df_aqi["full_time"]=pd.to_datetime(
    df_aqi["time"],format="%H:%M"
).dt.time

station=pd.read_sql(
    "SELECT station_key,station_id FROM dim_station WHERE is_current=TRUE",
    dw
)

dates=pd.read_sql(
    "SELECT date_key,full_date FROM dim_date",
    dw
)

times=pd.read_sql(
    "SELECT time_key,full_time FROM dim_time",
    dw
)

df_aqi=df_aqi.merge(
    station,
    left_on="stationID",
    right_on="station_id",
    how="left"
).merge(
    dates,
    on="full_date",
    how="left"
).merge(
    times,
    on="full_time",
    how="left"
)

print("AQI:",len(df_aqi))
print("Station not matched:",df_aqi["station_key"].isna().sum())
print("Date not matched:",df_aqi["date_key"].isna().sum())
print("Time not matched:",df_aqi["time_key"].isna().sum())

df_aqi=df_aqi.dropna(
    subset=["station_key","date_key","time_key"]
)

with dw.begin() as conn:
    for _,r in df_aqi.iterrows():
        conn.execute(text("""
            INSERT INTO fact_air_quality
            (
                station_key,date_key,time_key,
                pm25,pm10,o3,co,no2,so2,aqi
            )
            VALUES
            (
                :station_key,:date_key,:time_key,
                :pm25,:pm10,:o3,:co,:no2,:so2,:aqi
            )
            ON CONFLICT (
                station_key,date_key,time_key
            ) DO NOTHING
        """),{
            "station_key":r["station_key"],
            "date_key":r["date_key"],
            "time_key":r["time_key"],
            "pm25":r["PM25.value"],
            "pm10":r["PM10.value"],
            "o3":r["O3.value"],
            "co":r["CO.value"],
            "no2":r["NO2.value"],
            "so2":r["SO2.value"],
            "aqi":r["AQI.aqi"]
        })

print("Loaded")