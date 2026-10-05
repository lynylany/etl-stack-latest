import requests,pandas as pd
from sqlalchemy import create_engine,text

dw=create_engine("postgresql+psycopg2://airflow:airflow@localhost:5432/dw_db")

df=pd.read_sql("""
    SELECT DISTINCT ON (area_key)
           area_key,latitude,longitude
    FROM dim_station
    WHERE is_current=TRUE
      AND area_key IS NOT NULL
      AND latitude IS NOT NULL
      AND longitude IS NOT NULL
    ORDER BY area_key,station_key
""",dw)

rows = []

for _, row in df.iterrows():
    try:
        response = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": row["latitude"],
                "longitude": row["longitude"],
                "current": "temperature_2m,relative_humidity_2m",
                "timezone": "Asia/Bangkok"
            },
            timeout=30
        )

        response.raise_for_status()
        w = response.json()

        t = pd.to_datetime(w["current"]["time"])

        rows.append({
            "area_key": int(row["area_key"]),
            "date_key": int(t.strftime("%Y%m%d")),
            "time_key": t.hour * 100 + t.minute,
            "temperature": pd.to_numeric(
                w["current"]["temperature_2m"],
                errors="coerce"
            ),
            "humidity": pd.to_numeric(
                w["current"]["relative_humidity_2m"],
                errors="coerce"
            )
        })

    except requests.RequestException as e:
        print("Request failed:", row["area_key"], e)

df_weather=pd.DataFrame(rows)

if not df_weather.empty:
    df_weather=df_weather.drop_duplicates(
        ["area_key","date_key","time_key"]
    )

with dw.begin() as conn:
    for _,r in df_weather.iterrows():
        conn.execute(text("""
            INSERT INTO fact_weather
            (area_key,date_key,time_key,temperature,humidity)
            VALUES
            (:area_key,:date_key,:time_key,:temperature,:humidity)
            ON CONFLICT (area_key,date_key,time_key) DO NOTHING
        """),r.to_dict())

print("Areas:",len(df))
print("Weather:",len(df_weather))
print("Loaded")