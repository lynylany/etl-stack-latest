"""
ตัวอย่างการเขียน python จากเครื่อง host ต่อเข้า PostgreSQL ใน docker
ติดตั้งก่อน: pip install pandas sqlalchemy psycopg2-binary
"""
import pandas as pd
from sqlalchemy import create_engine

# host = localhost เพราะ compose map port 5432 ออกมาแล้ว
engine = create_engine("postgresql+psycopg2://etl_user:etl_pass@localhost:5432/etl_db")

# อ่านข้อมูลที่ DAG โหลดเข้าไป
df = pd.read_sql("SELECT product, SUM(quantity) AS total_qty FROM sales GROUP BY product", engine)
print(df)

# เขียนข้อมูลจาก host เข้าไปก็ได้เช่นกัน
new_data = pd.DataFrame([{"product": "webcam", "quantity": 3, "price": 990.00}])
new_data.to_sql("sales", engine, if_exists="append", index=False)
print("inserted 1 row from host")
