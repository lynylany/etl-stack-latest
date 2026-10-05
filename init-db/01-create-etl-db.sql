-- รันอัตโนมัติครั้งแรกที่ postgres container สร้าง volume ใหม่
-- สร้าง database + user แยกสำหรับงาน ETL (ไม่ปนกับ metadata ของ Airflow)

CREATE USER etl_user WITH PASSWORD 'etl_pass';
CREATE DATABASE etl_db OWNER etl_user;
GRANT ALL PRIVILEGES ON DATABASE etl_db TO etl_user;

-- ตารางตัวอย่างสำหรับสอน
\connect etl_db;

CREATE TABLE IF NOT EXISTS sales (
    id          SERIAL PRIMARY KEY,
    product     VARCHAR(100) NOT NULL,
    quantity    INT NOT NULL,
    price       NUMERIC(10, 2) NOT NULL,
    sold_at     TIMESTAMP NOT NULL DEFAULT NOW()
);

ALTER TABLE sales OWNER TO etl_user;
