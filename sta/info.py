import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch

# 数据库连接配置 - 请根据你的实际情况修改
DB_CONFIG = {
    "host": "localhost",
    "database": "celestrak",
    "user": "postgres",
    "password": "4906919",
    "port": "5432"
}

# CSV 文件路径
CSV_FILE_PATH = r"F:\卫星信息\satcat.csv"

def create_table(conn):
    """创建表结构"""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS satellite_data (
        object_name TEXT,
        object_id TEXT,
        norad_cat_id INTEGER,
        object_type TEXT,
        ops_status_code TEXT,
        owner TEXT,
        launch_date DATE,
        launch_site TEXT,
        decay_date DATE,
        period NUMERIC,
        inclination NUMERIC,
        apogee INTEGER,
        perigee INTEGER,
        rcs NUMERIC,
        data_status_code TEXT,
        orbit_center TEXT,
        orbit_type TEXT
    );
    """
    with conn.cursor() as cursor:
        cursor.execute(create_table_query)
    conn.commit()

def import_csv_to_postgres(csv_path, conn):
    """将CSV数据导入PostgreSQL"""
    # 读取CSV文件
    df = pd.read_csv(csv_path)
    
    # 处理可能的空值
    df = df.where(pd.notnull(df), None)
    
    # 准备插入语句 - 使用正确的参数占位符格式
    insert_query = """
    INSERT INTO satellite_data (
        object_name, object_id, norad_cat_id, object_type, ops_status_code,
        owner, launch_date, launch_site, decay_date, period, inclination,
        apogee, perigee, rcs, data_status_code, orbit_center, orbit_type
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    # 将DataFrame转换为元组列表
    data_tuples = [tuple(x) for x in df.to_numpy()]
    
    # 使用execute_batch批量插入数据
    with conn.cursor() as cursor:
        execute_batch(cursor, insert_query, data_tuples)
    
    conn.commit()
    print(f"成功导入 {len(data_tuples)} 条记录")

def main():
    conn = None
    try:
        # 连接数据库
        conn = psycopg2.connect(**DB_CONFIG)
        
        # 创建表
        create_table(conn)
        
        # 导入数据
        import_csv_to_postgres(CSV_FILE_PATH, conn)
        
    except Exception as e:
        print(f"发生错误: {e}")
    finally:
        if conn:
            conn.close()
            print("数据库连接已关闭")

if __name__ == "__main__":
    main()