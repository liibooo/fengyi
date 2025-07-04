import os
import csv
import configparser
import psycopg2
from datetime import datetime
from typing import List, Dict

def create_connection() -> psycopg2.extensions.connection:
    # 读取配置文件
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), 'config.ini'))

    # 获取数据库配置
    db_config = {
        'host': config.get('database', 'host'),
        'port': config.get('database', 'port'),
        'dbname': config.get('database', 'database'),
        'user': config.get('database', 'user'),
        'password': config.get('database', 'password')
    }

    # 创建数据库连接
    conn = psycopg2.connect(**db_config)
    print("PostgreSQL 连接成功！")
    return conn

conn = create_connection()

def create_info_table(conn):
    """创建卫星信息表结构"""
    cursor = conn.cursor()
    
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS sat_data (
        object_name VARCHAR(100) NOT NULL,
        object_id VARCHAR(20) NOT NULL,
        norad_cat_id INTEGER PRIMARY KEY,
        object_type VARCHAR(5) NOT NULL,
        ops_status_code VARCHAR(1) NOT NULL,
        owner VARCHAR(5) NOT NULL,
        launch_date DATE NOT NULL,
        launch_site VARCHAR(6) NOT NULL,
        decay_date DATE,
        period FLOAT,
        inclination FLOAT,
        apogee INTEGER,
        perigee INTEGER,
        rcs FLOAT,
        data_status_code VARCHAR(3) NOT NULL,
        orbit_center VARCHAR(10) NOT NULL,
        orbit_type VARCHAR(3) NOT NULL
    );
    """
    cursor.execute(create_table_sql)

    # 创建索引
    create_index_sql = """
    CREATE INDEX IF NOT EXISTS idx_object_name ON sat_data (object_name);
    CREATE INDEX IF NOT EXISTS idx_object_id ON sat_data (object_id);
    CREATE INDEX IF NOT EXISTS idx_norad_cat_id ON sat_data (norad_cat_id);
    CREATE INDEX IF NOT EXISTS idx_object_type ON sat_data (object_type);
    CREATE INDEX IF NOT EXISTS idx_ops_status_code ON sat_data (ops_status_code);
    CREATE INDEX IF NOT EXISTS idx_owner ON sat_data (owner);
    CREATE INDEX IF NOT EXISTS idx_launch_date ON sat_data (launch_date);
    CREATE INDEX IF NOT EXISTS idx_launch_site ON sat_data (launch_site);
    """
    cursor.execute(create_index_sql)

    conn.commit()
    cursor.close()
    print("卫星信息表 sat_data 创建成功！")

def parse_satellite_data(file_path: str) -> List[Dict]:
    """解析CSV格式的卫星数据"""
    satellites = []
    with open("F:\satcat.csv", 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # 处理空值
            decay_date = row['DECAY_DATE'] if row['DECAY_DATE'] else None
            period = float(row['PERIOD']) if row['PERIOD'] else None
            inclination = float(row['INCLINATION']) if row['INCLINATION'] else None
            apogee = int(row['APOGEE']) if row['APOGEE'] else None
            perigee = int(row['PERIGEE']) if row['PERIGEE'] else None
            rcs = float(row['RCS']) if row['RCS'] else None
            
            satellite = {
                'object_name': row['OBJECT_NAME'],
                'object_id': row['OBJECT_ID'],
                'norad_cat_id': int(row['NORAD_CAT_ID']),
                'object_type': row['OBJECT_TYPE'],
                'ops_status_code': row['OPS_STATUS_CODE'],
                'owner': row['OWNER'],
                'launch_date': datetime.strptime(row['LAUNCH_DATE'], '%Y-%m-%d').date(),
                'launch_site': row['LAUNCH_SITE'],
                'decay_date': datetime.strptime(decay_date, '%Y-%m-%d').date() if decay_date else None,
                'period': period,
                'inclination': inclination,
                'apogee': apogee,
                'perigee': perigee,
                'rcs': rcs,
                'data_status_code': row['DATA_STATUS_CODE'],
                'orbit_center': row['ORBIT_CENTER'],
                'orbit_type': row['ORBIT_TYPE']
            }
            satellites.append(satellite)
    return satellites

def insert_satellite_data(conn, satellites: List[Dict]):
    """批量插入卫星数据"""
    cursor = conn.cursor()
    
    insert_sql = """
    INSERT INTO sat_data (
        object_name, object_id, norad_cat_id, object_type, 
        ops_status_code, owner, launch_date, launch_site, 
        decay_date, period, inclination, apogee, perigee, 
        rcs, data_status_code, orbit_center, orbit_type
    ) VALUES (
        %(object_name)s, %(object_id)s, %(norad_cat_id)s, %(object_type)s,
        %(ops_status_code)s, %(owner)s, %(launch_date)s, %(launch_site)s,
        %(decay_date)s, %(period)s, %(inclination)s, %(apogee)s, %(perigee)s,
        %(rcs)s, %(data_status_code)s, %(orbit_center)s, %(orbit_type)s
    )
    ON CONFLICT (norad_cat_id) DO NOTHING;
    """
    
    try:
        cursor.executemany(insert_sql, satellites)
        conn.commit()
        print(f"成功插入/更新 {len(satellites)} 条卫星数据")
    except Exception as e:
        conn.rollback()
        print(f"插入数据时出错: {e}")
    finally:
        cursor.close()

# 使用示例
if __name__ == "__main__":
    # 创建表
    create_info_table(conn)
    
    # 假设数据保存在satellites.csv文件中
    csv_file = "satellites.csv"
    
    # 解析数据
    satellites = parse_satellite_data(csv_file)
    
    # 插入数据
    insert_satellite_data(conn, satellites)
    
    # 关闭连接
    conn.close()