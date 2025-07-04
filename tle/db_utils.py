import os
import configparser
import psycopg2
from datetime import datetime
from tle_utils import extract_tle_from_text, get_info_from_tle

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

def create_tle_table():
    # 创建"sat_tle"表，包含字段：id，norad_cat_id, tle_line1, tle_line2, created_at, source
    cursor = conn.cursor()
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS sat_tle (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        norad_cat_id VARCHAR(5) NOT NULL,
        tle_time TIMESTAMP NOT NULL,
        tle_str VARCHAR(140) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        source VARCHAR(128)
    );
    """

    cursor.execute(create_table_sql)

    create_index_sql = """
    CREATE INDEX IF NOT EXISTS idx_norad_cat_id ON sat_tle (norad_cat_id);\
    CREATE INDEX IF NOT EXISTS idx_tle_time ON sat_tle (tle_time);
    CREATE INDEX IF NOT EXISTS idx_created_at ON sat_tle (created_at);
    CREATE INDEX IF NOT EXISTS idx_source ON sat_tle (source);
    """

    cursor.execute(create_index_sql)

    conn.commit()
    cursor.close()

def create_sat_table():
    pass

def create_table():
    create_tle_table()
    create_sat_table()

def insert_single_tle(norad_cat_id: str, tle_time: datetime, tle_str: str, source: str):
    """
    插入单条TLE数据到数据库
    :param norad_cat_id: 卫星的NORAD_CAT_ID
    :param tle_time: TLE时间，datetime格式
    :param tle_str: TLE字符串
    :param source: 数据来源
    """
    cursor = conn.cursor()
    insert_sql = """
    INSERT INTO sat_tle (norad_cat_id, tle_time, tle_str, source)
    VALUES (%s, %s, %s, %s);
    """
    cursor.execute(insert_sql, (norad_cat_id, tle_time, tle_str, source))
    conn.commit()
    cursor.close()

def insert_tle_text(tle_text: str, source: str):
    tle_str_list = extract_tle_from_text(tle_text)
    cursor = conn.cursor()
    for tle in tle_str_list:
        norad_cat_id, tle_time = get_info_from_tle(tle)
        insert_sql = """
        INSERT INTO sat_tle (norad_cat_id, tle_time, tle_str, source)
        VALUES (%s, %s, %s, %s);
        """
        cursor.execute(insert_sql, (norad_cat_id, tle_time, tle, source))
        conn.commit()
    cursor.close()
    
    tle_count = len(tle_str_list)
    print(f"成功插入 {tle_count} 条TLE数据。")
    return tle_count

if __name__ == "__main__":
    create_table()
    # 示例：插入TLE数据
    filename = "F:\gp.txt"
    with open(filename, "r") as file:
        tle_data = file.read()
    insert_tle_text(tle_data, "test_source")
    print("TLE数据已插入数据库。")