"""
迁移：更新 parents.recommend_channel 枚举为 ('社媒','合伙人') 并清洗旧数据
规则：
- 旧值为 '其他' 时：
  - 若 partner_id 非空，更新为 '合伙人'
  - 若 partner_id 为空字符串，更新为 '社媒'
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pymysql
from config import config

ALTER_ENUM_SQL = """
ALTER TABLE parents 
  MODIFY COLUMN recommend_channel ENUM('社媒','合伙人') 
  NOT NULL DEFAULT '社媒' COMMENT '推荐渠道';
"""

def run():
    print("开始迁移：更新 parents.recommend_channel 枚举为 ('社媒','合伙人')")
    conn = None
    try:
        conn = pymysql.connect(
            host=config.MYSQL_HOST,
            user=config.MYSQL_USER,
            password=config.MYSQL_PASSWORD,
            database=config.MYSQL_DB,
            port=config.MYSQL_PORT,
            charset='utf8mb4'
        )
        with conn.cursor() as cur:
            # 清洗旧值 '其他' -> 按 partner_id 是否为空归类
            print("清洗旧数据：将 '其他' 按规则映射为 '社媒' 或 '合伙人'")
            cur.execute("UPDATE parents SET recommend_channel='合伙人' WHERE recommend_channel='其他' AND partner_id <> ''")
            cur.execute("UPDATE parents SET recommend_channel='社媒' WHERE recommend_channel='其他' AND partner_id = ''")
            conn.commit()
            print("旧数据清洗完成")

            # 修改枚举定义
            print("修改字段枚举定义...")
            cur.execute(ALTER_ENUM_SQL)
            conn.commit()
            print("枚举定义更新完成")
        print("迁移完成！")
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"迁移失败: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    run()