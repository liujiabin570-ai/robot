"""
2025-11 架构迁移：删除 staff_mapping.real_name 列

说明：
- 业务决定使用 wechat_name 作为真实名称，不再维护 real_name 字段。
- 本脚本检查列是否存在，若存在则执行 ALTER TABLE 删除该列。

使用方法：
    python scripts/migrate_202511_remove_real_name_from_staff_mapping.py
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pymysql
from config import config

DROP_SQL = "ALTER TABLE staff_mapping DROP COLUMN real_name;"

def migrate():
    conn = None
    try:
        conn = pymysql.connect(
            host=config.MYSQL_HOST,
            user=config.MYSQL_USER,
            password=config.MYSQL_PASSWORD,
            port=config.MYSQL_PORT,
            database=config.MYSQL_DB,
            charset='utf8mb4'
        )
        with conn.cursor() as cur:
            # 检查列是否存在
            cur.execute("SHOW COLUMNS FROM staff_mapping LIKE 'real_name';")
            row = cur.fetchone()
            if row:
                print("检测到 staff_mapping.real_name 列，执行删除...")
                cur.execute(DROP_SQL)
                conn.commit()
                print("删除完成：已移除 staff_mapping.real_name")
            else:
                print("列不存在：staff_mapping.real_name 已不存在，跳过删除")

            # 验证
            cur.execute("SHOW COLUMNS FROM staff_mapping LIKE 'real_name';")
            row = cur.fetchone()
            if row:
                print("验证失败：real_name 仍存在", row)
                sys.exit(1)
            else:
                print("验证成功：staff_mapping 不再包含 real_name 列")

    except Exception as e:
        if conn:
            conn.rollback()
        print("迁移失败:", e)
        sys.exit(1)
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    migrate()