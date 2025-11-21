"""
2025-11 枚举迁移：为 parents.current_status 增加 '待接手'

说明：
- 现有数据库列为 ENUM('待分配','合伙人跟进中','销售跟进中','已成交','已流失')。
- 业务已更新为引入 '待接手' 新状态，且新家长默认进入 '待接手'。
- 本脚本将通过 ALTER TABLE 修改枚举取值，并设置默认值为 '待接手'。

使用方法：
    python scripts/migrate_202511_add_pending_status.py
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pymysql
from config import config

ALTER_SQL = (
    """
    ALTER TABLE parents 
    MODIFY COLUMN current_status 
    ENUM('待分配','待接手','合伙人跟进中','销售跟进中','已成交','已流失') 
    NOT NULL DEFAULT '待接手' COMMENT '当前状态';
    """
)

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
            print("执行枚举迁移...")
            cur.execute(ALTER_SQL)
            conn.commit()
            print("迁移完成：已为 parents.current_status 增加 '待接手'，并设为默认值")

            # 验证当前列定义
            cur.execute("SHOW COLUMNS FROM parents LIKE 'current_status';")
            row = cur.fetchone()
            print("当前列定义:", row)
            print("若 Type 中包含 'enum('待分配','待接手','合伙人跟进中','销售跟进中','已成交','已流失')' 则迁移成功")

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