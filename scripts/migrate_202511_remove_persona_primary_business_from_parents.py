"""
迁移脚本：删除 parents 表中的 persona 与 primary_business 字段。

运行方法：
python scripts/migrate_202511_remove_persona_primary_business_from_parents.py
"""
import sys
import os

# 确保项目根可导入
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.append(ROOT)

from sqlalchemy import create_engine, text
from config import config


def column_exists(engine, table: str, column: str) -> bool:
    """检查列是否存在（MySQL）。"""
    try:
        with engine.connect() as conn:
            sql = text(
                """
                SELECT COUNT(*) AS cnt
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = :db AND TABLE_NAME = :table AND COLUMN_NAME = :column
                """
            )
            res = conn.execute(sql, {
                'db': getattr(config, 'MYSQL_DB', ''),
                'table': table,
                'column': column
            }).scalar()
            return (res or 0) > 0
    except Exception:
        return False


def drop_column(engine, table: str, column: str):
    """删除指定列，若不存在则跳过。"""
    if not column_exists(engine, table, column):
        print(f"跳过：{table}.{column} 不存在")
        return
    try:
        with engine.begin() as conn:
            conn.execute(text(f"ALTER TABLE `{table}` DROP COLUMN `{column}`"))
        print(f"已删除列：{table}.{column}")
    except Exception as e:
        print(f"删除列失败 {table}.{column}：{e}")


def main():
    engine = create_engine(config.database_url, pool_recycle=3600)
    print("开始迁移：移除 parents.persona / parents.primary_business ...")
    drop_column(engine, 'parents', 'persona')
    drop_column(engine, 'parents', 'primary_business')
    print("迁移完成。")


if __name__ == '__main__':
    main()