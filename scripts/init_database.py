"""
数据库初始化脚本
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.database import create_tables, SessionLocal, StaffMapping
from config import config
import pymysql

def create_database():
    """创建数据库"""
    try:
        # 连接MySQL服务器（不指定数据库）
        connection = pymysql.connect(
            host=config.MYSQL_HOST,
            user=config.MYSQL_USER,
            password=config.MYSQL_PASSWORD,
            port=config.MYSQL_PORT,
            charset='utf8mb4'
        )
        
        with connection.cursor() as cursor:
            # 创建数据库
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {config.MYSQL_DB} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            print(f"数据库 {config.MYSQL_DB} 创建成功")
        
        connection.close()
        
    except Exception as e:
        print(f"创建数据库失败: {e}")
        return False
    
    return True

def init_staff_data():
    """初始化员工数据"""
    db = SessionLocal()
    try:
        # 检查是否已有数据
        existing_count = db.query(StaffMapping).count()
        if existing_count > 0:
            print(f"员工数据已存在 {existing_count} 条记录，跳过初始化")
            return
        
        # 初始化示例员工数据
        staff_data = [
            {
                'staff_id': 'SM_社媒小王',
                'role': '社媒',
                'is_active': 1
            },
            {
                'staff_id': 'HP_合伙人张总', 
                'role': '合伙人',
                'is_active': 1
            },
            {
                'staff_id': 'XS_销售李经理',
                'role': '销售',
                'is_active': 1
            }
        ]
        
        for staff in staff_data:
            staff_obj = StaffMapping(**staff)
            db.add(staff_obj)
        
        db.commit()
        print(f"初始化员工数据成功，共 {len(staff_data)} 条记录")
        
    except Exception as e:
        db.rollback()
        print(f"初始化员工数据失败: {e}")
    finally:
        db.close()

def main():
    """主函数"""
    print("开始初始化数据库...")
    
    # 1. 创建数据库
    if not create_database():
        print("数据库创建失败，退出")
        return
    
    # 2. 创建表结构
    try:
        create_tables()
        print("数据表创建成功")
    except Exception as e:
        print(f"创建数据表失败: {e}")
        return
    
    # 3. 初始化基础数据
    init_staff_data()
    
    print("数据库初始化完成！")

if __name__ == "__main__":
    main()