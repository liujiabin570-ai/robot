"""
Flask应用启动脚本
"""
import os
import sys
from app import app
from config import config
from utils.logger import app_logger

def main():
    """启动Flask应用"""
    try:
        app_logger.info("正在启动Flask应用...")
        app_logger.info(f"配置信息: Host={config.FLASK_HOST}, Port={config.FLASK_PORT}, Debug={config.FLASK_DEBUG}")
        
        # 启动Flask应用
        app.run(
            host=config.FLASK_HOST,
            port=config.FLASK_PORT,
            debug=config.FLASK_DEBUG
        )
        
    except Exception as e:
        app_logger.error(f"启动Flask应用失败: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()