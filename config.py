"""
配置管理模块
"""
import os
from dotenv import load_dotenv

# 加载环境变量（确保 .env 覆盖系统已有变量）
load_dotenv(override=True)

class Config:
    """基础配置类"""
    
    # 数据库配置
    MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
    MYSQL_USER = os.getenv('MYSQL_USER', 'root')
    MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', '528629')
    MYSQL_DB = os.getenv('MYSQL_DB', 'lead_management')
    MYSQL_PORT = int(os.getenv('MYSQL_PORT', 3306))
    
    # Flask配置
    FLASK_HOST = os.getenv('FLASK_HOST', '127.0.0.1')
    FLASK_PORT = int(os.getenv('FLASK_PORT', 5001))
    FLASK_DEBUG = os.getenv('FLASK_DEBUG', 'False').lower() == 'true'
    SECRET_KEY = os.getenv('SECRET_KEY', 'your-secret-key-change-in-production')
    
    # Admin 管理页面登录配置（写死账号密码，亦可通过环境变量覆盖）
    ADMIN_USERNAME = os.getenv('ADMIN_USERNAME', 'admin')
    ADMIN_PASSWORD = os.getenv('ADMIN_PASSWORD', 'Admin@robot2025')
    
    # OpenAI配置
    OPENAI_API_KEY = os.getenv('OPENAI_API_KEY', '')
    OPENAI_BASE_URL = os.getenv('OPENAI_BASE_URL', 'https://api.openai.com/v1')
    # 可选模型名（兼容OpenAI风格），例如 Kimi 使用兼容接口
    OPENAI_MODEL = os.getenv('OPENAI_MODEL', 'default')
    # SQLAgent 模式：'auto'（默认，优先 LangChain 失败则直连）、'direct'（强制直连）、'langchain'（强制使用 LangChain）
    SQL_AGENT_MODE = os.getenv('SQL_AGENT_MODE', 'auto').lower()
    
    # 业务配置
    PARENT_CODE_PREFIX = os.getenv('PARENT_CODE_PREFIX', 'P')
    DEFAULT_TIMEZONE = os.getenv('DEFAULT_TIMEZONE', 'Asia/Shanghai')
    
    # 日志配置
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FILE = os.getenv('LOG_FILE', 'logs/app.log')

    # WorkTool配置
    WORKTOOL_API_HOST = os.getenv('WORKTOOL_API_HOST', 'api.worktool.ymdyes.cn')
    WORKTOOL_ROBOT_ID = os.getenv('WORKTOOL_ROBOT_ID', '')

    # Agent记忆配置：每个发送者保留最近回合数
    AGENT_MEMORY_MAX_TURNS = int(os.getenv('AGENT_MEMORY_MAX_TURNS', 6))
    
    @property
    def database_url(self):
        """获取数据库连接URL"""
        return f"mysql+pymysql://{self.MYSQL_USER}:{self.MYSQL_PASSWORD}@{self.MYSQL_HOST}:{self.MYSQL_PORT}/{self.MYSQL_DB}?charset=utf8mb4"

# 创建配置实例
config = Config()