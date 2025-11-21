"""
通用工具函数模块
"""
import re
import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List
from config import config

class IDGenerator:
    """ID生成器"""
    
    @staticmethod
    def generate_parent_code() -> str:
        """生成家长编号"""
        # 格式: JZ + 年月日 + 4位随机数
        now = datetime.now()
        date_str = now.strftime("%Y%m%d")
        random_suffix = str(uuid.uuid4().int)[-4:]
        return f"{config.PARENT_CODE_PREFIX}{date_str}{random_suffix}"
    
    @staticmethod
    def generate_uuid() -> str:
        """生成UUID"""
        return str(uuid.uuid4())

class DateTimeHelper:
    """日期时间工具"""
    
    @staticmethod
    def get_current_time() -> datetime:
        """获取当前时间"""
        tz = timezone(timedelta(hours=8))  # 北京时间
        return datetime.now(tz)
    
    @staticmethod
    def format_datetime(dt: datetime, format_str: str = "%Y-%m-%d %H:%M:%S") -> str:
        """格式化日期时间"""
        return dt.strftime(format_str)
    
    @staticmethod
    def parse_datetime(date_str: str, format_str: str = "%Y-%m-%d %H:%M:%S") -> Optional[datetime]:
        """解析日期时间字符串"""
        try:
            return datetime.strptime(date_str, format_str)
        except ValueError:
            return None

class TextProcessor:
    """文本处理工具"""
    
    @staticmethod
    def clean_text(text: str) -> str:
        """清理文本，去除多余空白字符"""
        if not text:
            return ""
        
        # 去除首尾空白
        text = text.strip()
        
        # 将多个连续空白字符替换为单个空格
        text = re.sub(r'\s+', ' ', text)
        
        return text
    
    @staticmethod
    def extract_mentions(text: str) -> List[str]:
        """提取@提及的用户名"""
        pattern = r'@([^\s@]+)'
        mentions = re.findall(pattern, text)
        return mentions
    
    @staticmethod
    def remove_mentions(text: str) -> str:
        """移除@提及"""
        pattern = r'@[^\s@]+'
        return re.sub(pattern, '', text).strip()
    
    @staticmethod
    def extract_template_content(text: str, template_pattern: str) -> Optional[str]:
        """提取模板内容"""
        match = re.search(template_pattern, text)
        if match:
            return match.group(1).strip()
        return None

class ContactDeduplicator:
    """联系方式去重器"""
    
    @staticmethod
    def normalize_contact(contact_type: str, contact_value: str) -> str:
        """标准化联系方式"""
        if contact_type == '微信号':
            return contact_value.lower().strip()
        elif contact_type in ['手机号', '香港WS手机号']:
            # 移除所有非数字字符，保留+号
            normalized = re.sub(r'[^\d+]', '', contact_value)
            return normalized
        else:
            return contact_value.strip()
    
    @staticmethod
    def is_duplicate_contact(contact1: Dict[str, str], contact2: Dict[str, str]) -> bool:
        """判断两个联系方式是否重复"""
        if contact1['type'] != contact2['type']:
            return False
        
        normalized1 = ContactDeduplicator.normalize_contact(contact1['type'], contact1['value'])
        normalized2 = ContactDeduplicator.normalize_contact(contact2['type'], contact2['value'])
        
        return normalized1 == normalized2

class MessageFormatter:
    """消息格式化工具"""
    
    @staticmethod
    def format_success_response(message: str, data: Optional[Dict] = None) -> Dict[str, Any]:
        """格式化成功响应"""
        response = {
            'success': True,
            'message': message,
            'timestamp': DateTimeHelper.get_current_time().isoformat()
        }
        if data:
            response['data'] = data
        return response
    
    @staticmethod
    def format_error_response(error: str, code: Optional[str] = None) -> Dict[str, Any]:
        """格式化错误响应"""
        response = {
            'success': False,
            'error': error,
            'timestamp': DateTimeHelper.get_current_time().isoformat()
        }
        if code:
            response['error_code'] = code
        return response
    
    @staticmethod
    def format_query_result(data: List[Dict], total: int, query: str) -> Dict[str, Any]:
        """格式化查询结果"""
        return {
            'success': True,
            'query': query,
            'total': total,
            'data': data,
            'timestamp': DateTimeHelper.get_current_time().isoformat()
        }

class ConfigHelper:
    """配置工具"""
    
    @staticmethod
    def get_role_display_name(role: str) -> str:
        """获取角色显示名称"""
        role_names = {
            '社媒': '社媒人员',
            '合伙人': '合伙人',
            '销售': '销售人员'
        }
        return role_names.get(role, role)
    
    @staticmethod
    def get_status_display_name(status: str) -> str:
        """获取状态显示名称"""
        status_names = {
            '待接手': '待接手',
            '待分配': '待分配',
            '合伙人跟进中': '合伙人跟进中',
            '销售跟进中': '销售跟进中',
            '已成交': '已成交',
            '已流失': '已流失'
        }
        return status_names.get(status, status)