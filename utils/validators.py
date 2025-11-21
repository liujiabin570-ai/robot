"""
数据验证工具模块
"""
import re
from typing import Optional, Dict, Any

class MessageValidator:
    """消息验证器"""
    
    # 平台来源枚举
    VALID_PLATFORMS = {'抖音', '小红书', '微信公众号', '快手', 'B站', '其他'}
    
    # 联系方式类型枚举（同时接受别名，但入库统一为规范值）
    VALID_CONTACT_TYPES = {'微信号', '手机号', '香港WS手机号', '微信二维码', '微信二维码昵称'}
    
    # 人设枚举
    VALID_PERSONAS = {'家长号', '机构号', '老师号', '校方号', '其他'}
    
    # 意向度枚举
    VALID_INTENT_LEVELS = {'低', '中', '高'}
    
    # 反馈类型枚举
    VALID_FEEDBACK_TYPES = {'当日', '3天内', '7天内', '7天后', '其他'}

    # 业务类型枚举
    VALID_SERVICE_CATEGORIES = {'DSE', '插班和相关培训', '外教', '中文'}

    # 销售团队枚举
    VALID_SALES_TEAMS = {'广州', '深圳'}

    # 跟进阶段枚举
    VALID_FOLLOWUP_STAGES = {
        '联系不上', '初步清洗', '成功约了首Call', '决策是否去香港', '对比机构阶段', '邀约到访/测评', '谈方案中'
    }
    
    @staticmethod
    def validate_wechat_id(wechat_id: str) -> bool:
        """验证微信号格式"""
        if not wechat_id:
            return False
        
        # 微信号不能是纯数字
        if wechat_id.isdigit():
            return False
        
        # 长度5-20，允许字母、数字、下划线、短横线
        pattern = r'^[a-zA-Z0-9_-]{5,20}$'
        return bool(re.match(pattern, wechat_id))
    
    @staticmethod
    def validate_phone_number(phone: str) -> bool:
        """验证大陆手机号格式"""
        if not phone:
            return False
        
        # 11位，以1开头
        pattern = r'^1[3-9]\d{9}$'
        return bool(re.match(pattern, phone))
    
    @staticmethod
    def validate_hk_phone(phone: str) -> bool:
        """验证香港手机号格式"""
        if not phone:
            return False
        
        # 支持+852前缀或8位纯数字
        if phone.startswith('+852'):
            # +852后面跟8位数字
            pattern = r'^\+852\d{8}$'
            return bool(re.match(pattern, phone))
        else:
            # 8位纯数字
            pattern = r'^\d{8}$'
            return bool(re.match(pattern, phone))
    
    @staticmethod
    def validate_contact_value(contact_type: str, contact_value: str) -> tuple[bool, str]:
        """验证联系方式值"""
        if not contact_value:
            return False, "联系方式不能为空"
        
        if contact_type == '微信号':
            if not MessageValidator.validate_wechat_id(contact_value):
                return False, "微信号格式不正确，应为5-20位字母数字下划线短横线组合，不能是纯数字"
        elif contact_type == '手机号':
            if not MessageValidator.validate_phone_number(contact_value):
                return False, "手机号格式不正确，应为11位数字，以1开头"
        elif contact_type == '香港WS手机号':
            if not MessageValidator.validate_hk_phone(contact_value):
                return False, "香港手机号格式不正确，应为+852开头或8位纯数字"
        elif contact_type in ('微信二维码', '微信二维码昵称'):
            # 微信二维码只需要非空即可
            pass
        else:
            return False, f"不支持的联系方式类型: {contact_type}"
        
        return True, ""

    @staticmethod
    def normalize_contact_type(contact_type: str) -> str:
        """将联系方式类型规范化为数据库枚举值"""
        ct = (contact_type or '').strip()
        if ct in ('微信二维码', '微信二维码昵称'):
            return '微信二维码昵称'
        return ct

    @staticmethod
    def normalize_feedback_type(feedback_type: str) -> str:
        """将反馈类型规范化为数据库枚举值
        - 兼容常见中文数字写法与口语别称
        - 仅做保守映射，未匹配则原样返回
        """
        ft = (feedback_type or '').strip()
        mapping = {
            '当天': '当日',
            '当日内': '当日',
            '今日': '当日',
            '今天': '当日',
            '三天内': '3天内',
            '三日内': '3天内',
            '7日内': '7天内',
            '七天内': '7天内',
            '七日内': '7天内',
            '一周内': '7天内',
            '七天后': '7天后',
            '七日后': '7天后',
            '一周后': '7天后'
        }
        return mapping.get(ft, ft)

    @staticmethod
    def parse_yes_no_to_int(value: str) -> Optional[int]:
        """将是/否解析为0/1，兼容大小写与常见别称"""
        if value is None:
            return None
        v = str(value).strip().lower()
        if v in {'是', 'yes', 'y', 'true', '1'}:
            return 1
        if v in {'否', 'no', 'n', 'false', '0'}:
            return 0
        return None
    
    @staticmethod
    def validate_enum_value(value: str, valid_values: set, field_name: str) -> tuple[bool, str]:
        """验证枚举值
        - 更健壮的匹配：去除所有空白（含全角/不可见空白），并进行基本大小写统一
        - 目的：兼容聊天输入中的特殊空格或大小写差异（如 DSE/dse）
        """
        # 标准化空白与大小写
        v = '' if value is None else str(value)
        # 去除首尾空白并压缩/移除所有空白字符（包含中文/全角空格、NBSP等）
        v = v.strip()
        v = re.sub(r"\s+", "", v)
        # 若值为纯ASCII字母数字/下划线/短横线，统一为大写以兼容 DSE 等写法
        if re.fullmatch(r"[A-Za-z0-9_-]+", v or ""):
            v = v.upper()

        if v not in valid_values:
            return False, f"{field_name}必须是以下值之一: {', '.join(valid_values)}"
        return True, ""
    
    @staticmethod
    def validate_amount(amount_str: str) -> tuple[bool, str, Optional[float]]:
        """验证金额格式"""
        try:
            amount = float(amount_str)
            if amount < 0:
                return False, "金额不能为负数", None
            if amount > 999999999.99:
                return False, "金额过大", None
            return True, "", amount
        except ValueError:
            return False, "金额格式不正确，请输入数字", None

class BusinessValidator:
    """业务逻辑验证器"""
    
    # 状态流转规则
    STATE_TRANSITIONS = {
        # 新逻辑：引入“待接手”，新家长分配后进入待接手，由被分配合伙人确认接手
        '待接手': ['合伙人跟进中', '销售跟进中'],
        # 兼容旧数据：历史上可能存在“待分配”，不再允许自动接手
        '待分配': [],
        '合伙人跟进中': ['已流失', '销售跟进中'],
        '销售跟进中': ['已成交', '已流失'],
        '已成交': [],
        '已流失': []
    }
    
    @staticmethod
    def validate_state_transition(current_state: str, target_state: str) -> tuple[bool, str]:
        """验证状态流转是否合法"""
        if current_state not in BusinessValidator.STATE_TRANSITIONS:
            return False, f"未知的当前状态: {current_state}"
        
        valid_targets = BusinessValidator.STATE_TRANSITIONS[current_state]
        if target_state not in valid_targets:
            if not valid_targets:
                return False, f"状态 {current_state} 不能再进行流转"
            else:
                return False, f"状态 {current_state} 只能流转到: {', '.join(valid_targets)}"
        
        return True, ""
    
    @staticmethod
    def get_next_valid_states(current_state: str) -> list:
        """获取当前状态可以流转到的下一个状态"""
        return BusinessValidator.STATE_TRANSITIONS.get(current_state, [])
    
    @staticmethod
    def validate_role_permission(role: str, action: str) -> tuple[bool, str]:
        """验证角色权限"""
        role_permissions = {
            '社媒': ['新家长'],
            '合伙人': ['补全微信号', '合伙人接手', '放弃', '转销售'],
            '销售': ['销售接手', '反馈', '成交', '流失']
        }
        
        if role not in role_permissions:
            return False, f"未知角色: {role}"
        
        if action not in role_permissions[role]:
            return False, f"角色 {role} 无权执行操作: {action}"
        
        return True, ""