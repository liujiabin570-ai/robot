"""
业务服务模块
"""
from typing import Dict, Any, Optional, List
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

from models.database import (
    SessionLocal, StaffMapping, Parents, ParentContacts, 
    ProcessLogs, FollowupFeedback, ChangeLogs
)
from parsers.message_parser import ParsedMessage
from utils.validators import BusinessValidator, MessageValidator
from utils.helpers import IDGenerator, DateTimeHelper, ContactDeduplicator, MessageFormatter, TextProcessor
from utils.logger import app_logger

class BusinessService:
    """业务服务类"""
    
    def __init__(self):
        self.validator = BusinessValidator()
        self.message_validator = MessageValidator()
        self.id_generator = IDGenerator()
        self.contact_deduplicator = ContactDeduplicator()
        self.text_processor = TextProcessor()
    
    def process_message(self, parsed_message: ParsedMessage) -> Dict[str, Any]:
        """处理解析后的消息"""
        try:
            # 确保并获取发送者权限（不存在则自动创建并启用）
            sender_info = self._ensure_staff_mapping(parsed_message.sender, parsed_message.message_type)
            if not sender_info:
                normalized_sender = self._normalize_wechat_name(parsed_message.sender) or parsed_message.sender
                if not self._has_valid_role_prefix(normalized_sender):
                    return MessageFormatter.format_error_response(
                        "群昵称缺少或包含未知前缀，请按规范设置：SM_/HP_/XS_。例如：XS_销售张三、HP_合伙人王五、SM_社媒赵六"
                    )
                return MessageFormatter.format_error_response(f"未找到或创建发送者信息失败: {parsed_message.sender}")
            
            # 验证角色权限
            is_valid, error = self.validator.validate_role_permission(
                sender_info['role'], parsed_message.message_type
            )
            if not is_valid:
                return MessageFormatter.format_error_response(error)
            
            # 根据消息类型处理
            handler_map = {
                '新家长': self._handle_new_parent,
                '补全微信号': self._handle_complete_wechat,
                '合伙人接手': self._handle_take_over,
                '放弃': self._handle_abandon,
                '转销售': self._handle_transfer_to_sales,
                '销售接手': self._handle_sales_take_over,
                '反馈': self._handle_feedback,
                '成交': self._handle_deal_closed,
                '流失': self._handle_lost
            }
            
            handler = handler_map.get(parsed_message.message_type)
            if not handler:
                return MessageFormatter.format_error_response(f"不支持的消息类型: {parsed_message.message_type}")
            
            return handler(parsed_message, sender_info)
            
        except Exception as e:
            app_logger.error(f"处理业务消息失败: {e}")
            return MessageFormatter.format_error_response(f"处理失败: {str(e)}")
    
    def _get_sender_info(self, sender: str) -> Optional[Dict[str, Any]]:
        """获取发送者信息"""
        db = SessionLocal()
        try:
            normalized_sender = self._normalize_wechat_name(sender) or sender
            staff = db.query(StaffMapping).filter(
                StaffMapping.staff_id == normalized_sender,
                StaffMapping.is_active == 1
            ).first()
            
            if staff:
                return {
                    'staff_id': staff.staff_id,
                    'role': staff.role
                }
            return None
            
        except Exception as e:
            app_logger.error(f"获取发送者信息失败: {e}")
            return None
        finally:
            db.close()

    def _derive_role_from_action(self, action: str) -> str:
        """根据消息类型推断角色"""
        mapping = {
            '新家长': '社媒',
            '补全微信号': '合伙人',
            '接手': '合伙人',  # 兼容旧标签
            '合伙人接手': '合伙人',
            '放弃': '合伙人',
            '转销售': '合伙人',
            '销售接手': '销售',
            '反馈': '销售',
            '成交': '销售',
            '流失': '销售'
        }
        return mapping.get(action, '社媒')

    def _derive_role_from_staff_id(self, staff_id: Optional[str]) -> Optional[str]:
        """根据群昵称前缀（前两位字母，必须位于昵称起始）推断角色。
        - SM -> 社媒
        - HP -> 合伙人
        - XS -> 销售
        - 缺失或未知前缀 -> 返回 None（不自动默认角色）
        """
        if not staff_id:
            return None
        s = (staff_id or '').strip()
        # 去除可能的@前缀与首尾空白
        if s.startswith('@'):
            s = s.lstrip('@').strip()
        # 若包含下划线，优先按下划线前的片段作为前缀；否则整个昵称作为前缀候选
        prefix = s.split('_', 1)[0] if '_' in s else s
        # 仅在字符串起始位置匹配两位字母作为前缀
        import re as _re
        m = _re.match(r'^[A-Za-z]{2}', prefix)
        letters = m.group(0).upper() if m else ''
        if letters == 'SM':
            return '社媒'
        if letters == 'HP':
            return '合伙人'
        if letters == 'XS':
            return '销售'
        return None

    def _has_valid_role_prefix(self, staff_id: Optional[str]) -> bool:
        """检查群昵称是否包含有效的角色前缀（起始两位字母：SM/HP/XS）。"""
        role = self._derive_role_from_staff_id(staff_id)
        return role in {'社媒', '合伙人', '销售'}

    def _normalize_wechat_name(self, name: Optional[str]) -> Optional[str]:
        """规范化昵称：移除@提及，保留纯昵称"""
        if not name:
            return name
        n = name.strip()
        if '@' in n or n.startswith('@'):
            mentions = self.text_processor.extract_mentions(n)
            if mentions:
                n = mentions[0]
            else:
                n = n.lstrip('@').strip()
        return n

    def _ensure_staff_mapping(self, sender: str, action: str) -> Optional[Dict[str, Any]]:
        """确保人员映射存在；不存在则按动作推断角色并自动创建/启用"""
        db = SessionLocal()
        try:
            normalized_sender = self._normalize_wechat_name(sender) or sender
            # 仅以 staff_id 作为唯一键查询
            staff_norm = db.query(StaffMapping).filter(StaffMapping.staff_id == normalized_sender).first()
            if staff_norm:
                # 若前缀可推断角色且与现有角色不一致，则按前缀对齐角色
                derived_role = self._derive_role_from_staff_id(normalized_sender)
                if derived_role and staff_norm.role != derived_role:
                    old_role = staff_norm.role
                    staff_norm.role = derived_role
                    db.add(staff_norm)
                    db.commit()
                    app_logger.info(f"按前缀对齐人员角色: staff_id={normalized_sender}, {old_role} -> {derived_role}")
                if staff_norm.is_active != 1:
                    staff_norm.is_active = 1
                    db.add(staff_norm)
                    db.commit()
                    app_logger.info(f"自动启用人员映射: staff_id={normalized_sender}, role={staff_norm.role}")
                return {
                    'staff_id': staff_norm.staff_id,
                    'role': staff_norm.role
                }

            # 两者都不存在则创建，角色由动作推断
            # 新规则：根据群昵称前缀（SM/HP/XS）直接推断角色
            role = self._derive_role_from_staff_id(normalized_sender)
            if role is None:
                # 未提供或未知前缀：不创建映射，提醒按规范设置群昵称
                app_logger.warning(f"缺少有效角色前缀，拒绝创建人员映射: staff_id={normalized_sender}")
                return None
            # 仅使用 staff_id（群昵称，含前缀 SM_/HP_/XS_）
            new_staff_id = normalized_sender
            new_staff = StaffMapping(
                staff_id=new_staff_id,
                role=role,
                is_active=1
            )
            db.add(new_staff)
            db.commit()
            app_logger.info(f"自动创建人员映射: staff_id={normalized_sender}, role={role} (前缀匹配)")
            return {
                'staff_id': new_staff_id,
                'role': role
            }
        except Exception as e:
            db.rollback()
            app_logger.error(f"确保人员映射失败: {e}")
            return None
        finally:
            db.close()
    
    def _handle_new_parent(self, parsed_message: ParsedMessage, sender_info: Dict[str, Any]) -> Dict[str, Any]:
        """处理新家长消息"""
        db = SessionLocal()
        try:
            content = parsed_message.content
            
            # 生成家长编号
            parent_code = self.id_generator.generate_parent_code()
            
            # 校验业务类型（必填）
            service_category = content.get('service_category')
            is_valid_sc, err_sc = self.message_validator.validate_enum_value(
                service_category, self.message_validator.VALID_SERVICE_CATEGORIES, '业务类型'
            ) if service_category else (False, '业务类型缺失')
            if not is_valid_sc:
                return MessageFormatter.format_error_response(err_sc)

            # 解析联系方式
            contact_str = content['contact']
            contact_type, contact_value = contact_str.split(':', 1)
            contact_type = contact_type.strip()
            contact_value = contact_value.strip()
            # 规范化联系类型以匹配DB枚举（如将“微信二维码”统一为“微信二维码昵称”）
            normalized_type = self.message_validator.normalize_contact_type(contact_type)
            if normalized_type != contact_type:
                app_logger.info(f"规范化联系类型: {contact_type} -> {normalized_type}")
            
            # 检查联系方式是否重复
            existing_contact = db.query(ParentContacts).filter(
                ParentContacts.contact_type == normalized_type,
                ParentContacts.contact_value == contact_value
            ).first()
            
            if existing_contact:
                parent = db.query(Parents).filter(Parents.id == existing_contact.parent_id).first()
                return MessageFormatter.format_error_response(
                    f"联系方式已存在，关联家长编号: {parent.parent_code}"
                )
            
            # 创建家长记录（字段对齐 models.Parents）
            # 可选字段
            requirement = content.get('requirement')
            intent_level = content.get('intent_level')

            # 新规则：新家长必须分配；创建后进入“待接手”，由被分配人（合伙人或销售）确认
            assignee_name = content.get('assignee')
            assignee_name = self._normalize_wechat_name(assignee_name) or assignee_name
            # 分配对象必须携带有效前缀
            assignee_prefix_role = self._derive_role_from_staff_id(assignee_name)
            if not assignee_prefix_role:
                return MessageFormatter.format_error_response(
                    "分配对象昵称缺少或包含未知前缀，请按规范设置：SM_/HP_/XS_。例如：XS_销售张三、HP_合伙人王五"
                )
            partner_id_value = ''
            salesperson_id_value = None
            sales_team_value = None
            current_status_value = '待接手'
            # 优先查找已登记的人员映射；若不存在则按“接手”逻辑创建（默认合伙人）
            assignee_staff = db.query(StaffMapping).filter(StaffMapping.staff_id == assignee_name).first()
            if not assignee_staff:
                created = self._ensure_staff_mapping(assignee_name, '合伙人接手')
                if created:
                    assignee_staff = db.query(StaffMapping).filter(StaffMapping.staff_id == assignee_name).first()
            if assignee_staff:
                # 若登记角色与前缀不一致，则对齐角色
                if assignee_staff.role != assignee_prefix_role:
                    old_role = assignee_staff.role
                    assignee_staff.role = assignee_prefix_role
                    db.add(assignee_staff)
                    db.commit()
                    app_logger.info(f"对齐分配对象角色: staff_id={assignee_staff.staff_id}, {old_role} -> {assignee_prefix_role}")
                if assignee_prefix_role == '合伙人':
                    partner_id_value = assignee_staff.staff_id
                elif assignee_prefix_role == '销售':
                    salesperson_id_value = assignee_staff.staff_id
                    sales_team_value = assignee_staff.sales_team
                else:
                    # 若角色为社媒，仍按合伙人处理（兼容旧登记）
                    partner_id_value = assignee_staff.staff_id
                app_logger.info(
                    f"新家长分配给: {assignee_name} (staff_id={assignee_staff.staff_id}, role={assignee_staff.role})，状态设为待接手"
                )
            else:
                # 防御性回退：当人员映射刚创建但当前事务未能立即可见，按昵称直接绑定ID，避免后续“未分配销售”
                if assignee_prefix_role == '销售':
                    salesperson_id_value = assignee_name
                    app_logger.warning(
                        f"分配对象未能在当前会话读取，按昵称绑定销售ID: {assignee_name}"
                    )
                elif assignee_prefix_role == '合伙人':
                    partner_id_value = assignee_name
                    app_logger.warning(
                        f"分配对象未能在当前会话读取，按昵称绑定合伙人ID: {assignee_name}"
                    )
                else:
                    app_logger.warning(f"分配失败：未能创建或获取分配对象 {assignee_name}，状态仍设为待接手")

            # 推荐渠道归因规则：
            # - 社媒直接@销售 → 社媒
            # - 社媒@合伙人（合伙人再@销售） → 合伙人
            # - 合伙人发起 → 合伙人
            is_social_sender = (sender_info['role'] == '社媒')
            is_partner_sender = (sender_info['role'] == '合伙人')
            is_assignee_partner = (assignee_prefix_role == '合伙人')
            if is_partner_sender or (is_social_sender and is_assignee_partner):
                recommend_channel_value = '合伙人'
            else:
                recommend_channel_value = '社媒'

            parent = Parents(
                parent_code=parent_code,
                recommend_channel=recommend_channel_value,
                source_platform=content['source'],
                service_category=service_category,
                requirement=requirement,
                current_status=current_status_value,
                social_media_id=sender_info['staff_id'],
                partner_id=partner_id_value,
                salesperson_id=salesperson_id_value,
                sales_team=sales_team_value,
                intent_level=intent_level,
                is_dse=1 if service_category == 'DSE' else 0,
                student_id=(contact_value if normalized_type != '微信二维码昵称' else None),
                created_at=DateTimeHelper.get_current_time(),
                updated_at=DateTimeHelper.get_current_time()
            )
            
            db.add(parent)
            db.flush()  # 获取parent.id
            
            # 创建联系方式记录
            contact = ParentContacts(
                parent_id=parent.id,
                contact_type=normalized_type,
                contact_value=contact_value,
                is_primary=0 if normalized_type == '微信二维码昵称' else 1,
                created_at=DateTimeHelper.get_current_time()
            )
            
            db.add(contact)
            
            # 创建处理日志（字段对齐 models.ProcessLogs）
            _assignee_id = assignee_staff.staff_id if assignee_staff else (
                assignee_name if assignee_prefix_role in ('合伙人', '销售') else None
            )
            _assignee_role = assignee_staff.role if assignee_staff else assignee_prefix_role
            _assignee_team = assignee_staff.sales_team if assignee_staff else None
            log = ProcessLogs(
                parent_id=parent.id,
                action_type='新家长',
                operator_id=sender_info['staff_id'],
                operator_role=sender_info['role'],
                assignee_id=_assignee_id,
                assignee_role=_assignee_role,
                assignee_team=_assignee_team,
                message_content=parsed_message.raw_message,
                notes=self._compose_notes_for_new_parent(content),
                created_at=DateTimeHelper.get_current_time()
            )
            
            db.add(log)
            db.commit()
            
            return MessageFormatter.format_success_response(
                f"✅ 新家长录入并分配成功！\n家长编号：{parent_code}\n业务类型：{service_category}\n分配给：{assignee_name}\n状态：待接手\n请被分配人员确认接手"
            )
            
        except Exception as e:
            db.rollback()
            app_logger.error(f"处理新家长失败: {e}")
            return MessageFormatter.format_error_response(f"录入失败: {str(e)}")
        finally:
            db.close()

    def _compose_notes_for_new_parent(self, content: Dict[str, Any]) -> Optional[str]:
        """组合新家长的备注信息，兼容需求与分配给"""
        notes = []
        remark = content.get('remark')
        requirement = content.get('requirement')
        assignee = content.get('assignee')
        if requirement:
            notes.append(f"需求:{requirement}")
        if assignee:
            notes.append(f"分配给:{assignee}")
        if remark:
            notes.append(remark)
        return '\n'.join(notes) if notes else None
    
    def _handle_complete_wechat(self, parsed_message: ParsedMessage, sender_info: Dict[str, Any]) -> Dict[str, Any]:
        """处理补全微信号消息"""
        db = SessionLocal()
        try:
            content = parsed_message.content
            parent_code = content['parent_code']
            wechat_id = content['wechat_id']
            
            # 查找家长
            parent = db.query(Parents).filter(Parents.parent_code == parent_code).first()
            if not parent:
                return MessageFormatter.format_error_response(f"未找到家长编号: {parent_code}")
            
            # 验证状态：仅允许“待接手”或“合伙人跟进中”时补全微信号
            if parent.current_status not in ('待接手', '合伙人跟进中'):
                return MessageFormatter.format_error_response(f"家长状态不正确，当前状态: {parent.current_status}")
            # 权限校验：必须为被分配的合伙人
            if not parent.partner_id:
                return MessageFormatter.format_error_response("家长未分配合伙人，无法补全微信号")
            if parent.partner_id != sender_info['staff_id']:
                return MessageFormatter.format_error_response("只能由被分配的合伙人补全微信号")
            
            # 检查微信号是否已存在
            existing_contact = db.query(ParentContacts).filter(
                ParentContacts.contact_type == '微信号',
                ParentContacts.contact_value == wechat_id
            ).first()
            
            if existing_contact and existing_contact.parent_id != parent.id:
                existing_parent = db.query(Parents).filter(Parents.id == existing_contact.parent_id).first()
                return MessageFormatter.format_error_response(
                    f"微信号已存在，关联家长编号: {existing_parent.parent_code}"
                )
            
            # 添加或更新微信号
            contact = db.query(ParentContacts).filter(
                ParentContacts.parent_id == parent.id,
                ParentContacts.contact_type == '微信号'
            ).first()
            
            if contact:
                contact.contact_value = wechat_id
                contact.is_primary = 1
                contact.is_verified = 1
            else:
                contact = ParentContacts(
                    parent_id=parent.id,
                    contact_type='微信号',
                    contact_value=wechat_id,
                    is_primary=1,
                    is_verified=1,
                    created_at=DateTimeHelper.get_current_time()
                )
                db.add(contact)
            
            # 更新家长状态为合伙人跟进中，分配给当前合伙人
            parent.current_status = '合伙人跟进中'
            parent.partner_id = sender_info['staff_id']
            parent.student_id = wechat_id
            parent.updated_at = DateTimeHelper.get_current_time()
            
            # 创建处理日志
            log = ProcessLogs(
                parent_id=parent.id,
                action_type='补全微信号',
                operator_id=sender_info['staff_id'],
                operator_role=sender_info['role'],
                message_content=parsed_message.raw_message,
                notes=content.get('remark'),
                created_at=DateTimeHelper.get_current_time()
            )
            
            db.add(log)
            db.commit()
            
            return MessageFormatter.format_success_response(
                f"✅ 微信号补全成功！\n家长编号：{parent_code}\n微信号：{wechat_id}\n状态：合伙人跟进中"
            )
            
        except Exception as e:
            db.rollback()
            app_logger.error(f"处理补全微信号失败: {e}")
            return MessageFormatter.format_error_response(f"补全失败: {str(e)}")
        finally:
            db.close()
    
    def _handle_take_over(self, parsed_message: ParsedMessage, sender_info: Dict[str, Any]) -> Dict[str, Any]:
        """处理合伙人接手消息"""
        db = SessionLocal()
        try:
            content = parsed_message.content
            parent_code = content['parent_code']
            
            # 查找家长
            parent = db.query(Parents).filter(Parents.parent_code == parent_code).first()
            if not parent:
                return MessageFormatter.format_error_response(f"未找到家长编号: {parent_code}")
            
            # 验证状态：仅允许“待接手”时接手
            if parent.current_status != '待接手':
                return MessageFormatter.format_error_response(f"家长状态不正确，当前状态: {parent.current_status}")
            # 权限校验：只能由被分配的合伙人接手确认
            if not parent.partner_id:
                return MessageFormatter.format_error_response("家长未分配合伙人，无法接手")
            if parent.partner_id != sender_info['staff_id']:
                return MessageFormatter.format_error_response("只能由被分配的合伙人接手")
            
            # 更新家长状态
            old_status = parent.current_status
            parent.current_status = '合伙人跟进中'
            parent.partner_id = sender_info['staff_id']
            parent.updated_at = DateTimeHelper.get_current_time()
            
            # 创建处理日志
            log = ProcessLogs(
                parent_id=parent.id,
                action_type='合伙人接手',
                operator_id=sender_info['staff_id'],
                operator_role=sender_info['role'],
                message_content=parsed_message.raw_message,
                notes=content.get('remark'),
                created_at=DateTimeHelper.get_current_time()
            )
            
            db.add(log)
            db.commit()
            
            return MessageFormatter.format_success_response(
                f"✅ 合伙人接手成功！\n家长编号：{parent_code}\n状态：合伙人跟进中"
            )
            
        except Exception as e:
            db.rollback()
            app_logger.error(f"处理接手失败: {e}")
            return MessageFormatter.format_error_response(f"接手失败: {str(e)}")
        finally:
            db.close()
    
    def _handle_abandon(self, parsed_message: ParsedMessage, sender_info: Dict[str, Any]) -> Dict[str, Any]:
        """处理放弃消息"""
        db = SessionLocal()
        try:
            content = parsed_message.content
            parent_code = content['parent_code']
            reason = content['reason']
            
            # 查找家长
            parent = db.query(Parents).filter(Parents.parent_code == parent_code).first()
            if not parent:
                return MessageFormatter.format_error_response(f"未找到家长编号: {parent_code}")
            
            # 验证状态和权限
            if parent.current_status != '合伙人跟进中':
                return MessageFormatter.format_error_response(f"家长状态不正确，当前状态: {parent.current_status}")
            
            if parent.partner_id != sender_info['staff_id']:
                return MessageFormatter.format_error_response("只能放弃自己跟进的家长")
            
            # 更新家长状态
            old_status = parent.current_status
            parent.current_status = '已流失'
            parent.updated_at = DateTimeHelper.get_current_time()
            
            # 创建处理日志
            log = ProcessLogs(
                parent_id=parent.id,
                action_type='放弃',
                operator_id=sender_info['staff_id'],
                operator_role=sender_info['role'],
                message_content=parsed_message.raw_message,
                notes=f"放弃原因: {reason}. {content.get('remark', '')}".strip(),
                created_at=DateTimeHelper.get_current_time()
            )
            
            db.add(log)
            db.commit()
            
            return MessageFormatter.format_success_response(
                f"✅ 放弃成功！\n家长编号：{parent_code}\n原因：{reason}\n状态：已流失"
            )
            
        except Exception as e:
            db.rollback()
            app_logger.error(f"处理放弃失败: {e}")
            return MessageFormatter.format_error_response(f"放弃失败: {str(e)}")
        finally:
            db.close()
    
    def _handle_transfer_to_sales(self, parsed_message: ParsedMessage, sender_info: Dict[str, Any]) -> Dict[str, Any]:
        """处理转销售消息"""
        db = SessionLocal()
        try:
            content = parsed_message.content
            parent_code = content['parent_code']
            
            # 查找家长
            parent = db.query(Parents).filter(Parents.parent_code == parent_code).first()
            if not parent:
                return MessageFormatter.format_error_response(f"未找到家长编号: {parent_code}")
            
            # 验证状态和权限
            if parent.current_status != '合伙人跟进中':
                return MessageFormatter.format_error_response(f"家长状态不正确，当前状态: {parent.current_status}")
            
            if parent.partner_id != sender_info['staff_id']:
                return MessageFormatter.format_error_response("只能转移自己跟进的家长")
            
            # 必须明确指定分配销售
            assignee_name = content.get('assignee')
            assignee_name = self._normalize_wechat_name(assignee_name) or assignee_name
            if not assignee_name:
                return MessageFormatter.format_error_response("请在模板中使用“HP_分配给:@销售昵称”明确分配销售")

            sales_staff = db.query(StaffMapping).filter(StaffMapping.staff_id == assignee_name).first()
            if not sales_staff:
                created = self._ensure_staff_mapping(assignee_name, '销售接手')
                if created:
                    sales_staff = db.query(StaffMapping).filter(StaffMapping.staff_id == assignee_name).first()
            if not sales_staff:
                derived_role = self._derive_role_from_staff_id(assignee_name)
                if derived_role == '销售':
                    parent.salesperson_id = assignee_name
                    parent.sales_team = None
                    old_status = parent.current_status
                    parent.current_status = '销售跟进中'
                    parent.updated_at = DateTimeHelper.get_current_time()
                    log = ProcessLogs(
                        parent_id=parent.id,
                        action_type='转销售',
                        operator_id=sender_info['staff_id'],
                        operator_role=sender_info['role'],
                        assignee_id=assignee_name,
                        assignee_role='销售',
                        assignee_team=None,
                        message_content=parsed_message.raw_message,
                        notes=content.get('remark'),
                        created_at=DateTimeHelper.get_current_time()
                    )
                    db.add(log)
                    db.commit()
                    return MessageFormatter.format_success_response(
                        f"✅ 转销售成功！\n家长编号：{parent_code}\n分配销售：{assignee_name}\n团队：未登记\n状态：销售跟进中"
                    )
                return MessageFormatter.format_error_response("分配对象不是销售或未登记，请先登记该销售昵称及角色")
            if sales_staff.role != '销售':
                derived_role = self._derive_role_from_staff_id(assignee_name)
                if derived_role == '销售':
                    sales_staff.role = '销售'
                    db.add(sales_staff)
                    db.commit()
                else:
                    return MessageFormatter.format_error_response("分配对象不是销售或未登记，请先登记该销售昵称及角色")

            # 绑定销售与团队快照，并更新状态
            parent.salesperson_id = sales_staff.staff_id
            parent.sales_team = sales_staff.sales_team
            old_status = parent.current_status
            parent.current_status = '销售跟进中'
            parent.updated_at = DateTimeHelper.get_current_time()
            
            # 创建处理日志
            log = ProcessLogs(
                parent_id=parent.id,
                action_type='转销售',
                operator_id=sender_info['staff_id'],
                operator_role=sender_info['role'],
                assignee_id=sales_staff.staff_id,
                assignee_role=sales_staff.role,
                assignee_team=sales_staff.sales_team,
                message_content=parsed_message.raw_message,
                notes=content.get('remark'),
                created_at=DateTimeHelper.get_current_time()
            )
            
            db.add(log)
            db.commit()
            
            return MessageFormatter.format_success_response(
                f"✅ 转销售成功！\n家长编号：{parent_code}\n分配销售：{assignee_name}\n团队：{sales_staff.sales_team or '未登记'}\n状态：销售跟进中"
            )
            
        except Exception as e:
            db.rollback()
            app_logger.error(f"处理转销售失败: {e}")
            return MessageFormatter.format_error_response(f"转销售失败: {str(e)}")
        finally:
            db.close()
    
    def _handle_sales_take_over(self, parsed_message: ParsedMessage, sender_info: Dict[str, Any]) -> Dict[str, Any]:
        """处理销售接手消息"""
        db = SessionLocal()
        try:
            content = parsed_message.content
            parent_code = content['parent_code']
            
            # 查找家长
            parent = db.query(Parents).filter(Parents.parent_code == parent_code).first()
            if not parent:
                return MessageFormatter.format_error_response(f"未找到家长编号: {parent_code}")
            
            # 验证状态：支持“待接手”由指定销售接手，或“销售跟进中”由当前销售确认接手
            if parent.current_status == '待接手':
                if not parent.salesperson_id:
                    return MessageFormatter.format_error_response("该家长未分配销售，无法接手")
                if parent.salesperson_id != sender_info['staff_id']:
                    return MessageFormatter.format_error_response("仅被分配的销售可接手")
                parent.current_status = '销售跟进中'
            elif parent.current_status == '销售跟进中':
                if parent.salesperson_id and parent.salesperson_id != sender_info['staff_id']:
                    return MessageFormatter.format_error_response("该家长已归属其他销售")
                parent.salesperson_id = sender_info['staff_id']
            else:
                return MessageFormatter.format_error_response(f"家长状态不正确，当前状态: {parent.current_status}")

            # 团队（可选）：若提供则更新销售人员团队并同步家长快照；未提供则沿用销售人员团队快照
            team = content.get('sales_team')
            sales_staff = db.query(StaffMapping).filter(StaffMapping.staff_id == sender_info['staff_id']).first()
            if team:
                ok_team, err_team = self.message_validator.validate_enum_value(team, self.message_validator.VALID_SALES_TEAMS, '团队')
                if not ok_team:
                    return MessageFormatter.format_error_response(err_team)
                if not sales_staff:
                    # 若不存在人员映射，自动创建（按前缀推断角色）
                    created = self._ensure_staff_mapping(sender_info['staff_id'], '销售接手')
                    if not created:
                        return MessageFormatter.format_error_response("无法创建销售人员映射，请确保群昵称以 XS_ 前缀开头")
                    sales_staff = db.query(StaffMapping).filter(StaffMapping.staff_id == sender_info['staff_id']).first()
                # 更新销售人员团队
                if sales_staff.sales_team != team:
                    sales_staff.sales_team = team
                    db.add(sales_staff)
                # 同步家长快照为提供的团队
                parent.sales_team = team
            else:
                # 未提供团队：沿用销售人员已有团队快照（若家长未设置）
                if sales_staff and sales_staff.sales_team and not parent.sales_team:
                    parent.sales_team = sales_staff.sales_team

            # 可选：覆盖意向度
            new_intent = content.get('intent_level')
            if new_intent:
                ok, err = self.message_validator.validate_enum_value(new_intent, self.message_validator.VALID_INTENT_LEVELS, '意向度')
                if not ok:
                    return MessageFormatter.format_error_response(err)
                if parent.intent_level != new_intent:
                    change = ChangeLogs(
                        parent_id=parent.id,
                        entity_type='parent',
                        field_name='intent_level',
                        old_value=parent.intent_level,
                        new_value=new_intent,
                        operator_id=sender_info['staff_id'],
                        created_at=DateTimeHelper.get_current_time()
                    )
                    db.add(change)
                parent.intent_level = new_intent
            parent.updated_at = DateTimeHelper.get_current_time()
            
            # 创建处理日志
            log = ProcessLogs(
                parent_id=parent.id,
                action_type='销售接手',
                operator_id=sender_info['staff_id'],
                operator_role=sender_info['role'],
                message_content=parsed_message.raw_message,
                notes=content.get('remark'),
                created_at=DateTimeHelper.get_current_time()
            )
            
            db.add(log)
            db.commit()
            
            # 成功响应增加团队信息（若存在）
            team_display = parent.sales_team or (sales_staff.sales_team if sales_staff else None)
            team_line = f"\n团队：{team_display}" if team_display else ""
            return MessageFormatter.format_success_response(
                f"✅ 销售接手成功！\n家长编号：{parent_code}\n销售：{sender_info['staff_id']}{team_line}\n状态：销售跟进中"
            )
            
        except Exception as e:
            db.rollback()
            app_logger.error(f"处理销售接手失败: {e}")
            return MessageFormatter.format_error_response(f"销售接手失败: {str(e)}")
        finally:
            db.close()
    
    def _handle_feedback(self, parsed_message: ParsedMessage, sender_info: Dict[str, Any]) -> Dict[str, Any]:
        """处理反馈消息"""
        db = SessionLocal()
        try:
            content = parsed_message.content
            parent_code = content['parent_code']
            # 规范化并验证反馈类型
            raw_feedback_type = content['feedback_type']
            feedback_type = self.message_validator.normalize_feedback_type(raw_feedback_type)
            if feedback_type != raw_feedback_type:
                app_logger.info(f"规范化反馈类型: {raw_feedback_type} -> {feedback_type}")
            is_valid, error = self.message_validator.validate_enum_value(
                feedback_type, self.message_validator.VALID_FEEDBACK_TYPES, "反馈类型"
            )
            if not is_valid:
                return MessageFormatter.format_error_response(error)
            # 反馈内容允许为空；若缺失则使用空字符串，以满足非空约束
            feedback_content = content.get('feedback_content', '').strip()
            # 解析 DSE 标志（是/否 -> 1/0），默认按否处理
            is_dse_flag = content.get('is_dse')
            is_dse_value = self.message_validator.parse_yes_no_to_int(is_dse_flag) or 0
            
            # 查找家长
            parent = db.query(Parents).filter(Parents.parent_code == parent_code).first()
            if not parent:
                return MessageFormatter.format_error_response(f"未找到家长编号: {parent_code}")
            
            # 验证状态和权限
            if parent.current_status != '销售跟进中':
                return MessageFormatter.format_error_response(f"家长状态不正确，当前状态: {parent.current_status}")
            
            if parent.salesperson_id != sender_info['staff_id']:
                return MessageFormatter.format_error_response("只能反馈自己跟进的家长")
            
            # 可选字段：跟进阶段、预收金额、是否上门、意向度
            fb_stage = content.get('followup_stage')
            if fb_stage:
                ok_stage, err_stage = self.message_validator.validate_enum_value(
                    fb_stage, self.message_validator.VALID_FOLLOWUP_STAGES, '跟进阶段'
                )
                if not ok_stage:
                    return MessageFormatter.format_error_response(err_stage)
            prepay_raw = content.get('prepayment_amount')
            prepay_val = None
            if prepay_raw:
                ok_amt, err_amt, val_amt = self.message_validator.validate_amount(prepay_raw)
                if not ok_amt:
                    return MessageFormatter.format_error_response(err_amt)
                prepay_val = val_amt
            is_visit_raw = content.get('is_visit')
            is_visit_val = None
            if is_visit_raw is not None:
                is_visit_val = self.message_validator.parse_yes_no_to_int(is_visit_raw)
                if is_visit_val is None:
                    return MessageFormatter.format_error_response("是否上门需为是/否")
            new_intent = content.get('intent_level')
            if new_intent:
                ok_intent, err_intent = self.message_validator.validate_enum_value(
                    new_intent, self.message_validator.VALID_INTENT_LEVELS, '意向度'
                )
                if not ok_intent:
                    return MessageFormatter.format_error_response(err_intent)
            
            # 若业务类型为DSE，则自动置DSE标志为1
            if parent.service_category == 'DSE':
                is_dse_value = 1

            # 创建反馈记录
            feedback = FollowupFeedback(
                parent_id=parent.id,
                feedback_type=feedback_type,
                content=feedback_content,
                is_dse=is_dse_value,
                followup_stage=fb_stage,
                prepayment_amount=prepay_val,
                is_visit=(is_visit_val or 0),
                operator_id=sender_info['staff_id'],
                created_at=DateTimeHelper.get_current_time()
            )
            
            db.add(feedback)
            
            # 同步家长上的阶段/预收/是否上门/意向度（覆盖式）
            updated_fields = []
            if fb_stage and parent.followup_stage != fb_stage:
                updated_fields.append(('followup_stage', parent.followup_stage, fb_stage))
                parent.followup_stage = fb_stage
            if prepay_val is not None:
                updated_fields.append(('prepayment_amount', parent.prepayment_amount, prepay_val))
                parent.prepayment_amount = prepay_val
            if is_visit_val is not None and parent.is_visit != is_visit_val:
                updated_fields.append(('is_visit', parent.is_visit, is_visit_val))
                parent.is_visit = is_visit_val
            if new_intent and parent.intent_level != new_intent:
                updated_fields.append(('intent_level', parent.intent_level, new_intent))
                parent.intent_level = new_intent

            for field_name, old_val, new_val in updated_fields:
                change = ChangeLogs(
                    parent_id=parent.id,
                    entity_type='parent',
                    field_name=field_name,
                    old_value=old_val,
                    new_value=new_val,
                    operator_id=sender_info['staff_id'],
                    created_at=DateTimeHelper.get_current_time()
                )
                db.add(change)

            # 创建处理日志
            log = ProcessLogs(
                parent_id=parent.id,
                action_type='反馈',
                operator_id=sender_info['staff_id'],
                operator_role=sender_info['role'],
                message_content=parsed_message.raw_message,
                notes=(
                    f"反馈类型: {feedback_type}; DSE: {'是' if is_dse_value == 1 else '否'}"
                    + (f"; 阶段: {fb_stage}" if fb_stage else "")
                    + (f"; 预收: {prepay_val}" if prepay_val is not None else "")
                    + (f"; 是否上门: {'是' if (is_visit_val or 0) == 1 else '否'}" if is_visit_val is not None else "")
                    + (f"; 意向度: {new_intent}" if new_intent else "")
                    + (f". {content.get('remark', '')}" if content.get('remark') else "")
                ).strip(),
                created_at=DateTimeHelper.get_current_time()
            )
            
            db.add(log)
            db.commit()
            
            # 空内容时提示为“无”以便阅读
            display_content = feedback_content if feedback_content else '无'
            return MessageFormatter.format_success_response(
                f"✅ 反馈记录成功！\n家长编号：{parent_code}\n反馈类型：{feedback_type}\nDSE：{'是' if is_dse_value == 1 else '否'}\n反馈内容：{display_content}"
            )
            
        except Exception as e:
            db.rollback()
            app_logger.error(f"处理反馈失败: {e}")
            return MessageFormatter.format_error_response(f"反馈失败: {str(e)}")
        finally:
            db.close()
    
    def _handle_deal_closed(self, parsed_message: ParsedMessage, sender_info: Dict[str, Any]) -> Dict[str, Any]:
        """处理成交消息"""
        db = SessionLocal()
        try:
            content = parsed_message.content
            parent_code = content['parent_code']
            amount = content['amount']
            
            # 查找家长
            parent = db.query(Parents).filter(Parents.parent_code == parent_code).first()
            if not parent:
                return MessageFormatter.format_error_response(f"未找到家长编号: {parent_code}")
            
            # 验证状态和权限
            if parent.current_status != '销售跟进中':
                return MessageFormatter.format_error_response(f"家长状态不正确，当前状态: {parent.current_status}")
            
            if parent.salesperson_id != sender_info['staff_id']:
                return MessageFormatter.format_error_response("只能操作自己跟进的家长")
            
            # 更新家长状态
            old_status = parent.current_status
            parent.current_status = '已成交'
            parent.deal_amount = amount
            parent.updated_at = DateTimeHelper.get_current_time()
            
            # 创建处理日志
            log = ProcessLogs(
                parent_id=parent.id,
                action_type='成交',
                operator_id=sender_info['staff_id'],
                operator_role=sender_info['role'],
                message_content=parsed_message.raw_message,
                notes=content.get('remark'),
                created_at=DateTimeHelper.get_current_time()
            )
            
            db.add(log)
            db.commit()
            
            return MessageFormatter.format_success_response(
                f"🎉 成交记录成功！\n家长编号：{parent_code}\n成交金额：¥{amount}\n状态：已成交"
            )
            
        except Exception as e:
            db.rollback()
            app_logger.error(f"处理成交失败: {e}")
            return MessageFormatter.format_error_response(f"成交记录失败: {str(e)}")
        finally:
            db.close()
    
    def _handle_lost(self, parsed_message: ParsedMessage, sender_info: Dict[str, Any]) -> Dict[str, Any]:
        """处理流失消息"""
        db = SessionLocal()
        try:
            content = parsed_message.content
            parent_code = content['parent_code']
            reason = content['reason']
            
            # 查找家长
            parent = db.query(Parents).filter(Parents.parent_code == parent_code).first()
            if not parent:
                return MessageFormatter.format_error_response(f"未找到家长编号: {parent_code}")
            
            # 验证状态和权限
            if parent.current_status != '销售跟进中':
                return MessageFormatter.format_error_response(f"家长状态不正确，当前状态: {parent.current_status}")
            
            if parent.salesperson_id != sender_info['staff_id']:
                return MessageFormatter.format_error_response("只能操作自己跟进的家长")
            
            # 更新家长状态
            old_status = parent.current_status
            parent.current_status = '已流失'
            parent.updated_at = DateTimeHelper.get_current_time()
            
            # 创建处理日志
            log = ProcessLogs(
                parent_id=parent.id,
                action_type='流失',
                operator_id=sender_info['staff_id'],
                operator_role=sender_info['role'],
                message_content=parsed_message.raw_message,
                notes=f"流失原因: {reason}. {content.get('remark', '')}".strip(),
                created_at=DateTimeHelper.get_current_time()
            )
            
            db.add(log)
            db.commit()
            
            return MessageFormatter.format_success_response(
                f"✅ 流失记录成功！\n家长编号：{parent_code}\n流失原因：{reason}\n状态：已流失"
            )
            
        except Exception as e:
            db.rollback()
            app_logger.error(f"处理流失失败: {e}")
            return MessageFormatter.format_error_response(f"流失记录失败: {str(e)}")
        finally:
            db.close()