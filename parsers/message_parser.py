"""
消息模板解析器
"""
import re
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from utils.validators import MessageValidator
from utils.helpers import TextProcessor

@dataclass
class ParsedMessage:
    """解析后的消息"""
    message_type: str
    sender: str
    content: Dict[str, Any]
    raw_message: str
    is_valid: bool
    error_message: Optional[str] = None

class MessageTemplateParser:
    """消息模板解析器"""
    
    def __init__(self):
        self.validator = MessageValidator()
        self.text_processor = TextProcessor()
        
        # 支持的模板类型标签
        self.supported_types = [
            '新家长', '补全微信号', '合伙人接手', '放弃', '转销售', '销售接手', '反馈', '成交', '流失'
        ]

        # 关键字到内部字段的映射（支持多种书写与SM_/HP_/XS_前缀）
        self.key_mappings = {
            # 通用
            '家长编号': 'parent_code',
            '线索编号': 'parent_code',
            '备注': 'remark',
            'SM_备注': 'remark',
            'HP_备注': 'remark',
            'XS_备注': 'remark',
            # 新家长/公共
            '来源': 'source',
            '平台来源': 'source',
            'SM_平台来源': 'source',
            'SM_业务类型': 'service_category',
            '联系方式': 'contact',
            'SM_联系方式': 'contact',
            '联系方式类别': 'contact_type',
            'SM_联系方式类别': 'contact_type',
            '需求': 'requirement',
            'SM_需求': 'requirement',
            'HP_需求': 'requirement',
            '分配给': 'assignee',
            'SM_分配给': 'assignee',
            'HP_分配给': 'assignee',
            # 为避免边界错误，保留人设键作为边界（验证层不强制）
            '人设': 'persona',
            'SM_人设': 'persona',
            'HP_人设': 'persona',
            'XS_人设': 'persona',
            # 移除人设与首推业务，统一使用 service_category 表示业务类型
            '意向度': 'intent_level',
            'SM_意向度': 'intent_level',
            'HP_意向度': 'intent_level',
            'XS_意向度': 'intent_level',
            '添加微信': 'is_added_wechat',
            '是否加微': 'is_added_wechat',
            '是否添加微信': 'is_added_wechat',
            'HP_添加微信': 'is_added_wechat',
            '联系方式说明': 'contact_desc',
            'HP_联系方式说明': 'contact_desc',
            # 补全微信号
            '微信昵称': 'wechat_nickname',
            'HP_微信昵称': 'wechat_nickname',
            '微信号': 'wechat_id',
            'HP_微信号': 'wechat_id',
            # 反馈
            '反馈类型': 'feedback_type',
            'XS_反馈类型': 'feedback_type',
            'XS_跟进阶段': 'followup_stage',
            'XS_预收金额': 'prepayment_amount',
            'XS_是否上门': 'is_visit',
            '内容': 'feedback_content',
            'XS_内容': 'feedback_content',
            'XS_DSE': 'is_dse',
            'DSE': 'is_dse',
            # 销售团队（销售专属，可选）
            'XS_团队': 'sales_team',
            '团队': 'sales_team',
            # 成交
            '金额': 'amount',
            '成交金额': 'amount',
            'XS_金额': 'amount',
            # 放弃/流失
            '原因': 'reason',
            'HP_原因': 'reason',
            'XS_原因': 'reason'
        }

    def parse_message(self, message: str, sender: str) -> ParsedMessage:
        """解析消息：不对value进行任何限制，只提取冒号后文本并删除空格
        - 保留原始换行进行键值提取
        - 值中的中文冒号统一转换为英文冒号
        - 若“联系方式类别+联系方式”分行，则合并为“类型:值”
        """
        raw = (message or '').strip()
        flat = self.text_processor.clean_text(message or '')

        # 若@了机器人，则无论是否包含模板相关内容，直接按查询处理
        try:
            if re.search(r'@机器人|@robot|@智能助手|@助手|@小助手', raw, flags=re.IGNORECASE):
                return ParsedMessage(
                    message_type='查询',
                    sender=sender,
                    content={'query': flat},
                    raw_message=message,
                    is_valid=True
                )
        except Exception:
            pass

        # 优先识别“模板帮助 [类型]”请求，避免被【类型】标签误判为业务模板
        try:
            if '模板帮助' in raw or '模板帮助' in flat:
                # 支持两种写法：模板帮助 新家长 / 模板帮助【流失】
                help_type = None
                m = re.search(r'模板帮助\s*[【\[]\s*([^】\]]+)\s*[】\]]', raw)
                if m:
                    help_type = m.group(1).strip()
                else:
                    m2 = re.search(r'模板帮助\s+([^\s【】]+)', flat)
                    if m2:
                        help_type = m2.group(1).strip()

                # 若类型不在支持列表，返回总览帮助但标记为有效以便上层正常回复
                return ParsedMessage(
                    message_type='模板帮助',
                    sender=sender,
                    content={'help_type': help_type} if help_type else {},
                    raw_message=message,
                    is_valid=True
                )
        except Exception:
            # 忽略帮助识别异常，后续按常规流程处理
            pass

        # 判断消息类型：标签存在即可（在原始文本中查找，以免换行被压缩影响）
        message_type = None
        for t in self.supported_types:
            if f'【{t}】' in raw:
                message_type = t
                break

        if not message_type:
            # 查询或未知
            if self._is_query_message(flat):
                return ParsedMessage(
                    message_type='查询',
                    sender=sender,
                    content={'query': flat},
                    raw_message=message,
                    is_valid=True
                )
            return ParsedMessage(
                message_type='未知',
                sender=sender,
                content={},
                raw_message=message,
                is_valid=False,
                error_message="消息格式不正确，请使用正确的模板格式"
            )

        # 提取主体（去掉首行标签；若标签与内容同一行，保留标签后的剩余文本）
        lines = [ln for ln in raw.splitlines() if ln.strip()]
        if lines and lines[0].startswith('【') and '】' in lines[0]:
            first = lines[0]
            idx = first.find('】')
            rest = first[idx + 1:].strip()
            if rest:
                lines[0] = rest
            else:
                lines = lines[1:]

        body = '\n'.join(lines)
        normalized_body = body.replace('：', ':')

        # 在整段文本中按“键:”扫描，提取该键到下一个键之间的内容
        # 修复：避免子串键（如“金额”）匹配到长键（如“预收金额”）内部，导致提取错位
        content: Dict[str, Any] = {}
        # 收集出现的键及其位置（仅匹配行首或换行后的键）
        key_positions: List[tuple[int, str]] = []
        for key in self.key_mappings.keys():
            # 键只能在行首或换行后出现，允许前导空白；统一为英文冒号
            # 捕获组1：行起始或换行符；组2：前导空白；随后是完整键名
            pattern = rf"(^|\n)(\s*){re.escape(key)}\s*:"
            for m in re.finditer(pattern, normalized_body):
                # 计算键实际开始位置（不包含前导换行与空白）
                key_start = m.start() + len(m.group(1)) + len(m.group(2))
                key_positions.append((key_start, key))

        # 按位置排序，从左到右提取
        key_positions.sort(key=lambda x: x[0])
        for i, (pos, key) in enumerate(key_positions):
            start = pos + len(key) + 1  # 跳过“key:”
            end = key_positions[i + 1][0] if i + 1 < len(key_positions) else len(normalized_body)
            raw_value = normalized_body[start:end]
            value = raw_value.replace(' ', '').replace('\n', '').strip()
            if not value:
                continue
            internal_key = self.key_mappings.get(key)
            if internal_key:
                # 针对家长编号，强制按首行/首个非空片段截断，避免后续未知键拼接
                if internal_key == 'parent_code':
                    first_line = raw_value.split('\n', 1)[0]
                    # 取首个由空白分隔的片段，并移除内部空白
                    first_token = first_line.strip().split(' ')[0]
                    value = re.sub(r"\s+", "", first_token)
                # 统一移除@符号：分配给与微信昵称字段均规范为纯昵称
                if internal_key == 'assignee':
                    if value.startswith('@') or '@' in value:
                        mentions = self.text_processor.extract_mentions(value)
                        if mentions:
                            # 使用第一个@提及的昵称
                            value = mentions[0]
                        else:
                            # 若无法提取到@提及，仅去掉前缀@
                            value = value.lstrip('@').strip()
                elif internal_key == 'wechat_nickname':
                    if value.startswith('@') or '@' in value:
                        mentions = self.text_processor.extract_mentions(value)
                        if mentions:
                            value = mentions[0]
                        else:
                            value = value.lstrip('@').strip()
                content[internal_key] = value
            else:
                content[key] = value

        # 若“联系方式类别/联系方式”分行且contact不含冒号，则合并为“类型:值”
        if 'contact' in content and ':' not in content['contact'] and content.get('contact_type'):
            content['contact'] = f"{content['contact_type']}:{content['contact']}"

        # 不进行任何值校验，识别到模板标签即认为有效
        return ParsedMessage(
            message_type=message_type,
            sender=sender,
            content=content,
            raw_message=message,
            is_valid=True,
            error_message=None
        )
    
    def _validate_message_content(self, message_type: str, content: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """验证消息内容"""
        try:
            if message_type == '新家长':
                return self._validate_new_parent(content)
            elif message_type == '补全微信号':
                return self._validate_complete_wechat(content)
            elif message_type == '合伙人接手':
                return self._validate_partner_take_over(content)
            elif message_type == '放弃':
                return self._validate_abandon(content)
            elif message_type == '转销售':
                return self._validate_transfer_to_sales(content)
            elif message_type == '销售接手':
                return self._validate_sales_take_over(content)
            elif message_type == '反馈':
                return self._validate_feedback(content)
            elif message_type == '成交':
                return self._validate_deal_closed(content)
            elif message_type == '流失':
                return self._validate_lost(content)
            else:
                return False, f"未知的消息类型: {message_type}"
        except Exception as e:
            return False, f"验证过程中出错: {str(e)}"
    
    def _validate_new_parent(self, content: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """验证新家长消息"""
        # 验证来源
        source = content.get('source')
        if not source:
            return False, "平台来源不能为空"
        is_valid, error = self.validator.validate_enum_value(
            source, self.validator.VALID_PLATFORMS, "平台来源"
        )
        if not is_valid:
            return False, error
        
        # 验证联系方式（类型:值）
        contact = content.get('contact')
        if not contact:
            return False, "联系方式不能为空"
        if ':' not in contact:
            return False, "联系方式格式错误，应为'类型:值'的格式"
        contact_type, contact_value = contact.split(':', 1)
        contact_type = contact_type.strip()
        contact_value = contact_value.strip()
        is_valid, error = self.validator.validate_enum_value(
            contact_type, self.validator.VALID_CONTACT_TYPES, "联系方式类型"
        )
        if not is_valid:
            return False, error
        is_valid, error = self.validator.validate_contact_value(contact_type, contact_value)
        if not is_valid:
            return False, error
        
        # 需求与分配给必须存在（按文档严格模板）
        requirement = content.get('requirement')
        if not requirement:
            return False, "需求不能为空"
        assignee = content.get('assignee')
        if not assignee:
            return False, "分配给不能为空，请@合伙人昵称"
        
        return True, None
    
    def _validate_complete_wechat(self, content: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """验证补全微信号消息"""
        parent_code = content.get('parent_code')
        if not parent_code:
            return False, "家长编号不能为空"
        # 必须包含微信昵称与微信号
        nickname = content.get('wechat_nickname')
        if not nickname:
            return False, "微信昵称不能为空"
        
        wechat_id = content.get('wechat_id')
        if not wechat_id:
            return False, "微信号不能为空"
        
        if not self.validator.validate_wechat_id(wechat_id):
            return False, "微信号格式不正确"
        
        return True, None
    
    def _validate_partner_take_over(self, content: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """验证合伙人接手消息"""
        parent_code = content.get('parent_code')
        if not parent_code:
            return False, "家长编号不能为空"
        # 放宽校验：不再要求人设字段
        
        return True, None
    
    def _validate_abandon(self, content: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """验证放弃消息"""
        parent_code = content.get('parent_code')
        if not parent_code:
            return False, "家长编号不能为空"
        
        reason = content.get('reason')
        if not reason:
            return False, "放弃原因不能为空"
        
        return True, None
    
    def _validate_transfer_to_sales(self, content: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """验证转销售消息"""
        parent_code = content.get('parent_code')
        if not parent_code:
            return False, "家长编号不能为空"
        # 需求
        if not content.get('requirement'):
            return False, "需求不能为空"
        # 意向度
        intent = content.get('intent_level')
        is_valid, error = self.validator.validate_enum_value(
            intent, self.validator.VALID_INTENT_LEVELS, "意向度"
        )
        if not is_valid:
            return False, error
        # 添加微信 是/否
        added = content.get('is_added_wechat')
        if added not in ('是', '否'):
            return False, "添加微信必须是 是/否"
        # 分配给
        assignee = content.get('assignee')
        if not assignee:
            return False, "分配给不能为空，请@销售昵称"
        
        return True, None
    
    def _validate_sales_take_over(self, content: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """验证销售接手消息"""
        parent_code = content.get('parent_code')
        if not parent_code:
            return False, "家长编号不能为空"

        # 团队（可选，若提供需为有效枚举）
        team = content.get('sales_team')
        if team is not None and str(team).strip() != '':
            is_valid, error = self.validator.validate_enum_value(
                team, self.validator.VALID_SALES_TEAMS, "团队"
            )
            if not is_valid:
                return False, error
        
        return True, None
    
    def _validate_feedback(self, content: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """验证反馈消息"""
        parent_code = content.get('parent_code')
        if not parent_code:
            return False, "家长编号不能为空"
        
        feedback_type = content.get('feedback_type')
        if not feedback_type:
            return False, "反馈类型不能为空"
        
        is_valid, error = self.validator.validate_enum_value(
            feedback_type, self.validator.VALID_FEEDBACK_TYPES, "反馈类型"
        )
        if not is_valid:
            return False, error
        
        # 反馈内容允许为空（服务层将以空字符串入库以满足非空约束）
        # 若没有提供则不在解析层报错
        # DSE 是/否（若提供则必须为 是/否；未提供则默认按否处理）
        is_dse = content.get('is_dse')
        if is_dse is not None and str(is_dse).strip() != '':
            if is_dse not in ('是', '否'):
                return False, "XS_DSE 必须是 是/否"
        
        return True, None
    
    def _validate_deal_closed(self, content: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """验证成交消息"""
        parent_code = content.get('parent_code')
        if not parent_code:
            return False, "家长编号不能为空"
        
        amount_str = content.get('amount')
        if not amount_str:
            return False, "成交金额不能为空"
        
        is_valid, error, amount = self.validator.validate_amount(amount_str)
        if not is_valid:
            return False, error
        
        # 将验证后的金额存回content
        content['amount'] = amount
        
        return True, None
    
    def _validate_lost(self, content: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """验证流失消息"""
        parent_code = content.get('parent_code')
        if not parent_code:
            return False, "家长编号不能为空"
        
        reason = content.get('reason')
        if not reason:
            return False, "流失原因不能为空"
        
        return True, None
    
    def _is_query_message(self, message: str) -> bool:
        """判断是否是查询消息"""
        # 若@了机器人，则按查询处理（即使没有关键词），优先级最高
        if re.search(r'@机器人|@robot|@智能助手|@助手|@小助手', message, flags=re.IGNORECASE):
            return True
        # 若包含模板标签，则不是查询
        if re.search(r'【(新家长|补全微信号|合伙人接手|放弃|转销售|销售接手|反馈|成交|流失)】', message):
            return False
        # 查询关键词（扩展覆盖自然表达）
        query_keywords = [
            '查询', '统计', '数据', '报表', '总数', '数量',
            '今天', '昨天', '这周', '本周', '过去一周', '近一周', '最近一周', '目前这一周', '一周',
            '本月', '上月',
            '跟进',
            '多少', '几个', '几条', '是什么', '哪些',
            '名称', '名字', '名单', '列表', '明细', '详情'
        ]
        message_lower = message.lower()
        return any(kw in message for kw in query_keywords) or any(kw in message_lower for kw in query_keywords)
    
    def get_template_help(self, message_type: Optional[str] = None) -> str:
        """获取模板帮助信息"""
        if message_type and message_type in self.supported_types:
            return self._get_single_template_help(message_type)
        else:
            return self._get_all_templates_help()
    
    def _get_single_template_help(self, message_type: str) -> str:
        """获取单个模板的帮助信息"""
        help_texts = {
            '新家长': """【新家长】模板格式（严格按文档）
【新家长】
SM_平台来源: 抖音/小红书/微信公众号/快手/B站/其他
SM_联系方式类别: 微信号/手机号/香港WS手机号/微信二维码
SM_联系方式: 值（若为微信二维码，填写微信昵称）
SM_业务类型: DSE/插班和相关培训/外教/中文
SM_需求: 文本
SM_分配给: @HP_合伙人昵称 或 @XS_销售昵称
SM_备注: 文本（可选）
注：发送者与分配对象群昵称必须以 SM_/HP_/XS_ 前缀开头""",
            
            '补全微信号': """【补全微信号】模板格式（保留家长编号行）
【补全微信号】
家长编号：Pxxxxxxxx
HP_微信昵称: 昵称文本（可选）
HP_微信号: wxid_example""",
            
            '合伙人接手': """【合伙人接手】模板格式（保留家长编号行）
【合伙人接手】
家长编号：Pxxxxxxxx
HP_人设: 家长号/机构号/老师号/校方号/其他""",
            
            '放弃': """【放弃】模板格式（保留家长编号行）
【放弃】
家长编号：Pxxxxxxxx
HP_原因: 文本""",
            
            '转销售': """【转销售】模板格式（保留家长编号行）
【转销售】
家长编号：Pxxxxxxxx
HP_需求: 文本
HP_意向度: 低/中/高
HP_添加微信: 是/否
HP_联系方式说明: 文本（可选）
HP_分配给: @销售昵称
HP_备注: 文本（可选）""",
            
            '销售接手': """【销售接手】模板格式（保留家长编号行）
【销售接手】
家长编号：Pxxxxxxxx
XS_团队: 广州/深圳（可选）""",
            
            '反馈': """【反馈】模板格式（保留家长编号行）
【反馈】
家长编号：Pxxxxxxxx
XS_反馈类型: 当日/3天内/7天内/7天后/其他
XS_跟进阶段: 联系不上/初步清洗/成功约了首Call/决策是否去香港/对比机构阶段/邀约到访/测评/谈方案中（可选）
XS_预收金额: 数字（可选）
XS_是否上门: 是/否（可选）
XS_意向度: 低/中/高（可选）
XS_DSE: 是/否（可选）
XS_内容: 文本（可选）
XS_备注: 文本（可选）""",
            
            '成交': """【成交】模板格式（保留家长编号行）
【成交】
家长编号：Pxxxxxxxx
成交金额: 数字（RMB）
XS_备注: 文本（可选）""",
            
            '流失': """【流失】模板格式（保留家长编号行）
【流失】
家长编号：Pxxxxxxxx
XS_原因: 文本"""
        }
        
        return help_texts.get(message_type, f"未找到 {message_type} 模板的帮助信息")
    
    def _get_all_templates_help(self) -> str:
        """获取所有模板的帮助信息"""
        return """支持的消息模板：

1. 【新家长】- 社媒人员录入新家长信息
2. 【补全微信号】- 合伙人补全家长微信号
3. 【合伙人接手】- 合伙人接手家长
4. 【放弃】- 合伙人放弃家长
5. 【转销售】- 合伙人转给销售
6. 【销售接手】- 销售接手家长
7. 【反馈】- 销售反馈跟进情况
8. 【成交】- 销售录入成交信息
9. 【流失】- 销售录入流失信息

发送 "模板帮助 [类型]" 获取具体模板格式
例如：模板帮助 新家长"""