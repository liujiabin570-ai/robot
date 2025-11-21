"""
数据库模型定义
"""
from sqlalchemy import create_engine, Column, Integer, String, Text, DECIMAL, TIMESTAMP, Enum, ForeignKey, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.sql import func
from datetime import datetime
from config import config

Base = declarative_base()

class StaffMapping(Base):
    """人员映射表"""
    __tablename__ = 'staff_mapping'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    staff_id = Column(String(50), nullable=False, unique=True, comment='员工ID')
    role = Column(Enum('社媒', '合伙人', '销售'), nullable=False, comment='角色')
    sales_team = Column(Enum('广州', '深圳'), comment='销售团队')
    is_active = Column(Integer, default=1, comment='是否激活')
    created_at = Column(TIMESTAMP, default=func.current_timestamp(), comment='创建时间')
    updated_at = Column(TIMESTAMP, default=func.current_timestamp(), onupdate=func.current_timestamp(), comment='更新时间')

class Parents(Base):
    """家长主表"""
    __tablename__ = 'parents'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    parent_code = Column(String(30), nullable=False, unique=True, comment='家长编号')
    recommend_channel = Column(Enum('社媒', '合伙人'), nullable=False, default='社媒', comment='推荐渠道')
    source_platform = Column(Enum('抖音', '小红书', '微信公众号', '快手', 'B站', '其他'), nullable=False, comment='来源平台')
    # 使用 service_category 作为业务类型字段，替代历史上的 primary_business
    service_category = Column(Enum('DSE', '插班和相关培训', '外教', '中文'), nullable=False, comment='业务类型')
    requirement = Column(Text, comment='需求')
    current_status = Column(Enum('待接手', '合伙人跟进中', '销售跟进中', '已成交', '已流失'), 
                         nullable=False, default='待接手', comment='当前状态')
    social_media_id = Column(String(50), nullable=False, comment='社媒人员ID')
    partner_id = Column(String(50), nullable=False, comment='合伙人ID')
    salesperson_id = Column(String(50), comment='销售人员ID')
    sales_team = Column(Enum('广州', '深圳'), comment='销售团队快照')
    deal_amount = Column(DECIMAL(12, 2), comment='成交金额')
    intent_level = Column(Enum('低', '中', '高'), comment='意向度')
    is_dse = Column(Integer, nullable=False, default=0, comment='是否DSE')
    student_id = Column(String(200), comment='学生ID')
    followup_stage = Column(Enum('联系不上', '初步清洗', '成功约了首Call', '决策是否去香港', '对比机构阶段', '邀约到访/测评', '谈方案中'), comment='跟进阶段')
    prepayment_amount = Column(DECIMAL(12, 2), comment='预收金额')
    is_visit = Column(Integer, nullable=False, default=0, comment='是否上门')
    first_group_id = Column(String(100), comment='首发群ID')
    first_group_name = Column(String(200), comment='首发群名称')
    created_at = Column(TIMESTAMP, default=func.current_timestamp(), comment='创建时间')
    updated_at = Column(TIMESTAMP, default=func.current_timestamp(), onupdate=func.current_timestamp(), comment='更新时间')
    
    # 索引
    __table_args__ = (
        Index('idx_status', 'current_status'),
        Index('idx_created', 'created_at'),
        Index('idx_partner', 'partner_id'),
        Index('idx_sales', 'salesperson_id'),
        Index('idx_platform', 'source_platform'),
        Index('idx_intent', 'intent_level'),
        Index('idx_service', 'service_category'),
        Index('idx_sales_team', 'sales_team'),
        Index('idx_followup_stage', 'followup_stage'),
    )

class ParentContacts(Base):
    """联系方式表"""
    __tablename__ = 'parent_contacts'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    parent_id = Column(Integer, ForeignKey('parents.id', ondelete='CASCADE'), nullable=False, comment='家长ID')
    contact_type = Column(Enum('微信号', '手机号', '香港WS手机号', '微信二维码昵称'), nullable=False, comment='联系方式类型')
    contact_value = Column(String(200), nullable=False, comment='联系方式值')
    contact_desc = Column(String(200), comment='联系方式说明')
    is_primary = Column(Integer, nullable=False, default=0, comment='是否主要联系方式')
    is_verified = Column(Integer, nullable=False, default=0, comment='是否已验证')
    is_added_wechat = Column(Integer, nullable=False, default=0, comment='是否已添加微信')
    created_at = Column(TIMESTAMP, default=func.current_timestamp(), comment='创建时间')
    updated_at = Column(TIMESTAMP, default=func.current_timestamp(), onupdate=func.current_timestamp(), comment='更新时间')
    
    # 索引和约束
    __table_args__ = (
        Index('uniq_contact', 'contact_type', 'contact_value', unique=True),
        Index('idx_parent', 'parent_id'),
    )

class ProcessLogs(Base):
    """流程日志表"""
    __tablename__ = 'process_logs'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    parent_id = Column(Integer, ForeignKey('parents.id', ondelete='CASCADE'), nullable=False, comment='家长ID')
    action_type = Column(Enum('新家长', '补全微信号', '合伙人接手', '放弃', '转销售', '销售接手', '成交', '流失', '反馈'), 
                        nullable=False, comment='操作类型')
    operator_id = Column(String(50), nullable=False, comment='操作人员ID')
    operator_role = Column(Enum('社媒', '合伙人', '销售'), nullable=False, comment='操作人员角色')
    assignee_id = Column(String(50), comment='被分配人员ID')
    assignee_role = Column(Enum('社媒', '合伙人', '销售'), comment='被分配人员角色')
    assignee_team = Column(Enum('广州', '深圳'), comment='被分配人员团队')
    group_id = Column(String(100), comment='群ID')
    group_name = Column(String(200), comment='群名称')
    message_id = Column(String(100), comment='消息ID')
    message_content = Column(Text, comment='消息内容')
    notes = Column(Text, comment='备注')
    created_at = Column(TIMESTAMP, default=func.current_timestamp(), comment='创建时间')
    
    # 索引
    __table_args__ = (
        Index('idx_log_parent', 'parent_id'),
        Index('idx_log_action', 'action_type'),
    )

class RawMessages(Base):
    """原始消息表"""
    __tablename__ = 'raw_messages'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    sender_wechat_name = Column(String(100), nullable=False, comment='发送者微信昵称')
    group_id = Column(String(100), comment='群ID')
    group_name = Column(String(200), comment='群名称')
    message_id = Column(String(100), comment='消息ID')
    message_content = Column(Text, comment='消息内容')
    is_processed = Column(Integer, nullable=False, default=0, comment='是否已处理')
    created_at = Column(TIMESTAMP, default=func.current_timestamp(), comment='创建时间')
    
    # 索引
    __table_args__ = (
        Index('idx_msg_group', 'group_id'),
    )

class ChangeLogs(Base):
    """变更记录表"""
    __tablename__ = 'change_logs'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    parent_id = Column(Integer, ForeignKey('parents.id', ondelete='CASCADE'), nullable=False, comment='家长ID')
    entity_type = Column(Enum('parent', 'contact'), nullable=False, default='parent', comment='实体类型')
    field_name = Column(String(50), nullable=False, comment='字段名')
    old_value = Column(Text, comment='旧值')
    new_value = Column(Text, comment='新值')
    operator_id = Column(String(50), nullable=False, comment='操作人员ID')
    group_id = Column(String(100), comment='群ID')
    group_name = Column(String(200), comment='群名称')
    reason = Column(String(200), comment='变更原因')
    created_at = Column(TIMESTAMP, default=func.current_timestamp(), comment='创建时间')
    
    # 索引
    __table_args__ = (
        Index('idx_change_parent', 'parent_id'),
        Index('idx_change_field', 'field_name'),
    )

class FollowupFeedback(Base):
    """跟进反馈表"""
    __tablename__ = 'followup_feedback'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    parent_id = Column(Integer, ForeignKey('parents.id', ondelete='CASCADE'), nullable=False, comment='家长ID')
    feedback_type = Column(Enum('当日', '3天内', '7天内', '7天后', '其他'), nullable=False, comment='反馈类型')
    content = Column(Text, nullable=False, comment='反馈内容')
    is_dse = Column(Integer, nullable=False, default=0, comment='是否DSE')
    followup_stage = Column(Enum('联系不上', '初步清洗', '成功约了首Call', '决策是否去香港', '对比机构阶段', '邀约到访/测评', '谈方案中'), comment='跟进阶段')
    prepayment_amount = Column(DECIMAL(12, 2), comment='预收金额')
    is_visit = Column(Integer, nullable=False, default=0, comment='是否上门')
    operator_id = Column(String(50), nullable=False, comment='操作人员ID')
    created_at = Column(TIMESTAMP, default=func.current_timestamp(), comment='创建时间')
    
    # 索引
    __table_args__ = (
        Index('idx_feedback_parent', 'parent_id'),
        Index('idx_feedback_type', 'feedback_type'),
    )

# 数据库引擎和会话
engine = create_engine(config.database_url, echo=False, pool_recycle=3600)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    """获取数据库会话"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def create_tables():
    """创建所有表"""
    Base.metadata.create_all(bind=engine)

def drop_tables():
    """删除所有表"""
    Base.metadata.drop_all(bind=engine)