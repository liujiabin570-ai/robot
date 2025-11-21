"""
Flask主应用
"""
import asyncio
from flask import Flask, request, jsonify, render_template, redirect, url_for, session, send_file
from datetime import datetime
import json
import re

from typing import Optional
from models.database import SessionLocal, RawMessages, Parents, ProcessLogs, FollowupFeedback
from utils.logger import app_logger
from utils.helpers import MessageFormatter, TextProcessor, DateTimeHelper
from config import config
from sqlalchemy.sql import func
from sqlalchemy import and_, or_
from sqlalchemy.orm import aliased
from decimal import Decimal

# 创建Flask应用
app = Flask(__name__)
app.config['SECRET_KEY'] = config.SECRET_KEY

# 智能体实例（延迟加载，避免启动时导入冲突）
_agent: Optional[object] = None

def get_agent():
    global _agent
    if _agent is None:
        from agents.langgraph_agent import LangGraphAgent  # 延迟导入
        _agent = LangGraphAgent()
    return _agent
text_processor = TextProcessor()

@app.route('/health', methods=['GET'])
def health_check():
    """健康检查"""
    return jsonify({
        'status': 'healthy',
        'timestamp': DateTimeHelper.get_current_time().isoformat(),
        'service': 'WeChat Robot Agent'
    })

@app.route('/get_news', methods=['GET', 'POST'])
def receive_message():
    """接收 WorkTool 回调并处理消息"""
    try:
        # 兼容多种Content-Type与握手GET
        if request.method == 'GET':
            return jsonify({"status": "ok", "message": "接口正常"})

        data = request.get_json(silent=True) or request.form.to_dict() or {}
        app_logger.info(f"收到WorkTool消息: {data}")

        # WorkTool字段映射
        raw_spoken = data.get('rawSpoken')

        at_me = (str(data.get('atMe', 'false')).lower() == 'true')
        group_name = data.get('groupName')
        sender = data.get('receivedName')

        app_logger.info(f"消息元信息: atMe={at_me}, groupName={group_name}, sender={sender}")

        if not raw_spoken:
            # 返回带有 content 的错误响应，便于 WorkTool 直接回复具体错误
            err = MessageFormatter.format_error_response("消息内容为空")
            err["content"] = err.get("error", "消息内容为空")
            return jsonify(err), 400

        save_raw_message({
            'content': raw_spoken,
            'sender': sender,
            'group_name': group_name,
            'type': 'text',
            'worktool_raw': data
        })

        # 判断是否需要处理：满足以下任一条件
        # 1) @了机器人；2) 含业务模板标签（如【新家长】等）
        template_tag_match = re.search(r'【(新家长|补全微信号|合伙人接手|放弃|转销售|销售接手|反馈|成交|流失)】', raw_spoken)
        is_template = bool(template_tag_match)

        # 查询关键词检测（用于日志与限制）
        query_keywords = [
            '查询', '统计', '数据', '报表', '总数', '数量',
            '今天', '昨天', '本周', '本月', '上月',
            '新家长', '成交', '流失', '跟进',
            '多少', '几个', '几条'
        ]
        looks_like_query = any(k in raw_spoken.lower() for k in query_keywords)

        # 触发条件：@机器人 或 模板消息；查询必须@Me
        should_process = at_me or is_template

        # 清理@前缀，例如 @小明 ...（不论是否@都清理）
        clean_message = re.sub(r'^\s*@\S+\s*', '', raw_spoken).strip()

        if not should_process:
            if looks_like_query:
                app_logger.info("检测到查询关键词但未@机器人，忽略该查询")
            else:
                app_logger.info("未@机器人且非模板消息，不处理且不回复")
            # 返回 204 No Content，避免群内产生任何回复文案
            return ('', 204)

        

        # 处理消息：为确保@机器人查询被正确识别，传递原始文本（包含@提及）给智能体
        response_text = asyncio.run(get_agent().process_message(raw_spoken, sender))

        # 推送群消息（按群名定向），若无有效群名则不推送，避免误投
        try:
            from send_message import send_group_text
            if not group_name:
                app_logger.warning("推送跳过：缺少有效的groupName，避免误把内容当群名")
            else:
                push_res = send_group_text(response_text, group_name=group_name)
                app_logger.info(f"推送结果: {push_res}")
        except Exception as e:
            app_logger.error(f"推送消息失败: {e}")

        # 包含 content 字段以便 WorkTool 在开启 replyAll 时直接群内回复查询/业务结果
        return jsonify({
            'success': True,
            'response': response_text,
            'content': response_text,
            'timestamp': DateTimeHelper.get_current_time().isoformat()
        })
    except Exception as e:
        app_logger.error(f"处理消息失败: {e}")
        error_response = MessageFormatter.format_error_response(f"处理失败: {str(e)}")
        # 同步提供 content 字段以便 WorkTool 直接显示错误原因
        error_response["content"] = error_response.get("error", f"处理失败: {str(e)}")
        return jsonify(error_response), 500

@app.route('/stats', methods=['GET'])
def get_stats():
    """获取快速统计"""
    try:
        stats_text = asyncio.run(get_agent().get_quick_stats())
        
        return jsonify({
            'success': True,
            'stats': stats_text,
            'timestamp': DateTimeHelper.get_current_time().isoformat()
        })
        
    except Exception as e:
        app_logger.error(f"获取统计失败: {e}")
        error_response = MessageFormatter.format_error_response(f"获取统计失败: {str(e)}")
        return jsonify(error_response), 500

@app.route('/help', methods=['GET'])
def get_help():
    """获取帮助信息"""
    try:
        help_text = get_agent().get_help_message()
        
        return jsonify({
            'success': True,
            'help': help_text,
            'timestamp': DateTimeHelper.get_current_time().isoformat()
        })
        
    except Exception as e:
        app_logger.error(f"获取帮助失败: {e}")
        error_response = MessageFormatter.format_error_response(f"获取帮助失败: {str(e)}")
        return jsonify(error_response), 500

@app.route('/query', methods=['POST'])
def handle_query():
    """处理查询请求"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify(MessageFormatter.format_error_response("无效的请求数据")), 400
        
        query = data.get('query', '')
        sender = data.get('sender', 'API用户')
        
        if not query:
            return jsonify(MessageFormatter.format_error_response("查询内容不能为空")), 400
        
        # 处理查询
        response_text = asyncio.run(get_agent().process_message(query, sender))
        
        return jsonify({
            'success': True,
            'query': query,
            'response': response_text,
            'timestamp': DateTimeHelper.get_current_time().isoformat()
        })
        
    except Exception as e:
        app_logger.error(f"处理查询失败: {e}")
        error_response = MessageFormatter.format_error_response(f"查询处理失败: {str(e)}")
        return jsonify(error_response), 500

def save_raw_message(data: dict):
    """保存原始消息"""
    db = SessionLocal()
    try:
        # 兼容数据库字段：sender_wechat_name, group_id, group_name, message_id, message_content, is_processed, created_at
        wt_raw = data.get('worktool_raw') or {}
        raw_message = RawMessages(
            sender_wechat_name=data.get('sender', ''),
            group_id=wt_raw.get('groupId') or '',
            group_name=data.get('group_name', ''),
            message_id=wt_raw.get('messageId') or '',
            message_content=data.get('content', ''),
            is_processed=0,
            created_at=DateTimeHelper.get_current_time()
        )
        
        db.add(raw_message)
        db.commit()
        try:
            app_logger.info(
                f"RawMessage入库成功: id={getattr(raw_message, 'id', None)} sender={raw_message.sender_wechat_name} group={raw_message.group_name} groupId={raw_message.group_id} content_len={len(raw_message.message_content or '')}"
            )
        except Exception:
            pass
        
    except Exception as e:
        app_logger.error(f"保存原始消息失败: {e}")
        db.rollback()
    finally:
        db.close()

def is_robot_mentioned(message: str) -> bool:
    """检查消息是否@了机器人"""
    # 检查常见的@机器人模式
    robot_patterns = [
        r'@机器人',
        r'@robot',
        r'@智能助手',
        r'@助手',
        r'@小助手'
    ]
    
    message_lower = message.lower()
    
    for pattern in robot_patterns:
        if re.search(pattern, message, re.IGNORECASE):
            return True
    
    return False

def clean_robot_mention(message: str) -> str:
    """清理消息中的@机器人部分"""
    # 移除@机器人相关的内容
    robot_patterns = [
        r'@机器人\s*',
        r'@robot\s*',
        r'@智能助手\s*',
        r'@助手\s*',
        r'@小助手\s*'
    ]
    
    cleaned_message = message
    
    for pattern in robot_patterns:
        cleaned_message = re.sub(pattern, '', cleaned_message, flags=re.IGNORECASE)
    
    return text_processor.clean_text(cleaned_message)

@app.errorhandler(404)
def not_found(error):
    """404错误处理"""
    return jsonify(MessageFormatter.format_error_response("接口不存在")), 404

@app.errorhandler(500)
def internal_error(error):
    """500错误处理"""
    app_logger.error(f"内部服务器错误: {error}")
    return jsonify(MessageFormatter.format_error_response("内部服务器错误")), 500

# Admin 登录保护装饰器
def admin_login_required(fn):
    def wrapper(*args, **kwargs):
        if not session.get('admin_logged_in'):
            return redirect(url_for('admin_login'))
        return fn(*args, **kwargs)
    # 保留原函数名，避免 Flask 路由冲突
    wrapper.__name__ = fn.__name__
    return wrapper

@app.route('/admin/login', methods=['GET', 'POST'])
def admin_login():
    if request.method == 'POST':
        username = request.form.get('username', '')
        password = request.form.get('password', '')
        if username == config.ADMIN_USERNAME and password == config.ADMIN_PASSWORD:
            session['admin_logged_in'] = True
            return redirect(url_for('admin_parents_list'))
        else:
            return render_template('admin_login.html', error='账号或密码错误')
    return render_template('admin_login.html')

@app.route('/admin/logout')
@admin_login_required
def admin_logout():
    session.pop('admin_logged_in', None)
    return redirect(url_for('admin_login'))

@app.route('/admin')
@admin_login_required
def admin_dashboard():
    return redirect(url_for('ui_index'))

@app.route('/admin/parents', methods=['GET'])
@admin_login_required
def admin_parents_list():
    return redirect(url_for('ui_index'))
    q = request.args.get('q', '').strip()
    db = SessionLocal()
    try:
        # 子查询：每个群的最新原始消息 id
        rm_latest_ids = db.query(
            RawMessages.group_id.label('group_id'),
            func.max(RawMessages.id).label('max_id')
        ).group_by(RawMessages.group_id).subquery()

        RM = aliased(RawMessages)
        query = db.query(Parents, RM.group_name) \
            .outerjoin(rm_latest_ids, rm_latest_ids.c.group_id == Parents.first_group_id) \
            .outerjoin(RM, RM.id == rm_latest_ids.c.max_id)

        if q:
            if q.isdigit():
                query = query.filter(Parents.id == int(q))
            else:
                query = query.filter(Parents.parent_code.like(f"%{q}%"))

        results = query.order_by(Parents.created_at.desc()).limit(50).all()

        rows = []
        for p, group_name in results:
            rows.append({
                'p': p,
                'group_name': group_name or getattr(p, 'first_group_name', None)
            })

        # ====== 统计面板数据聚合 ======
        overall_total = db.query(func.count(Parents.id)).scalar() or 0
        today_total = db.query(func.count(Parents.id)) \
            .filter(func.date(Parents.created_at) == func.curdate()).scalar() or 0

        team_counts = db.query(Parents.sales_team, func.count(Parents.id)) \
            .group_by(Parents.sales_team).all()
        team_totals = [{'team': (t or '未分配'), 'count': c} for t, c in team_counts]

        today_by_channel_rows = db.query(Parents.recommend_channel, func.count(Parents.id)) \
            .filter(func.date(Parents.created_at) == func.curdate()) \
            .group_by(Parents.recommend_channel).all()
        today_by_channel = [{'channel': (ch or '未知'), 'count': c} for ch, c in today_by_channel_rows]

        individual_totals_sales_rows = db.query(Parents.salesperson_id, func.count(Parents.id)) \
            .filter(Parents.salesperson_id != None) \
            .group_by(Parents.salesperson_id) \
            .order_by(func.count(Parents.id).desc()) \
            .limit(10).all()
        individual_totals_sales = [{'staff_id': sid or '未分配', 'count': c} for sid, c in individual_totals_sales_rows]

        individual_totals_partner_rows = db.query(Parents.partner_id, func.count(Parents.id)) \
            .filter(Parents.partner_id != '') \
            .group_by(Parents.partner_id) \
            .order_by(func.count(Parents.id).desc()) \
            .limit(10).all()
        individual_totals_partner = [{'staff_id': pid or '未分配', 'count': c} for pid, c in individual_totals_partner_rows]

        individual_totals_social_rows = db.query(Parents.social_media_id, func.count(Parents.id)) \
            .group_by(Parents.social_media_id) \
            .order_by(func.count(Parents.id).desc()) \
            .limit(10).all()
        individual_totals_social = [{'staff_id': sid or '未知', 'count': c} for sid, c in individual_totals_social_rows]

        individual_daily_sales_today_rows = db.query(Parents.salesperson_id, func.count(Parents.id)) \
            .filter(Parents.salesperson_id != None, func.date(Parents.created_at) == func.curdate()) \
            .group_by(Parents.salesperson_id) \
            .order_by(func.count(Parents.id).desc()) \
            .limit(10).all()
        individual_daily_sales_today = [{'staff_id': sid or '未分配', 'count': c} for sid, c in individual_daily_sales_today_rows]

        # 调整分配明细映射（去团队维度）
        alloc_social_to_sales_rows = db.query(Parents.social_media_id, Parents.salesperson_id, func.count(Parents.id)) \
            .filter(Parents.recommend_channel == '社媒', Parents.salesperson_id != None) \
            .group_by(Parents.social_media_id, Parents.salesperson_id) \
            .order_by(func.count(Parents.id).desc()).all()
        alloc_social_to_sales = [
            {'social_id': sid or '未知', 'sales_id': spid or '未分配', 'count': c}
            for sid, spid, c in alloc_social_to_sales_rows
        ]

        alloc_social_to_partner_rows = db.query(Parents.social_media_id, Parents.partner_id, func.count(Parents.id)) \
            .filter(Parents.recommend_channel == '社媒', Parents.partner_id != '') \
            .group_by(Parents.social_media_id, Parents.partner_id) \
            .order_by(func.count(Parents.id).desc()).all()
        alloc_social_to_partner = [
            {'social_id': sid or '未知', 'partner_id': pid or '未分配', 'count': c}
            for sid, pid, c in alloc_social_to_partner_rows
        ]

        alloc_partner_to_sales_rows = db.query(Parents.partner_id, Parents.salesperson_id, func.count(Parents.id)) \
            .filter(Parents.recommend_channel == '合伙人', Parents.salesperson_id != None) \
            .group_by(Parents.partner_id, Parents.salesperson_id) \
            .order_by(func.count(Parents.id).desc()).all()
        alloc_partner_to_sales = [
            {'partner_id': pid or '未分配', 'sales_id': spid or '未分配', 'count': c}
            for pid, spid, c in alloc_partner_to_sales_rows
        ]

        followup_stage_rows = db.query(Parents.followup_stage, func.count(Parents.id)) \
            .filter(Parents.current_status == '销售跟进中') \
            .group_by(Parents.followup_stage).all()
        followup_stage_stats = [
            {'stage': stage or '未设置', 'count': c}
            for stage, c in followup_stage_rows
        ]

        # ====== 明细过滤 ======
        detail_kind = request.args.get('detail_kind', '').strip()
        detail_title = None
        detail_entries = []

        if detail_kind:
            pq = db.query(Parents)
            sales_id = request.args.get('sales_id', '').strip()
            partner_id = request.args.get('partner_id', '').strip()
            social_id = request.args.get('social_id', '').strip()
            stage = request.args.get('stage', '').strip()

            if detail_kind == 'by_salesperson' and sales_id:
                pq = pq.filter(Parents.salesperson_id == sales_id)
                detail_title = f"销售 {sales_id} 的例子"
            elif detail_kind == 'daily_sales_today' and sales_id:
                pq = pq.filter(Parents.salesperson_id == sales_id, func.date(Parents.created_at) == func.curdate())
                detail_title = f"销售 {sales_id} 今日新增例子"
            elif detail_kind == 'by_partner' and partner_id:
                pq = pq.filter(Parents.partner_id == partner_id)
                detail_title = f"合伙人 {partner_id} 的例子"
            elif detail_kind == 'by_social' and social_id:
                pq = pq.filter(Parents.social_media_id == social_id)
                detail_title = f"社媒 {social_id} 的例子"
            elif detail_kind == 'alloc_social_to_sales' and social_id and sales_id:
                pq = pq.filter(Parents.recommend_channel == '社媒', Parents.social_media_id == social_id, Parents.salesperson_id == sales_id)
                detail_title = f"社媒 {social_id} 分配给销售 {sales_id} 的例子"
            elif detail_kind == 'alloc_social_to_partner' and social_id and partner_id:
                pq = pq.filter(Parents.recommend_channel == '社媒', Parents.social_media_id == social_id, Parents.partner_id == partner_id)
                detail_title = f"社媒 {social_id} 分配给合伙人 {partner_id} 的例子"
            elif detail_kind == 'alloc_partner_to_sales' and partner_id and sales_id:
                pq = pq.filter(Parents.recommend_channel == '合伙人', Parents.partner_id == partner_id, Parents.salesperson_id == sales_id)
                detail_title = f"合伙人 {partner_id} 分配给销售 {sales_id} 的例子"
            elif detail_kind == 'followup_stage' and stage:
                pq = pq.filter(Parents.current_status == '销售跟进中', Parents.followup_stage == stage)
                detail_title = f"销售跟进阶段 {stage} 的例子"
            else:
                detail_title = "明细"

            selected_parents = pq.order_by(Parents.created_at.desc()).limit(200).all()
            ids = [p.id for p in selected_parents]
            code_map = {p.id: p.parent_code for p in selected_parents}

            if ids:
                logs = db.query(ProcessLogs).filter(ProcessLogs.parent_id.in_(ids)).order_by(ProcessLogs.created_at.desc()).all()
                feedbacks = db.query(FollowupFeedback).filter(FollowupFeedback.parent_id.in_(ids)).order_by(FollowupFeedback.created_at.desc()).all()

                for lg in logs:
                    detail_entries.append({
                        'parent_code': code_map.get(lg.parent_id, '未编号'),
                        'type': lg.action_type,
                        'operator_id': lg.operator_id,
                        'role': lg.operator_role,
                        'content': (lg.message_content or lg.notes or ''),
                        'created_at': lg.created_at
                    })
                for fb in feedbacks:
                    detail_entries.append({
                        'parent_code': code_map.get(fb.parent_id, '未编号'),
                        'type': f"反馈·{fb.feedback_type}",
                        'operator_id': fb.operator_id,
                        'role': '销售',
                        'content': fb.content,
                        'created_at': fb.created_at
                    })

        stats = {
            'overall_total': overall_total,
            'team_totals': team_totals,
            'today_total': today_total,
            'today_by_channel': today_by_channel,
            'individual_totals_sales': individual_totals_sales,
            'individual_totals_partner': individual_totals_partner,
            'individual_totals_social': individual_totals_social,
            'individual_daily_sales_today': individual_daily_sales_today,
            'alloc_social_to_sales': alloc_social_to_sales,
            'alloc_social_to_partner': alloc_social_to_partner,
            'alloc_partner_to_sales': alloc_partner_to_sales,
            'followup_stage_stats': followup_stage_stats,
        }

        return render_template('admin_parents.html', rows=rows, q=q, stats=stats, detail_entries=detail_entries, detail_title=detail_title)
    finally:
        db.close()

@app.route('/api/dashboard', methods=['GET'])
def api_dashboard():
    db = SessionLocal()
    try:
        overall_total = db.query(func.count(Parents.id)).scalar() or 0
        today_total = db.query(func.count(Parents.id)).filter(func.date(Parents.created_at) == func.curdate()).scalar() or 0
        team_counts = db.query(Parents.sales_team, func.count(Parents.id)).filter(Parents.sales_team.in_(['广州', '深圳'])).group_by(Parents.sales_team).all()
        app_logger.info(f"api_dashboard team_counts filtered: {team_counts}")
        team_totals = [{'team': t, 'count': c} for t, c in team_counts]
        today_by_channel_rows = db.query(Parents.recommend_channel, func.count(Parents.id)).filter(func.date(Parents.created_at) == func.curdate()).group_by(Parents.recommend_channel).all()
        today_by_channel = [{'channel': (ch or '未知'), 'count': c} for ch, c in today_by_channel_rows]
        individual_totals_sales_rows = db.query(Parents.salesperson_id, func.count(Parents.id)).filter(Parents.salesperson_id != None).group_by(Parents.salesperson_id).order_by(func.count(Parents.id).desc()).limit(10).all()
        individual_totals_sales = [{'staff_id': sid or '未分配', 'count': c} for sid, c in individual_totals_sales_rows]
        individual_totals_partner_rows = db.query(Parents.partner_id, func.count(Parents.id)).filter(Parents.partner_id != '').group_by(Parents.partner_id).order_by(func.count(Parents.id).desc()).limit(10).all()
        individual_totals_partner = [{'staff_id': pid or '未分配', 'count': c} for pid, c in individual_totals_partner_rows]
        individual_totals_social_rows = db.query(Parents.social_media_id, func.count(Parents.id)).group_by(Parents.social_media_id).order_by(func.count(Parents.id).desc()).limit(10).all()
        individual_totals_social = [{'staff_id': sid or '未知', 'count': c} for sid, c in individual_totals_social_rows]
        individual_daily_sales_today_rows = db.query(Parents.salesperson_id, func.count(Parents.id)).filter(Parents.salesperson_id != None, func.date(Parents.created_at) == func.curdate()).group_by(Parents.salesperson_id).order_by(func.count(Parents.id).desc()).limit(10).all()
        individual_daily_sales_today = [{'staff_id': sid or '未分配', 'count': c} for sid, c in individual_daily_sales_today_rows]
        daily_social_today_rows = db.query(Parents.social_media_id, func.count(Parents.id)).filter(func.date(Parents.created_at) == func.curdate()).group_by(Parents.social_media_id).order_by(func.count(Parents.id).desc()).limit(50).all()
        daily_social_today = [{'staff_id': sid or '未知', 'count': c} for sid, c in daily_social_today_rows]
        total_by_service_rows = db.query(Parents.service_category, func.count(Parents.id)).group_by(Parents.service_category).all()
        total_by_service = [{'service': svc or '未知', 'count': c} for svc, c in total_by_service_rows]
        alloc_social_to_sales_rows = db.query(Parents.social_media_id, Parents.salesperson_id, func.count(Parents.id)).filter(Parents.recommend_channel == '社媒', Parents.salesperson_id != None).group_by(Parents.social_media_id, Parents.salesperson_id).order_by(func.count(Parents.id).desc()).all()
        alloc_social_to_sales = [{'social_id': sid or '未知', 'sales_id': spid or '未分配', 'count': c} for sid, spid, c in alloc_social_to_sales_rows]
        alloc_social_to_partner_rows = db.query(Parents.social_media_id, Parents.partner_id, func.count(Parents.id)).filter(Parents.partner_id != '').group_by(Parents.social_media_id, Parents.partner_id).order_by(func.count(Parents.id).desc()).all()
        if not alloc_social_to_partner_rows:
            alloc_social_to_partner_rows = db.query(ProcessLogs.operator_id, ProcessLogs.assignee_id, func.count(ProcessLogs.id)) \
                .filter(ProcessLogs.action_type.in_(['新家长']), ProcessLogs.operator_role == '社媒', ProcessLogs.assignee_role == '合伙人') \
                .group_by(ProcessLogs.operator_id, ProcessLogs.assignee_id) \
                .order_by(func.count(ProcessLogs.id).desc()).all()
        app_logger.info(f"api_dashboard alloc_social_to_partner_rows: {alloc_social_to_partner_rows}")
        alloc_social_to_partner = [{'social_id': sid or '未知', 'partner_id': pid or '未分配', 'count': c} for sid, pid, c in alloc_social_to_partner_rows]
        alloc_partner_to_sales_rows = db.query(Parents.partner_id, Parents.salesperson_id, func.count(Parents.id)).filter(Parents.recommend_channel == '合伙人', Parents.partner_id != '', Parents.salesperson_id != None).group_by(Parents.partner_id, Parents.salesperson_id).order_by(func.count(Parents.id).desc()).all()
        alloc_partner_to_sales = [{'partner_id': pid or '未分配', 'sales_id': spid or '未分配', 'count': c} for pid, spid, c in alloc_partner_to_sales_rows]
        followup_stage_rows = db.query(Parents.followup_stage, func.count(Parents.id)).filter(Parents.current_status == '销售跟进中').group_by(Parents.followup_stage).all()
        followup_stage_stats = [{'stage': stage or '未设置', 'count': c} for stage, c in followup_stage_rows]
        resp = jsonify({'success': True, 'version': 'api_dashboard_v2', 'overview': {'overall_total': overall_total, 'today_total': today_total}, 'team_totals': team_totals, 'today_by_channel': today_by_channel, 'individual_totals_sales': individual_totals_sales, 'individual_totals_partner': individual_totals_partner, 'individual_totals_social': individual_totals_social, 'individual_daily_sales_today': individual_daily_sales_today, 'daily_social_today': daily_social_today, 'alloc_social_to_sales': alloc_social_to_sales, 'alloc_social_to_partner': alloc_social_to_partner, 'alloc_partner_to_sales': alloc_partner_to_sales, 'followup_stage_stats': followup_stage_stats, 'total_by_service': total_by_service})
        return resp
    finally:
        db.close()

@app.route('/api/parents', methods=['GET'])
def api_parents():
    q = request.args.get('q', '').strip()
    db = SessionLocal()
    try:
        rm_latest_ids = db.query(RawMessages.group_id.label('group_id'), func.max(RawMessages.id).label('max_id')).group_by(RawMessages.group_id).subquery()
        RM = aliased(RawMessages)
        query = db.query(Parents, RM.group_name).outerjoin(rm_latest_ids, rm_latest_ids.c.group_id == Parents.first_group_id).outerjoin(RM, RM.id == rm_latest_ids.c.max_id)
        if q:
            if q.isdigit():
                query = query.filter(Parents.id == int(q))
            else:
                query = query.filter(Parents.parent_code.like(f"%{q}%"))
        results = query.order_by(Parents.created_at.desc()).limit(50).all()
        items = []
        for p, group_name in results:
            items.append({
                'id': str(p.id),
                'parentCode': getattr(p, 'parent_code', None) or '',
                'attributeGroup': group_name or getattr(p, 'first_group_name', None) or 'None',
                'businessType': getattr(p, 'business_type', None) or 'None',
                'partner': getattr(p, 'partner_id', None) or 'None',
                'salesPerson': getattr(p, 'salesperson_id', None) or 'None',
                'salesTeam': getattr(p, 'sales_team', None) or 'None',
                'completedAmount': str(getattr(p, 'deal_amount', '') or '') or 'None',
                'studentId': getattr(p, 'student_id', None) or 'None',
                'studentStatus': getattr(p, 'followup_stage', None) or 'None',
                'prepaidAmount': str(getattr(p, 'prepayment_amount', '') or '') or 'None',
                'visitStatus': ('是' if getattr(p, 'is_visit', 0) else '否'),
                'createdTime': (p.created_at.isoformat() if getattr(p, 'created_at', None) else ''),
                'updatedTime': (p.updated_at.isoformat() if getattr(p, 'updated_at', None) else '')
            })
        resp = jsonify({'success': True, 'total': len(items), 'rows': items})
        return resp
    finally:
        db.close()

@app.route('/dashboard', methods=['GET'])
def ui_index():
    import os
    base_dir = os.path.dirname(os.path.abspath(__file__))
    index_path = os.path.join(base_dir, 'Optimize Page Content', 'src', 'index.html')
    return send_file(index_path)

@app.route('/', methods=['GET'])
def root_index():
    return redirect(url_for('ui_index'))

@app.route('/admin/parents/delete/<int:pid>', methods=['POST'])
@admin_login_required
def admin_parents_delete(pid: int):
    db = SessionLocal()
    try:
        obj = db.query(Parents).get(pid)
        if not obj:
            return jsonify({'success': False, 'error': '记录不存在'}), 404
        db.delete(obj)
        db.commit()
        return jsonify({'success': True})
    except Exception as e:
        db.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        db.close()

@app.route('/admin/parents/detail', methods=['GET'])
@admin_login_required
def admin_parents_detail():
    db = SessionLocal()
    try:
        detail_kind = request.args.get('detail_kind', '')
        sales_id = request.args.get('sales_id')
        partner_id = request.args.get('partner_id')
        social_id = request.args.get('social_id')
        stage = request.args.get('stage')
        today_only = request.args.get('today_only') in ('1', 'true', 'True')

        # 基础父级查询
        pq = db.query(Parents)

        # 复用原有过滤逻辑（销售/合伙人/社媒/阶段/今日新增等）
        if detail_kind == 'by_salesperson' and sales_id:
            pq = pq.filter(Parents.salesperson_id == sales_id)
        elif detail_kind == 'by_partner' and partner_id:
            pq = pq.filter(Parents.partner_id == partner_id)
        elif detail_kind == 'by_social' and social_id:
            pq = pq.filter(Parents.social_media_id == social_id)
        elif detail_kind == 'followup_stage' and stage:
            pq = pq.filter(and_(
                Parents.followup_stage == stage,
                Parents.current_status == '销售跟进中',
                or_(Parents.deal_amount == None, Parents.deal_amount == 0)
            ))
        elif detail_kind == 'daily_sales_today' and sales_id:
            pq = pq.filter(Parents.salesperson_id == sales_id)
            today_only = True
        elif detail_kind == 'daily_added' or (detail_kind == 'overall' and today_only):
            today_only = True

        # 分配明细过滤（社媒->销售、社媒->合伙人、合伙人->销售）
        if detail_kind == 'alloc_social_to_sales' and social_id and sales_id:
            pq = pq.filter(and_(Parents.recommend_channel == '社媒', Parents.social_media_id == social_id, Parents.salesperson_id == sales_id))
        elif detail_kind == 'alloc_social_to_partner' and social_id and partner_id:
            pq = pq.filter(Parents.partner_id == partner_id)
        elif detail_kind == 'alloc_partner_to_sales' and partner_id and sales_id:
            pq = pq.filter(and_(Parents.recommend_channel == '合伙人', Parents.partner_id == partner_id, Parents.salesperson_id == sales_id))
        app_logger.info(f"detail_kind={detail_kind}, applied filters: sales_id={sales_id}, partner_id={partner_id}, social_id={social_id}, count_after_filter={pq.count()}")

        # 今日过滤（按创建时间）
        if today_only:
            from datetime import datetime, timedelta
            now = datetime.now()
            start_day = datetime(now.year, now.month, now.day)
            end_day = start_day + timedelta(days=1)
            pq = pq.filter(and_(Parents.created_at >= start_day, Parents.created_at < end_day))

        # 限制数量并排序
        parents = pq.order_by(Parents.created_at.desc()).limit(500).all()
        if detail_kind == 'alloc_social_to_partner' and social_id and partner_id:
            parents = [p for p in parents if (p.social_media_id or '') == social_id]
        if not parents and detail_kind == 'alloc_social_to_partner' and social_id and partner_id:
            pid_rows = db.query(ProcessLogs.parent_id).filter(ProcessLogs.action_type.in_(['新家长']), ProcessLogs.operator_role == '社媒', ProcessLogs.assignee_role == '合伙人', ProcessLogs.operator_id == social_id, ProcessLogs.assignee_id == partner_id).all()
            ids = [pid for (pid,) in pid_rows]
            if ids:
                parents = db.query(Parents).filter(Parents.id.in_(ids)).order_by(Parents.created_at.desc()).limit(500).all()
        parent_ids = [p.id for p in parents]

        # 统计反馈与日志（横向展示用）
        feedback_counts = {}
        feedback_last_at = {}
        logs_counts = {}
        logs_last_at = {}

        if parent_ids:
            fb_rows = db.query(FollowupFeedback.parent_id, func.count(FollowupFeedback.id), func.max(FollowupFeedback.created_at)) \
                .filter(FollowupFeedback.parent_id.in_(parent_ids)) \
                .group_by(FollowupFeedback.parent_id).all()
            for pid, cnt, last_at in fb_rows:
                feedback_counts[pid] = cnt or 0
                feedback_last_at[pid] = last_at.isoformat() if last_at else None

            pl_rows = db.query(ProcessLogs.parent_id, func.count(ProcessLogs.id), func.max(ProcessLogs.created_at)) \
                .filter(ProcessLogs.parent_id.in_(parent_ids)) \
                .group_by(ProcessLogs.parent_id).all()
            for pid, cnt, last_at in pl_rows:
                logs_counts[pid] = cnt or 0
                logs_last_at[pid] = last_at.isoformat() if last_at else None

        # 组装横向数据（每个 parent_code 一行）
        rows = []
        for p in parents:
            rows.append({
                'parent_code': p.parent_code,
                'social_media_id': p.social_media_id,
                'partner_id': p.partner_id,
                'salesperson_id': p.salesperson_id,
                'followup_stage': p.followup_stage,
                'prepayment_amount': getattr(p, 'prepayment_amount', None),
                'visit_status': getattr(p, 'visit_status', None),
                'deal_amount': getattr(p, 'deal_amount', None),
                'created_at': p.created_at.isoformat() if getattr(p, 'created_at', None) else None,
                'updated_at': p.updated_at.isoformat() if getattr(p, 'updated_at', None) else None,
                'feedback_count': feedback_counts.get(p.id, 0),
                'last_feedback_at': feedback_last_at.get(p.id),
                'log_count': logs_counts.get(p.id, 0),
                'last_action_at': logs_last_at.get(p.id),
            })

        # 标题文案
        detail_title = '横向明细'
        if detail_kind == 'by_salesperson' and sales_id:
            detail_title = f"销售 {sales_id} 的例子"
        elif detail_kind == 'by_partner' and partner_id:
            detail_title = f"合伙人 {partner_id} 的例子"
        elif detail_kind == 'by_social' and social_id:
            detail_title = f"社媒 {social_id} 的例子"
        elif detail_kind == 'alloc_social_to_sales' and social_id and sales_id:
            detail_title = f"社媒 {social_id} 分配给销售 {sales_id} 的例子"
        elif detail_kind == 'alloc_social_to_partner' and social_id and partner_id:
            detail_title = f"社媒 {social_id} 分配给合伙人 {partner_id} 的例子"
        elif detail_kind == 'alloc_partner_to_sales' and partner_id and sales_id:
            detail_title = f"合伙人 {partner_id} 分配给销售 {sales_id} 的例子"
        elif detail_kind == 'followup_stage' and stage:
            detail_title = f"处于阶段 {stage} 的例子"
        elif today_only:
            detail_title = "今日新增的例子"

        resp = jsonify({
            'success': True,
            'title': detail_title,
            'total': len(rows),
            'rows': rows
        })
        return resp
    finally:
        db.close()

@app.route('/api/parents/detail', methods=['GET'])
def api_parents_detail():
    db = SessionLocal()
    try:
        detail_kind = request.args.get('detail_kind', '')
        sales_id = request.args.get('sales_id')
        partner_id = request.args.get('partner_id')
        social_id = request.args.get('social_id')
        stage = request.args.get('stage')
        team = request.args.get('team')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        today_only = request.args.get('today_only') in ('1', 'true', 'True')

        pq = db.query(Parents)

        if detail_kind == 'by_salesperson' and sales_id:
            pq = pq.filter(Parents.salesperson_id == sales_id)
        elif detail_kind == 'by_partner' and partner_id:
            pq = pq.filter(Parents.partner_id == partner_id)
        elif detail_kind == 'by_social' and social_id:
            pq = pq.filter(Parents.social_media_id == social_id)
        elif detail_kind == 'followup_stage' and stage:
            pq = pq.filter(and_(
                Parents.followup_stage == stage,
                Parents.current_status == '销售跟进中',
                or_(Parents.deal_amount == None, Parents.deal_amount == 0)
            ))
        elif detail_kind == 'by_sales_team' and team:
            pq = pq.filter(Parents.sales_team == team)
        elif detail_kind == 'daily_sales_today' and sales_id:
            pq = pq.filter(Parents.salesperson_id == sales_id)
            today_only = True
        elif detail_kind == 'daily_added' or (detail_kind == 'overall' and today_only):
            today_only = True

        if detail_kind == 'alloc_social_to_sales' and social_id and sales_id:
            pq = pq.filter(and_(Parents.recommend_channel == '社媒', Parents.social_media_id == social_id, Parents.salesperson_id == sales_id))
        elif detail_kind == 'alloc_social_to_partner' and social_id and partner_id:
            pq = pq.filter(and_(Parents.social_media_id == social_id, Parents.partner_id == partner_id))
        elif detail_kind == 'alloc_partner_to_sales' and partner_id and sales_id:
            pq = pq.filter(and_(Parents.recommend_channel == '合伙人', Parents.partner_id == partner_id, Parents.salesperson_id == sales_id))

        from datetime import datetime, timedelta, date as _date
        app_logger.info(f"detail_kind={detail_kind}, sales_id={sales_id}, partner_id={partner_id}, social_id={social_id}, stage={stage}, team={team}, start_date={start_date}, end_date={end_date}, today_only={today_only}")
        if start_date or end_date:
            try:
                def _parse_day(v: str):
                    try:
                        return datetime.strptime(v, '%Y-%m-%d').date()
                    except Exception:
                        return None
                sd = _parse_day(start_date) if start_date else None
                ed = _parse_day(end_date) if end_date else None
                if sd and ed:
                    start_dt = datetime(sd.year, sd.month, sd.day)
                    end_dt = datetime(ed.year, ed.month, ed.day) + timedelta(days=1)
                    pq = pq.filter(and_(Parents.created_at >= start_dt, Parents.created_at < end_dt))
                elif sd and not ed:
                    start_dt = datetime(sd.year, sd.month, sd.day)
                    pq = pq.filter(Parents.created_at >= start_dt)
                elif ed and not sd:
                    end_dt = datetime(ed.year, ed.month, ed.day) + timedelta(days=1)
                    pq = pq.filter(Parents.created_at < end_dt)
            except Exception:
                pass
        elif today_only:
            now = datetime.now()
            start_day = datetime(now.year, now.month, now.day)
            end_day = start_day + timedelta(days=1)
            pq = pq.filter(and_(Parents.created_at >= start_day, Parents.created_at < end_day))

        parents = pq.order_by(Parents.created_at.desc()).limit(500).all()
        parent_ids = [p.id for p in parents]

        feedback_counts = {}
        feedback_last_at = {}
        logs_counts = {}
        logs_last_at = {}

        if parent_ids:
            fb_rows = db.query(FollowupFeedback.parent_id, func.count(FollowupFeedback.id), func.max(FollowupFeedback.created_at)) \
                .filter(FollowupFeedback.parent_id.in_(parent_ids)) \
                .group_by(FollowupFeedback.parent_id).all()
            for pid, cnt, last_at in fb_rows:
                feedback_counts[pid] = cnt or 0
                feedback_last_at[pid] = last_at.isoformat() if last_at else None

            pl_rows = db.query(ProcessLogs.parent_id, func.count(ProcessLogs.id), func.max(ProcessLogs.created_at)) \
                .filter(ProcessLogs.parent_id.in_(parent_ids)) \
                .group_by(ProcessLogs.parent_id).all()
            for pid, cnt, last_at in pl_rows:
                logs_counts[pid] = cnt or 0
                logs_last_at[pid] = last_at.isoformat() if last_at else None

        rows = []
        for p in parents:
            rows.append({
                'parent_code': p.parent_code,
                'social_media_id': p.social_media_id,
                'partner_id': p.partner_id,
                'salesperson_id': p.salesperson_id,
                'followup_stage': p.followup_stage,
                'prepayment_amount': getattr(p, 'prepayment_amount', None),
                'visit_status': getattr(p, 'visit_status', None),
                'deal_amount': getattr(p, 'deal_amount', None),
                'created_at': p.created_at.isoformat() if getattr(p, 'created_at', None) else None,
                'updated_at': p.updated_at.isoformat() if getattr(p, 'updated_at', None) else None,
                'feedback_count': feedback_counts.get(p.id, 0),
                'last_feedback_at': feedback_last_at.get(p.id),
                'log_count': logs_counts.get(p.id, 0),
                'last_action_at': logs_last_at.get(p.id),
            })

        detail_title = '横向明细'
        if detail_kind == 'by_salesperson' and sales_id:
            detail_title = f"销售 {sales_id} 的例子"
        elif detail_kind == 'by_partner' and partner_id:
            detail_title = f"合伙人 {partner_id} 的例子"
        elif detail_kind == 'by_social' and social_id:
            detail_title = f"社媒 {social_id} 的例子"
        elif detail_kind == 'alloc_social_to_sales' and social_id and sales_id:
            detail_title = f"社媒 {social_id} 分配给销售 {sales_id} 的例子"
        elif detail_kind == 'alloc_social_to_partner' and social_id and partner_id:
            detail_title = f"社媒 {social_id} 分配给合伙人 {partner_id} 的例子"
        elif detail_kind == 'alloc_partner_to_sales' and partner_id and sales_id:
            detail_title = f"合伙人 {partner_id} 分配给销售 {sales_id} 的例子"
        elif detail_kind == 'followup_stage' and stage:
            detail_title = f"处于阶段 {stage} 的例子"
        elif detail_kind == 'by_sales_team' and team:
            detail_title = f"销售团队 {team} 的例子"
        elif today_only:
            detail_title = "今日新增的例子"

        return jsonify({'success': True, 'title': detail_title, 'total': len(rows), 'rows': rows, 'query_debug': {'kind': detail_kind, 'sales_id': sales_id, 'partner_id': partner_id, 'social_id': social_id}})
    finally:
        db.close()

@app.route('/api/parents/update', methods=['POST'])
def api_parents_update():
    db = SessionLocal()
    try:
        data = request.get_json(force=True) or {}
        pid = data.get('id')
        if not pid:
            return jsonify({'success': False, 'error': '缺少id'}), 400
        obj = db.query(Parents).get(int(pid))
        if not obj:
            return jsonify({'success': False, 'error': '记录不存在'}), 404
        if 'partner_id' in data:
            obj.partner_id = data.get('partner_id') or None
        if 'salesperson_id' in data:
            obj.salesperson_id = data.get('salesperson_id') or None
        if 'followup_stage' in data:
            obj.followup_stage = data.get('followup_stage') or None
        if 'prepayment_amount' in data:
            obj.prepayment_amount = data.get('prepayment_amount') or None
        if 'deal_amount' in data:
            obj.deal_amount = data.get('deal_amount') or None
        if 'visit_status' in data:
            vs = data.get('visit_status')
            obj.is_visit = 1 if str(vs).lower() in ('1','true','是') else 0
        db.commit()
        return jsonify({'success': True})
    except Exception as e:
        db.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        db.close()

@app.route('/api/parents/delete', methods=['POST'])
def api_parents_delete():
    db = SessionLocal()
    try:
        data = request.get_json(force=True) or {}
        pid = data.get('id')
        if not pid:
            return jsonify({'success': False, 'error': '缺少id'}), 400
        obj = db.query(Parents).get(int(pid))
        if not obj:
            return jsonify({'success': False, 'error': '记录不存在'}), 404
        db.delete(obj)
        db.commit()
        return jsonify({'success': True})
    except Exception as e:
        db.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        db.close()

@app.route('/admin/parents/edit/<int:pid>', methods=['GET', 'POST'])
@admin_login_required
def admin_parents_edit(pid: int):
    db = SessionLocal()
    try:
        obj = db.query(Parents).get(pid)
        if not obj:
            return render_template('admin_parents_edit.html', error='记录不存在', obj=None)
        if request.method == 'POST':
            # 除 created_at / updated_at 外均可更新
            obj.parent_code = request.form.get('parent_code', obj.parent_code)
            obj.recommend_channel = request.form.get('recommend_channel', obj.recommend_channel)
            obj.source_platform = request.form.get('source_platform', obj.source_platform)
            obj.service_category = request.form.get('service_category', obj.service_category)
            _intent_level = request.form.get('intent_level', obj.intent_level)
            obj.intent_level = _intent_level or None
            obj.current_status = request.form.get('current_status', obj.current_status)
            obj.social_media_id = request.form.get('social_media_id', obj.social_media_id)
            obj.partner_id = request.form.get('partner_id', obj.partner_id)
            _salesperson_id = request.form.get('salesperson_id', obj.salesperson_id)
            obj.salesperson_id = _salesperson_id or None
            _sales_team = request.form.get('sales_team', obj.sales_team)
            obj.sales_team = _sales_team or None

            deal_amount_str = request.form.get('deal_amount', '').strip()
            obj.deal_amount = Decimal(deal_amount_str) if deal_amount_str else None
            prepay_str = request.form.get('prepayment_amount', '').strip()
            obj.prepayment_amount = Decimal(prepay_str) if prepay_str else None

            obj.is_dse = 1 if request.form.get('is_dse') in ('1','on','true') else 0
            obj.student_id = request.form.get('student_id', obj.student_id)
            _followup_stage = request.form.get('followup_stage', obj.followup_stage)
            obj.followup_stage = _followup_stage or None
            obj.is_visit = 1 if request.form.get('is_visit') in ('1','on','true') else 0

            obj.first_group_id = request.form.get('first_group_id', obj.first_group_id)
            obj.first_group_name = request.form.get('first_group_name', obj.first_group_name)
            obj.requirement = request.form.get('requirement', obj.requirement)

            db.commit()
            return redirect(url_for('ui_index'))
        return render_template('admin_parents_edit.html', obj=obj)
    except Exception as e:
        db.rollback()
        return render_template('admin_parents_edit.html', error=str(e), obj=None)
    finally:
        db.close()




if __name__ == '__main__':
    app_logger.info("启动WeChat Robot Agent服务...")
    app_logger.info(f"服务地址: http://{config.FLASK_HOST}:{config.FLASK_PORT}")
    
    app.run(
        host=config.FLASK_HOST,
        port=config.FLASK_PORT,
        debug=config.FLASK_DEBUG
    )