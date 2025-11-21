"""
LangGraph智能体主逻辑
"""
from typing import Dict, Any, List, Optional, TypedDict, Annotated
import re
from langgraph.graph import StateGraph, END
# 移除对langchain_core.messages的依赖，避免触发transformers/torch

from parsers.message_parser import MessageTemplateParser, ParsedMessage
from agents.sql_agent import SQLAgent
from tools import sql_tools
from services.business_service import BusinessService
from models.database import SessionLocal
from utils.logger import app_logger
from utils.helpers import MessageFormatter
from config import config

class AgentState(TypedDict):
    """智能体状态"""
    messages: Annotated[List[str], "消息列表"]
    sender: str
    parsed_message: Optional[ParsedMessage]
    query_result: Optional[Dict[str, Any]]
    response: Optional[str]
    error: Optional[str]

class LangGraphAgent:
    """LangGraph智能体"""
    
    def __init__(self):
        self.message_parser = MessageTemplateParser()
        self.sql_agent = SQLAgent()
        self.business_service = BusinessService()
        # 简单会话记忆：按发送者保存最近回合的问答
        self._memory: Dict[str, List[Dict[str, str]]] = {}
        self._memory_max_turns: int = getattr(config, 'AGENT_MEMORY_MAX_TURNS', 6)
        
        # 取消在初始化阶段创建LLM，避免不必要的依赖加载
        
        # 构建图
        self.graph = self._build_graph()
    
    def _build_graph(self) -> StateGraph:
        """构建LangGraph图"""
        # 创建状态图
        workflow = StateGraph(AgentState)
        
        # 添加节点
        workflow.add_node("parse_message", self._parse_message_node)
        workflow.add_node("handle_business_message", self._handle_business_message_node)
        workflow.add_node("handle_query_message", self._handle_query_message_node)
        workflow.add_node("handle_help_message", self._handle_help_message_node)
        workflow.add_node("generate_response", self._generate_response_node)
        
        # 设置入口点
        workflow.set_entry_point("parse_message")
        
        # 添加条件边
        workflow.add_conditional_edges(
            "parse_message",
            self._route_message,
            {
                "business": "handle_business_message",
                "query": "handle_query_message",
                "help": "handle_help_message",
                "error": "generate_response"
            }
        )
        
        # 添加边
        workflow.add_edge("handle_business_message", "generate_response")
        workflow.add_edge("handle_query_message", "generate_response")
        workflow.add_edge("handle_help_message", "generate_response")
        workflow.add_edge("generate_response", END)
        
        return workflow.compile()
    
    def _parse_message_node(self, state: AgentState) -> AgentState:
        """解析消息节点"""
        try:
            # 获取最新消息
            last_message = state["messages"][-1]
            message_content = last_message
            sender = state["sender"]
            
            app_logger.info(f"解析消息: {message_content} (发送者: {sender})")
            
            # 解析消息
            parsed_message = self.message_parser.parse_message(message_content, sender)
            
            state["parsed_message"] = parsed_message
            
            if not parsed_message.is_valid:
                state["error"] = parsed_message.error_message
            
            return state
            
        except Exception as e:
            app_logger.error(f"解析消息失败: {e}")
            state["error"] = f"消息解析失败: {str(e)}"
            return state
    
    def _route_message(self, state: AgentState) -> str:
        """路由消息"""
        parsed_message = state.get("parsed_message")
        
        if state.get("error"):
            return "error"
        
        if not parsed_message:
            return "error"
        
        if parsed_message.message_type == "查询":
            # 在应用层已判断@机器人或API入口，智能体不再重复校验
            return "query"
        elif parsed_message.message_type == "模板帮助":
            return "help"
        elif parsed_message.message_type in ["新家长", "补全微信号", "合伙人接手", "放弃", "转销售", "销售接手", "反馈", "成交", "流失"]:
            return "business"
        else:
            return "error"

    def _handle_help_message_node(self, state: AgentState) -> AgentState:
        """处理模板帮助消息节点"""
        try:
            parsed_message = state["parsed_message"]
            help_type = None
            if parsed_message and parsed_message.content:
                help_type = parsed_message.content.get("help_type")
            help_info = self.message_parser.get_template_help(help_type)
            state["response"] = help_info
            return state
        except Exception as e:
            app_logger.error(f"处理模板帮助失败: {e}")
            state["error"] = f"获取模板帮助失败: {str(e)}"
            return state
    
    def _handle_business_message_node(self, state: AgentState) -> AgentState:
        """处理业务消息节点"""
        try:
            parsed_message = state["parsed_message"]
            
            app_logger.info(f"处理业务消息: {parsed_message.message_type}")
            
            # 调用业务服务处理
            result = self.business_service.process_message(parsed_message)
            
            if result.get("success"):
                state["response"] = result.get("message", "操作成功")
            else:
                state["error"] = result.get("error", "操作失败")
            
            return state
            
        except Exception as e:
            app_logger.error(f"处理业务消息失败: {e}")
            state["error"] = f"业务处理失败: {str(e)}"
            return state
    
    async def _handle_query_message_node(self, state: AgentState) -> AgentState:
        """处理查询消息节点"""
        try:
            parsed_message = state["parsed_message"]
            query = parsed_message.content.get("query", "")
            sender = state["sender"]
            # 获取会话记忆文本
            history_text = self._format_memory_text(sender)
            
            app_logger.info(f"处理查询消息: {query}")
            # ReAct 串联工具调用
            # 1) 获取数据库理解（M-Schema）——便于日志与调试（SQLAgent 的系统/用户提示中已注入）
            mschema = sql_tools.get_mschema()
            app_logger.debug(f"M-Schema 加载完成，长度: {len(mschema)}")

            # 2) 生成 SQL
            sql = await sql_tools.generate_sql(query, history_text)
            app_logger.info(f"初始SQL: {sql}")

            # 3) 评估并修复 SQL（若失败自动重生）
            fixed_sql = sql_tools.evaluate_sql(query, sql)
            if fixed_sql and fixed_sql != sql:
                app_logger.info(f"修复后SQL: {fixed_sql}")
            final_sql = fixed_sql or sql

            # 4) 执行 SQL
            result = sql_tools.execute_sql(final_sql)
            state["query_result"] = result

            # 5) AI 总结查询结果，并在控制台打印思维链内容
            if result.get("success"):
                summary_text = sql_tools.summarize_result(query, final_sql, result, history_text)
                # 附加一个简单的ReAct步骤日志，便于查看整体链路
                react_trace = [
                    "[ReAct] 步骤1: 获取 M-Schema",
                    "[ReAct] 步骤2: 生成 SQL",
                    "[ReAct] 步骤3: 评估与修复 SQL",
                    "[ReAct] 步骤4: 执行 SQL",
                    "[ReAct] 步骤5: AI 总结并生成群聊自然语言"
                ]
                app_logger.info("思维链步骤:\n" + "\n".join(react_trace))
                state["response"] = summary_text
            else:
                state["error"] = result.get("error", "查询失败")
            
            return state
            
        except Exception as e:
            app_logger.error(f"处理查询消息失败: {e}")
            state["error"] = f"查询处理失败: {str(e)}"
            return state
    
    def _generate_response_node(self, state: AgentState) -> AgentState:
        """生成响应节点"""
        try:
            if state.get("error"):
                # 错误响应
                error_msg = state["error"]
                parsed_message = state.get("parsed_message")
                raw_input = state["messages"][0] if state.get("messages") else ""
                # 若为带模板标签的消息但解析失败，告知已入库并附帮助
                if re.search(r"【(新家长|补全微信号|合伙人接手|放弃|转销售|销售接手|反馈|成交|流失)】", raw_input):
                    help_info = self.message_parser.get_template_help()
                    state["response"] = f"❗ 模板消息已入库，但存在问题\n{error_msg}\n\n{help_info}"
                else:
                    # 如果是格式错误，提供帮助信息
                    if "格式不正确" in error_msg or "模板格式" in error_msg:
                        help_info = self.message_parser.get_template_help()
                        state["response"] = f"❌ {error_msg}\n\n{help_info}"
                    else:
                        # 查询类型的错误不展示技术细节，返回友好提示
                        if parsed_message and parsed_message.message_type == "查询":
                            state["response"] = "❌ 查询执行遇到问题，已自动处理或记录。请稍后重试。"
                        else:
                            state["response"] = f"❌ {error_msg}"
            
            elif not state.get("response"):
                # 默认响应
                state["response"] = "✅ 消息已处理"
            
            # 添加AI消息到状态
            state["messages"].append(state["response"])  # 直接追加字符串

            # 将本轮问答写入记忆
            try:
                sender = state.get("sender", "")
                user_msg = state["messages"][0] if state.get("messages") else ""
                assistant_msg = state.get("response", "")
                self._append_memory(sender, user_msg, assistant_msg)
            except Exception:
                pass
            
            return state
            
        except Exception as e:
            app_logger.error(f"生成响应失败: {e}")
            state["response"] = f"❌ 系统错误: {str(e)}"
            return state

    def _is_robot_mentioned(self, message: str) -> bool:
        """判断消息是否@了机器人（本地规则）"""
        robot_patterns = [
            r"@机器人",
            r"@robot",
            r"@智能助手",
            r"@助手",
            r"@小助手"
        ]
        try:
            for pattern in robot_patterns:
                if re.search(pattern, message, re.IGNORECASE):
                    return True
            return False
        except Exception:
            return False
    
    async def process_message(self, message: str, sender: str) -> str:
        """处理消息"""
        try:
            # 创建初始状态
            initial_state = AgentState(
                messages=[message],
                sender=sender,
                parsed_message=None,
                query_result=None,
                response=None,
                error=None,
            )
            
            # 运行图
            final_state = await self.graph.ainvoke(initial_state)

            return final_state.get("response", "处理完成")
            
        except Exception as e:
            app_logger.error(f"处理消息失败: {e}")
            return f"❌ 系统错误: {str(e)}"
    
    def get_help_message(self) -> str:
        """获取帮助信息"""
        return """🤖 智能助手帮助信息

📝 支持的业务操作：
• 【新家长】- 录入新家长信息
• 【补全微信号】- 补全家长微信号
• 【接手】- 合伙人接手家长
• 【放弃】- 合伙人放弃家长
• 【转销售】- 转给销售跟进
• 【销售接手】- 销售接手家长
• 【反馈】- 销售反馈跟进情况
• 【成交】- 录入成交信息
• 【流失】- 录入流失信息

📊 支持的查询功能：
• 统计查询：今天新增多少家长？
• 状态查询：有多少家长在跟进中？
• 金额查询：本月成交金额是多少？
• 详细查询：查询某个家长的信息

💡 使用提示：
• 发送 "模板帮助 [类型]" 获取具体格式
• 直接用自然语言提问进行数据查询
• @机器人 + 你的问题或操作

示例：
@机器人 今天新增了多少家长？
@机器人 模板帮助 新家长
"""
    
    async def get_quick_stats(self) -> str:
        """获取快速统计"""
        try:
            result = self.sql_agent.get_quick_stats()
            
            if not result.get("success"):
                return f"❌ 获取统计失败: {result.get('error')}"
            
            stats = result.get("data", {})
            
            response = "📊 快速统计\n\n"
            response += f"👥 总家长数：{stats.get('total_parents', 0)}\n"
            response += f"🆕 今日新增：{stats.get('today_new', 0)}\n"
            response += f"💰 本月成交：¥{stats.get('monthly_revenue', 0):.2f}\n\n"
            
            response += "📈 状态分布：\n"
            status_stats = stats.get('status_stats', {})
            for status, count in status_stats.items():
                response += f"• {status}：{count}\n"
            
            return response
            
        except Exception as e:
            app_logger.error(f"获取快速统计失败: {e}")
            return f"❌ 获取统计失败: {str(e)}"
    def _append_memory(self, sender: str, user: str, assistant: str) -> None:
        """追加一轮问答到记忆，限最近 N 轮。"""
        if not sender:
            return
        turns = self._memory.get(sender, [])
        turns.append({"user": user, "assistant": assistant})
        # 仅保留最近 _memory_max_turns 轮
        if len(turns) > self._memory_max_turns:
            turns = turns[-self._memory_max_turns:]
        self._memory[sender] = turns

    def _format_memory_text(self, sender: str) -> str:
        """将最近回合格式化为简短文本。"""
        turns = self._memory.get(sender, [])
        if not turns:
            return ""
        # 限制每条消息长度，避免提示过长
        def _clip(s: str, max_len: int = 500) -> str:
            if s is None:
                return ""
            s = str(s)
            return s if len(s) <= max_len else (s[:max_len] + "…")
        lines: List[str] = []
        for i, t in enumerate(turns[-self._memory_max_turns:]):
            lines.append(f"[第{i+1}轮] 用户: {_clip(t.get('user', ''))}")
            lines.append(f"[第{i+1}轮] 助手: {_clip(t.get('assistant', ''))}")
        return "\n".join(lines)