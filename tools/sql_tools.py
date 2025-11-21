"""
LangGraph 工具函数封装：M-Schema、SQL 生成/评估/执行/格式化
"""
from typing import Dict, Any

from agents.sql_agent import SQLAgent

_agent: SQLAgent = None

def _get_agent() -> SQLAgent:
    global _agent
    if _agent is None:
        _agent = SQLAgent()
    return _agent

def get_mschema() -> str:
    """获取 M-Schema 字符串"""
    return _get_agent().get_mschema()

async def generate_sql(query: str, history: str = "") -> str:
    """生成 SQL（支持上下文记忆）"""
    return await _get_agent().generate_sql(query, history)

def evaluate_sql(natural_query: str, sql: str) -> str:
    """评估并修复 SQL"""
    return _get_agent().evaluate_and_fix_sql(natural_query, sql)

def execute_sql(sql: str) -> Dict[str, Any]:
    """执行 SQL 并返回结构化结果"""
    return _get_agent().execute_sql(sql)

def format_response(result: Dict[str, Any]) -> str:
    """格式化查询响应文本"""
    return _get_agent().format_query_response(result)

def summarize_result(natural_query: str, sql: str, result: Dict[str, Any], history: str = "") -> str:
    """根据查询结果生成自然语言总结（支持上下文记忆）"""
    return _get_agent().summarize_result(natural_query, sql, result, history)