"""
Text-to-SQL智能体
"""
import re
from typing import Dict, Any, List, Optional
import requests
from sqlalchemy import text
from models.database import SessionLocal
from config import config
from utils.logger import app_logger
from utils.helpers import MessageFormatter, DateTimeHelper
from typing import Optional
from utils.mschema_helper import get_db_schema

class SQLAgent:
    """SQL查询智能体"""
    
    def __init__(self):
        # 延迟初始化LLM，避免导入冲突
        self.llm = None
        # llm_mode: None / 'langchain' / 'openai_direct'
        self.llm_mode = None
        
        # 数据库表结构信息
        self.schema_info = self._get_schema_info()
        # M-Schema 字符串缓存（懒加载）
        self._mschema_str: Optional[str] = None
        
        # 提示模板和链在首次使用时创建
        self.sql_prompt = None
        self.chain = None

    def _ensure_chain(self):
        """确保LLM和Chain已初始化"""
        if self.llm is None:
            # 若配置强制直连模式，则直接启用直连
            force_mode = getattr(config, 'SQL_AGENT_MODE', 'auto')
            api_key = getattr(config, 'OPENAI_API_KEY', '') or ''
            if force_mode == 'direct':
                if (not api_key) or (api_key.strip().lower() in {"your-openai-api-key", "sk-your-openai-api-key"}):
                    app_logger.warning("OPENAI_API_KEY 未配置或为占位符，SQLAgent使用规则化回退")
                    return
                self.llm = 'openai_direct'
                self.llm_mode = 'openai_direct'
                app_logger.info("SQLAgent 已按配置强制使用 OpenAI 直连模式")
                # 直连模式不需要 chain
                return
            # 若未配置API密钥或使用占位符，则不初始化LLM，走规则化回退
            if (not api_key) or (api_key.strip().lower() in {"your-openai-api-key", "sk-your-openai-api-key"}):
                app_logger.warning("OPENAI_API_KEY 未配置或为占位符，SQLAgent使用规则化回退")
                return
            # 优先尝试使用 LangChain OpenAI；若导入或依赖失败，则退回 OpenAI 直连模式
            try:
                # 延迟导入，避免启动时加载transformers/torch
                from langchain_openai import ChatOpenAI
                from langchain_core.prompts import ChatPromptTemplate
                self.llm = ChatOpenAI(
                    model=getattr(config, 'OPENAI_MODEL', 'default'),
                    api_key=config.OPENAI_API_KEY,
                    base_url=getattr(config, 'OPENAI_BASE_URL', ''),
                    temperature=0.1
                )
                self.llm_mode = 'langchain'
                if self.sql_prompt is None:
                    self.sql_prompt = ChatPromptTemplate.from_messages([
                        ("system", self._get_system_prompt()),
                        ("human", "用户查询：{query}\n\n会话上下文（最近几轮）：\n{history}\n\n附加数据库理解（M-Schema）：\n{mschema}\n请生成对应的SQL查询语句。仅输出SQL，不要解释。不要使用反引号。")
                    ])
                app_logger.info("SQLAgent 已使用 LangChain OpenAI 初始化")
            except Exception as e:
                # 回退到直连 OpenAI 模式，不依赖 transformers/torch
                self.llm = 'openai_direct'
                self.llm_mode = 'openai_direct'
                app_logger.warning(f"LangChain 初始化失败，改用 OpenAI 直连模式：{e}")
        if self.chain is None and self.llm is not None:
            # 仅在使用 LangChain 时创建 chain
            if self.llm_mode == 'langchain':
                self.chain = self.sql_prompt | self.llm
    
    def _get_schema_info(self) -> str:
        """获取数据库表结构信息"""
        return """
数据库表结构（简化说明，用于指导LLM生成正确的字段名）：

1. staff_mapping (员工映射表)
   - staff_id: 员工ID (唯一，使用群内前缀昵称，如 SM_/HP_/XS_)
   - role: 角色 (社媒/合伙人/销售)
   - is_active: 是否激活
   - created_at, updated_at

2. parents (家长信息表)
   - id: 主键ID
   - parent_code: 家长编号 (以 P 开头)
   - source_platform: 来源平台 (抖音/小红书/...)
   - service_category: 业务类型 (DSE/留学/游学/其他)
   - intent_level: 意向度 (低/中/高)
   - current_status: 当前状态 (待接手/合伙人跟进中/销售跟进中/已成交/已流失)
   - social_media_id: 社媒人员ID
   - partner_id: 合伙人ID
   - salesperson_id: 销售人员ID
   - deal_amount: 成交金额
   - created_at, updated_at

3. parent_contacts (家长联系方式表)
   - parent_id: 家长ID (外键)
   - contact_type: 联系方式类型 (微信号/手机号/香港WS手机号/微信二维码昵称)
   - contact_value: 联系方式值
   - contact_desc: 说明
   - is_primary: 是否主要联系方式
   - created_at, updated_at

4. process_logs (处理日志表)
   - parent_id: 家长ID
   - action_type: 操作类型 (新家长/补全微信号/接手/放弃/转销售/销售接手/成交/流失/反馈)
   - operator_id, operator_role
   - message_content, notes
   - created_at

5. followup_feedback (跟进反馈表)
   - parent_id: 家长ID
   - feedback_type: 反馈类型
   - content: 反馈内容
   - is_dse: 是否DSE
  - operator_id
  - created_at
"""

    def _get_mschema_str(self) -> str:
        """获取 M-Schema 字符串（带缓存）。"""
        if not self._mschema_str:
            # 与 example.py 对齐：使用 db_schema（底层仍为 M-Schema 字符串）
            self._mschema_str = get_db_schema()
        return self._mschema_str
    
    def _get_system_prompt(self) -> str:
        """获取系统提示"""

        return f"""你是一个专业的 SQL 查询生成助手，专门为家长线索管理系统生成 SQL 查询。

【简化结构说明（关键表/字段）】
{self.schema_info}

【思考流程】（严格执行）
- 一步一步分析用户问题，识别时间范围、目标实体与需要的字段。
 - SQL 生成后评估其正确性与安全性（仅允许 SELECT）。
- 如评估失败或存在问题，SQL 失败重新生成并修正，确保可执行。

【查询与语法规则】
1. 只生成 SELECT 查询，不要生成 INSERT/UPDATE/DELETE/DDL 等修改语句。
2. 使用标准的 MySQL 语法。
3. 时间过滤：
   - “今天/当日” 使用 DATE(created_at) = CURDATE()
   - “本周/这周” 使用 YEARWEEK(created_at, 1) = YEARWEEK(CURDATE(), 1)
   - “过去一周/近一周/一周” 使用 DATE(created_at) >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
4. 当用户意图为 “名称/名单/列表/明细/详情/名字” 时，返回明细列表，不要统计数量。
   - 将“家长名称”视为 parents.parent_code（系统无 parent_name 字段）。
5. 关联规则：当关联 parent_contacts 时，必须使用 parents.id = parent_contacts.parent_id 进行连接。
6. 聚合规则：若 SELECT 中包含聚合函数（COUNT/SUM/AVG/MIN/MAX），所有非聚合列必须在 GROUP BY 中。
7. 联系方式过滤：除非用户明确要求“已验证/验证过/真实”，不要加入 is_verified = 1 条件；如需主联系方式，仅使用 is_primary = 1。
8. 兼容性：避免在 IN/EXISTS 子查询中使用 LIMIT，可改为 JOIN 派生表实现限制。
9. 返回的 SQL 必须是完整、可执行的；只返回 SQL，不要包含解释或反引号。

【示例】
- 今天新增的家长数量：SELECT COUNT(*) FROM parents WHERE DATE(created_at) = CURDATE();
- 本周家长编号列表：SELECT parent_code FROM parents WHERE YEARWEEK(created_at, 1) = YEARWEEK(CURDATE(), 1) ORDER BY created_at DESC LIMIT 50;
"""
    
    async def generate_sql(self, query: str, history: Optional[str] = None) -> str:
        """生成SQL查询语句"""
        try:
            # 确保链已初始化
            self._ensure_chain()
            if self.llm is None:
                # 规则化回退
                sql = self._rule_based_sql(query)
            else:
                if self.llm_mode == 'langchain' and self.chain is not None:
                    response = await self.chain.ainvoke({"query": query, "mschema": self._get_mschema_str(), "history": (history or "")})
                    sql = response.content.strip()
                elif self.llm_mode == 'openai_direct':
                    sql = self._llm_generate_sql_openai(query, history)
                else:
                    # 未知模式，回退规则
                    sql = self._rule_based_sql(query)
            
            # 清理SQL语句，移除可能的markdown格式
            if sql.startswith("```sql"):
                sql = sql[6:]
            if sql.startswith("```"):
                sql = sql[3:]
            if sql.endswith("```"):
                sql = sql[:-3]
            
            sql = sql.strip()
            # LLM/规则生成后进行自检与修正
            sql = self._review_and_fix_sql(query, sql)
            
            app_logger.info(f"生成SQL: {sql}")
            return sql
            
        except Exception as e:
            app_logger.error(f"生成SQL失败: {e}")
            raise Exception(f"生成SQL查询失败: {str(e)}")

    def _rule_based_sql(self, query: str) -> str:
        """在未配置LLM时的简单规则化SQL生成"""
        q = query.lower()
        wants_list = self._wants_list(query)
        # 1) 今天新增家长数量
        if ("今天" in q or "今日" in q) and ("新增" in q) and ("家长" in q) and not wants_list:
            return "SELECT COUNT(*) AS total FROM parents WHERE DATE(created_at) = CURDATE()"
        # 今天的家长名称/名单
        if ("今天" in q or "今日" in q) and wants_list:
            return "SELECT parent_code FROM parents WHERE DATE(created_at) = CURDATE() ORDER BY created_at DESC LIMIT 50"
        # 2) 总家长数或跟进中家长数量
        if ("总" in q or "多少" in q) and ("家长" in q) and ("跟进" not in q):
            return "SELECT COUNT(*) AS total FROM parents"
        if ("跟进" in q) and ("多少" in q or "有多少" in q or "数量" in q):
            return "SELECT COUNT(*) AS total FROM parents WHERE current_status IN ('合伙人跟进中','销售跟进中')"
        # 3) 本月成交金额（以父表的成交状态与更新时间统计）
        if ("本月" in q) and ("成交" in q) and ("金额" in q):
            return (
                "SELECT COALESCE(SUM(deal_amount), 0) AS total_amount FROM parents "
                "WHERE current_status = '已成交' AND MONTH(updated_at) = MONTH(CURDATE()) "
                "AND YEAR(updated_at) = YEAR(CURDATE())"
            )
        # 4+) 本周/这周/目前这一周 名称/名单列表
        if ("本周" in q or "这周" in q or "目前这一周" in q) and wants_list:
            return (
                "SELECT parent_code FROM parents "
                "WHERE YEARWEEK(created_at, 1) = YEARWEEK(CURDATE(), 1) "
                "ORDER BY created_at DESC LIMIT 50"
            )
        # 4++) 过去一周/近一周/最近一周/一周 名称/名单列表
        if ("过去一周" in q or "近一周" in q or "最近一周" in q or "一周" in q) and wants_list:
            return (
                "SELECT parent_code FROM parents "
                "WHERE DATE(created_at) >= DATE_SUB(CURDATE(), INTERVAL 7 DAY) "
                "ORDER BY created_at DESC LIMIT 50"
            )
        # 4) 查询家长编号详细（如：查询P202511057549）
        m = re.search(r"p\s*\d{9,}", query, flags=re.IGNORECASE)
        if ("查询" in q) and m:
            parent_code = m.group(0).upper().replace(" ", "")
            return (
                "SELECT p.parent_code, p.source_platform, p.current_status, p.partner_id, p.salesperson_id, "
                "p.intent_level, p.deal_amount, p.created_at, p.updated_at "
                "FROM parents p WHERE p.parent_code = '" + parent_code + "'"
            )
        # 5) 已成交家长列表
        if ("已成交" in q) and ("查询" in q or "所有" in q or "列表" in q):
            return (
                "SELECT parent_code, deal_amount, updated_at FROM parents "
                "WHERE current_status = '已成交' ORDER BY updated_at DESC LIMIT 50"
            )
        # 6) 状态统计
        if ("状态" in q) and ("统计" in q or "分布" in q):
            return "SELECT current_status AS status, COUNT(*) AS count FROM parents GROUP BY current_status"
        # 默认兜底
        raise Exception("LLM未配置，且无法根据规则生成SQL。请配置有效的OPENAI_API_KEY或使用支持的查询句式。")

    def _wants_list(self, natural_query: str) -> bool:
        q = (natural_query or "").lower()
        for kw in ["名称", "名单", "列表", "明细", "详情", "名字", "name", "names", "名称列表"]:
            if (natural_query and kw in natural_query) or (kw in q):
                return True
        return False

    def _static_sql_issues(self, sql: str, natural_query: str) -> List[str]:
        issues: List[str] = []
        s = (sql or "").strip()
        sup = s.upper()
        # 非 SELECT
        if not sup.startswith("SELECT"):
            issues.append("not_select")
        # 聚合与 GROUP BY
        agg = re.search(r"\b(COUNT|SUM|AVG|MIN|MAX)\s*\(", sup)
        m = re.search(r"^\s*SELECT\s+(.*?)\s+FROM\s", s, flags=re.IGNORECASE | re.DOTALL)
        select_list = m.group(1) if m else ""
        group_by = re.search(r"\bGROUP\s+BY\b", sup)
        if agg and ("," in select_list) and (group_by is None):
            issues.append("aggregate_with_non_aggregate_without_group_by")
        # 名称意图却返回聚合
        if self._wants_list(natural_query) and agg:
            issues.append("list_intent_but_aggregate")

        # 父表与联系方式关联字段错误：应当是 parents.id = parent_contacts.parent_id
        try:
            parents_alias_m = re.search(r"\bFROM\s+parents\s+(\w+)", s, flags=re.IGNORECASE)
            pc_alias_m = re.search(r"\bJOIN\s+parent_contacts\s+(\w+)", s, flags=re.IGNORECASE)
            if parents_alias_m and pc_alias_m:
                p_alias = parents_alias_m.group(1)
                pc_alias = pc_alias_m.group(1)
                # 查找 ON 子句
                on_m = re.search(rf"\bON\s+{re.escape(p_alias)}\.parent_id\s*=\s*{re.escape(pc_alias)}\.parent_id\b", s, flags=re.IGNORECASE)
                if on_m:
                    issues.append("wrong_join_parent_contacts")
        except Exception:
            pass

        # 兼容低版本 MySQL：IN 子查询中包含 LIMIT 会报 NotSupportedError
        try:
            if re.search(r"\bIN\s*\(\s*SELECT\b[\s\S]*?\bLIMIT\b", s, flags=re.IGNORECASE):
                issues.append("in_subquery_with_limit")
        except Exception:
            pass

        # 未明确要求“已验证/真实”却使用 is_verified 过滤
        try:
            nq = (natural_query or "")
            nq_lower = nq.lower()
            wants_verified = any(kw in nq for kw in ["已验证", "验证", "真实微信号", "实名", "已添加微信"]) or \
                              any(kw in nq_lower for kw in ["verified", "real wechat", "real name"])
            if not wants_verified and re.search(r"\bis_verified\s*=\s*1\b", s, flags=re.IGNORECASE):
                issues.append("unwarranted_is_verified_filter")
        except Exception:
            pass

        # 顶层 LIMIT 在 ORDER BY 之前（语法错误 1064）
        try:
            order_idx = self._find_top_level_clause_index(s, clause="ORDER BY")
            limit_idx = self._find_top_level_clause_index(s, clause="LIMIT")
            if (order_idx is not None) and (limit_idx is not None) and (limit_idx < order_idx):
                issues.append("top_level_limit_before_orderby")
        except Exception:
            pass
        
        # 检测：DISTINCT + ORDER BY 使用未在 SELECT 列表中的列（MySQL 3065）
        try:
            if re.search(r"^\s*SELECT\s+DISTINCT\b", s, flags=re.IGNORECASE):
                m_sel = re.search(r"^\s*SELECT\s+DISTINCT\s+([\s\S]+?)\s+FROM\s+", s, flags=re.IGNORECASE)
                m_ob = re.search(r"\bORDER\s+BY\s+([\s\S]+?)(?:\bLIMIT\b|;|$)", s, flags=re.IGNORECASE)
                if m_sel and m_ob:
                    select_part = m_sel.group(1)
                    order_part = m_ob.group(1)
                    order_exprs = [e.strip() for e in re.split(r",", order_part) if e.strip()]
                    def _strip_dir(e: str) -> str:
                        return re.sub(r"\s+(ASC|DESC)\b", "", e, flags=re.IGNORECASE).strip()
                    missing = []
                    for e in order_exprs:
                        base = _strip_dir(e)
                        if not re.search(re.escape(base), select_part, flags=re.IGNORECASE):
                            missing.append(base)
                    if missing:
                        issues.append("distinct_orderby_not_in_select")
        except Exception:
            pass
        return issues

    def _review_and_fix_sql(self, natural_query: str, sql: str) -> str:
        issues = self._static_sql_issues(sql, natural_query)
        if not issues:
            return sql
        # 先进行已知可确定的规则修复
        fixed_sql = sql
        try:
            parents_alias_m = re.search(r"\bFROM\s+parents\s+(\w+)", fixed_sql, flags=re.IGNORECASE)
            pc_alias_m = re.search(r"\bJOIN\s+parent_contacts\s+(\w+)", fixed_sql, flags=re.IGNORECASE)
            if parents_alias_m and pc_alias_m and ("wrong_join_parent_contacts" in issues):
                p_alias = parents_alias_m.group(1)
                pc_alias = pc_alias_m.group(1)
                fixed_sql = re.sub(
                    rf"\bON\s+{re.escape(p_alias)}\.parent_id\s*=\s*{re.escape(pc_alias)}\.parent_id\b",
                    f"ON {p_alias}.id = {pc_alias}.parent_id",
                    fixed_sql,
                    flags=re.IGNORECASE
                )
        except Exception:
            pass

        # 修复：IN 子查询包含 LIMIT，改写为 JOIN 派生表
        try:
            if "in_subquery_with_limit" in issues:
                fixed_sql = self._rewrite_in_with_limit_to_join(fixed_sql)
        except Exception:
            pass

        # 修复：未要求验证却过滤 is_verified=1，删除该条件
        try:
            if "unwarranted_is_verified_filter" in issues:
                fixed_sql = self._remove_unwarranted_is_verified(fixed_sql)
        except Exception:
            pass

        # 修复：顶层 LIMIT/ORDER BY 顺序（若错误则自动重排，允许回退策略）
        try:
            fixed_sql = self._reorder_top_level_order_limit(fixed_sql)
        except Exception:
            pass

        # 修复：DISTINCT + ORDER BY 非选择列 —— 改写为分组派生表排序
        try:
            if "distinct_orderby_not_in_select" in issues:
                fixed_sql = self._fix_distinct_orderby_not_in_select(fixed_sql)
        except Exception:
            pass

        # 如仍存在其他问题，交由 LLM 进行审查修复
        try:
            fixed = self._llm_review_sql_openai(natural_query, fixed_sql, issues)
            return fixed or fixed_sql
        except Exception:
            return fixed_sql

    def _rewrite_in_with_limit_to_join(self, sql: str) -> str:
        """将 WHERE <alias>.id IN (SELECT ... LIMIT N) 改写为 JOIN 派生表形式。
        该改写主要兼容较旧 MySQL 版本，避免 "LIMIT & IN subquery" 错误。
        """
        s = sql
        # 1) 获取 parents 表别名（支持带库名或不带库名）
        m_alias = re.search(r"\bFROM\s+(?:lead_management\.)?parents\s+(\w+)\b", s, flags=re.IGNORECASE)
        alias = m_alias.group(1) if m_alias else None
        if not alias:
            return sql
        # 2) 找到 IN 子查询块
        m_in = re.search(rf"{re.escape(alias)}\.id\s+IN\s*\(\s*(SELECT[\s\S]+?)\s*\)", s, flags=re.IGNORECASE)
        if not m_in:
            return sql
        inner_select = m_in.group(1).strip()
        # 3) 构造 JOIN 派生表语句
        join_clause = f"INNER JOIN ({inner_select}) AS recent_parent_ids ON {alias}.id = recent_parent_ids.parent_id"

        # 4) 插入 JOIN：紧跟在父表之后
        m_from_parents = re.search(r"\bFROM\s+(?:lead_management\.)?parents\s+\w+", s, flags=re.IGNORECASE)
        if not m_from_parents:
            return sql
        insert_pos = m_from_parents.end()
        s_with_join = s[:insert_pos] + "\n " + join_clause + s[insert_pos:]

        # 5) 简化删除：移除 WHERE 中的 IN 子查询谓词（对当前问题场景足够）
        s_final = re.sub(
            rf"\bWHERE\s+{re.escape(alias)}\.id\s+IN\s*\(\s*SELECT[\s\S]+?\)\s*",
            " ",
            s_with_join,
            flags=re.IGNORECASE
        )
        return s_final

    def _find_top_level_clause_index(self, s: str, clause: str) -> Optional[int]:
        """查找顶层（括号深度为0）指定子句的起始索引。大小写不敏感。"""
        pattern = re.compile(r"\b" + re.escape(clause).replace(" ", "\\s+") + r"\b", re.IGNORECASE)
        for m in pattern.finditer(s):
            pos = m.start()
            depth = 0
            for ch in s[:pos]:
                if ch == '(': depth += 1
                elif ch == ')': depth = max(0, depth - 1)
            if depth == 0:
                return pos
        return None

    def _reorder_top_level_order_limit(self, s: str) -> str:
        """确保顶层 ORDER BY 在 LIMIT 之前。若发现 LIMIT 在 ORDER BY 前则进行重排。"""
        order_idx = self._find_top_level_clause_index(s, "ORDER BY")
        limit_idx = self._find_top_level_clause_index(s, "LIMIT")
        # 回退策略：若顶层识别失败，使用全局最后出现位置作为近似顶层
        if order_idx is None:
            order_idx = s.upper().rfind("ORDER BY")
        if limit_idx is None:
            limit_idx = s.upper().rfind("LIMIT")
        if order_idx is None or limit_idx is None or order_idx < 0 or limit_idx < 0:
            return s
        if limit_idx < order_idx:
            limit_start = limit_idx
            limit_end = order_idx
            order_start = order_idx
            semi_idx = s.rfind(';')
            if semi_idx == -1:
                order_end = len(s)
                suffix = ""
            else:
                order_end = semi_idx
                suffix = s[semi_idx:]
            prefix = s[:limit_start]
            limit_clause = s[limit_start:limit_end].strip()
            order_clause = s[order_start:order_end].strip()
            rebuilt = prefix.rstrip() + "\n" + order_clause + " " + limit_clause + suffix
            return rebuilt
        return s

    def _fix_distinct_orderby_not_in_select(self, sql: str) -> str:
        """修复 MySQL 3065：当 SELECT DISTINCT 与 ORDER BY 的列不在选择列表中时，
        改写为：对 DISTINCT 选择列做分组，并对排序列做聚合（MAX/MIN），在外层按聚合结果排序。
        形态：
        SELECT <distinct_cols> FROM (
          SELECT <distinct_cols>, AGG(order_col1) AS __order_col1, ...
          FROM ...
          GROUP BY <distinct_cols>
        ) t ORDER BY __order_col1 [ASC|DESC], ... [LIMIT ...];
        """
        s = (sql or "").strip()
        if not re.search(r"^\s*SELECT\s+DISTINCT\b", s, flags=re.IGNORECASE):
            return sql
        # 使用正则直接定位 FROM 与 ORDER BY，避免顶层识别失败导致不改写
        m_sel = re.search(r"^\s*SELECT\s+DISTINCT\s+([\s\S]+?)\s+FROM\s+", s, flags=re.IGNORECASE)
        if not m_sel:
            return sql
        select_part = m_sel.group(1).strip()
        m_from = re.search(r"\bFROM\b", s, flags=re.IGNORECASE)
        m_order = re.search(r"\bORDER\s+BY\s+([\s\S]+?)(?:\bLIMIT\b|;|$)", s, flags=re.IGNORECASE)
        if not m_order:
            return sql
        if not m_from:
            return sql
        order_start_idx = m_order.start()
        from_start_idx = m_from.start()
        body_until_order = s[from_start_idx:order_start_idx].rstrip()
        order_part = m_order.group(1).strip()
        m_limit = re.search(r"\bLIMIT\s+[\d\s,]+(?:;|$)", s, flags=re.IGNORECASE)
        limit_clause = m_limit.group(0).strip() if m_limit else ""

        raw_order_items = [i.strip() for i in re.split(r",", order_part) if i.strip()]
        order_items = []  # [(expr, dir, alias)]
        for idx, item in enumerate(raw_order_items, start=1):
            m_dir = re.search(r"\b(ASC|DESC)\b", item, flags=re.IGNORECASE)
            direction = (m_dir.group(1).upper() if m_dir else "ASC")
            expr = re.sub(r"\bASC\b|\bDESC\b", "", item, flags=re.IGNORECASE).strip()
            alias = f"__order_col{idx}"
            order_items.append((expr, direction, alias))

        agg_parts = []
        order_outer_parts = []
        for expr, direction, alias in order_items:
            agg_fn = "MAX" if direction == "DESC" else "MIN"
            agg_parts.append(f"{agg_fn}({expr}) AS {alias}")
            order_outer_parts.append(f"{alias} {direction}")

        inner_select = f"SELECT {select_part}, " + ", ".join(agg_parts) + f"\n{body_until_order}\n GROUP BY {select_part}"
        # 外层选择列需去除表别名前缀或采用别名
        select_items = [i.strip() for i in re.split(r",", select_part) if i.strip()]
        outer_labels = []
        for item in select_items:
            m_as = re.search(r"\bAS\s+([A-Za-z_][A-Za-z0-9_]*)\b", item, flags=re.IGNORECASE)
            if m_as:
                outer_labels.append(m_as.group(1))
                continue
            m_col = re.search(r"\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\b", item)
            if m_col:
                outer_labels.append(m_col.group(2))
                continue
            # 尝试直接取最后的标识符作为列名
            m_plain = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", item)
            outer_labels.append(m_plain[-1] if m_plain else f"col_{len(outer_labels)+1}")

        outer_select = (
            "SELECT " + ", ".join(outer_labels) + " FROM (\n" +
            inner_select + "\n) AS __t\n ORDER BY " + ", ".join(order_outer_parts)
        )
        if limit_clause:
            outer_select += f" {limit_clause.rstrip(';')}"
        if s.endswith(";"):
            outer_select += ";"
        return outer_select

    def _remove_unwarranted_is_verified(self, sql: str) -> str:
        """删除 WHERE/AND 中的 is_verified=1 条件（当用户未要求"已验证"时）。"""
        s = sql
        # 删除 AND 列表中的过滤项
        s = re.sub(r"\s+AND\s+\w+\.is_verified\s*=\s*1\b", "", s, flags=re.IGNORECASE)
        # 删除 "WHERE is_verified=1 AND"，保留其他条件
        s = re.sub(r"\bWHERE\s+\w+\.is_verified\s*=\s*1\s+AND\s+", " WHERE ", s, flags=re.IGNORECASE)
        # 删除孤立的 "WHERE is_verified=1"
        s = re.sub(r"\bWHERE\s+\w+\.is_verified\s*=\s*1\b", " ", s, flags=re.IGNORECASE)
        return s

    def _llm_review_sql_openai(self, natural_query: str, sql: str, issues: List[str]) -> str:
        base_url = getattr(config, 'OPENAI_BASE_URL', '')
        model = getattr(config, 'OPENAI_MODEL', 'gpt-4o-mini')
        api_key = getattr(config, 'OPENAI_API_KEY', '')
        endpoint = base_url.rstrip('/') + '/chat/completions'
        headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        system_prompt = (
            "你是严格的 MySQL SQL 审查与修复助手。\n"
            "任务：根据用户意图与问题清单，给出可执行且更合理的 SQL。\n"
            "要求：\n"
            "- 仅返回一条修正后的 SQL，不要解释。\n"
            "- 只允许 SELECT 查询，禁止 DDL/DML。\n"
            "- 若用户意图为 名称/名单/列表，必须返回明细列表（非聚合），优先使用 parents.parent_code。\n"
            "- ‘本周/这周’ 用 YEARWEEK(created_at,1)=YEARWEEK(CURDATE(),1)；‘过去一周/近一周/一周’ 用最近7天。\n"
            "- 若存在聚合且包含非聚合列，必须补充 GROUP BY 或改为明细。\n"
            "- 对明细结果限制为 LIMIT 50，并按 created_at DESC 排序。\n"
            "- 关联 parent_contacts 时，必须使用 parents.id = parent_contacts.parent_id 进行连接，不存在 parents.parent_id 字段。\n"
            "- 联系方式过滤：除非用户明确要求‘已验证/验证过/真实’，不要加入 is_verified = 1；如需主联系方式，仅使用 is_primary = 1。\n"
            "- 兼容性：避免在 IN/EXISTS 子查询中使用 LIMIT，请改写为 JOIN 派生表以实现限制。\n"
        )
        messages = [
            {'role': 'system', 'content': system_prompt},
            {
                'role': 'user',
                'content': (
                    f"用户意图: {natural_query}\n"
                    f"已生成 SQL: {sql}\n"
                    f"检测到问题: {', '.join(issues)}\n"
                    "请输出修正后的 SQL（仅 SQL）。"
                )
            }
        ]
        payload = {
            'model': model,
            'temperature': 0.0,
            'messages': messages
        }
        resp = requests.post(endpoint, headers=headers, json=payload, timeout=30)
        if resp.status_code != 200:
            raise Exception(f"LLM接口错误: {resp.status_code} {resp.text}")
        data = resp.json()
        content = (
            data.get('choices', [{}])[0]
            .get('message', {})
            .get('content', '')
        )
        if not content:
            raise Exception("LLM无返回内容")
        fixed_sql = content.strip()
        # 清理markdown
        if fixed_sql.startswith("```sql"):
            fixed_sql = fixed_sql[6:]
        if fixed_sql.startswith("```"):
            fixed_sql = fixed_sql[3:]
        if fixed_sql.endswith("```"):
            fixed_sql = fixed_sql[:-3]
        return fixed_sql.strip()

    def _llm_generate_sql_openai(self, query: str, history: Optional[str] = None) -> str:
        """通过 OpenAI/Moonshot 直连接口生成 SQL，避免 transformers/torch 依赖"""
        base_url = getattr(config, 'OPENAI_BASE_URL', '')
        model = getattr(config, 'OPENAI_MODEL', 'gpt-4o-mini')
        api_key = getattr(config, 'OPENAI_API_KEY', '')
        if not (base_url and model and api_key):
            raise Exception("缺少 OPENAI 配置")

        # 兼容 OpenAI 与 Moonshot 的 chat/completions 接口
        endpoint = base_url.rstrip('/') + '/chat/completions'
        headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        db_schema = self._get_mschema_str()
        payload = {
            'model': model,
            'temperature': 0.1,
            'messages': [
                {'role': 'system', 'content': self._get_system_prompt()},
                {'role': 'user', 'content': f"用户查询：{query}\n\n会话上下文（最近几轮）：\n{history or ''}\n\n附加数据库理解（Schema）：\n{db_schema}\n请生成对应的SQL查询语句。仅输出SQL，不要解释。不要使用反引号。"}
            ]
        }
        try:
            resp = requests.post(endpoint, headers=headers, json=payload, timeout=30)
            if resp.status_code != 200:
                raise Exception(f"LLM接口错误: {resp.status_code} {resp.text}")
            data = resp.json()
            # OpenAI/Moonshot 兼容：choices[0].message.content
            content = (
                data.get('choices', [{}])[0]
                .get('message', {})
                .get('content', '')
            )
            if not content:
                raise Exception("LLM无返回内容")
            sql = content.strip()
            # 清理markdown
            if sql.startswith("```sql"):
                sql = sql[6:]
            if sql.startswith("```"):
                sql = sql[3:]
            if sql.endswith("```"):
                sql = sql[:-3]
            return sql.strip()
        except Exception as e:
            app_logger.error(f"OpenAI直连生成SQL失败: {e}")
            # 失败时仍回退规则化，提升可用性
            return self._rule_based_sql(query)
    
    def execute_sql(self, sql: str) -> Dict[str, Any]:
        """执行SQL查询"""
        db = SessionLocal()
        try:
            # 安全检查：只允许SELECT查询
            sql_upper = sql.upper().strip()
            if not sql_upper.startswith('SELECT'):
                raise Exception("只允许执行SELECT查询")
            
            # 检查是否包含危险关键词（整词匹配，避免误伤如 created_at / updated_at）
            dangerous_pattern = re.compile(r"\b(DROP|DELETE|UPDATE|INSERT|ALTER|CREATE|TRUNCATE|REPLACE|MERGE|GRANT|REVOKE)\b", re.IGNORECASE)
            if dangerous_pattern.search(sql):
                # 提取具体关键词用于提示
                match = dangerous_pattern.search(sql)
                keyword = match.group(1).upper() if match else 'UNKNOWN'
                raise Exception(f"SQL查询包含危险关键词: {keyword}")
            
            # 执行查询
            result = db.execute(text(sql))
            
            # 获取列名
            columns = list(result.keys()) if result.keys() else []
            
            # 获取数据
            rows = result.fetchall()
            
            # 转换为字典列表
            data = []
            for row in rows:
                row_dict = {}
                for i, column in enumerate(columns):
                    value = row[i]
                    # 处理日期时间类型
                    if hasattr(value, 'isoformat'):
                        value = value.isoformat()
                    row_dict[column] = value
                data.append(row_dict)
            
            return MessageFormatter.format_query_result(
                data=data,
                total=len(data),
                query=sql
            )
            
        except Exception as e:
            err_text = str(e)
            app_logger.error(f"执行SQL失败: {err_text}")
            # 兼容处理：低版本 MySQL 不支持 IN 子查询中使用 LIMIT，尝试改写并重试
            if "LIMIT & IN/ALL/ANY/SOME subquery" in err_text:
                try:
                    rewritten = self._rewrite_in_with_limit_to_join(sql)
                    if rewritten and rewritten != sql:
                        app_logger.info("自动修复 SQL：将 IN+LIMIT 改写为 JOIN 派生表后重试执行")
                        result = db.execute(text(rewritten))
                        columns = list(result.keys()) if result.keys() else []
                        rows = result.fetchall()
                        data = []
                        for row in rows:
                            row_dict = {}
                            for i, column in enumerate(columns):
                                value = row[i]
                                if hasattr(value, 'isoformat'):
                                    value = value.isoformat()
                                row_dict[column] = value
                            data.append(row_dict)
                        return MessageFormatter.format_query_result(
                            data=data,
                            total=len(data),
                            query=rewritten
                        )
                except Exception as e2:
                    app_logger.warning(f"自动修复失败: {e2}")
            # 兼容处理：MySQL 3065 —— DISTINCT + ORDER BY 非选择列，尝试改写为分组派生表并重试
            if ("incompatible with DISTINCT" in err_text) or (
                ("not in SELECT list" in err_text) and ("DISTINCT" in sql.upper())
            ):
                try:
                    rewritten = self._fix_distinct_orderby_not_in_select(sql)
                    if rewritten and rewritten != sql:
                        app_logger.info("自动修复 SQL：DISTINCT+ORDER BY 非选择列改写为分组派生表后重试执行")
                        result = db.execute(text(rewritten))
                        columns = list(result.keys()) if result.keys() else []
                        rows = result.fetchall()
                        data = []
                        for row in rows:
                            row_dict = {}
                            for i, column in enumerate(columns):
                                value = row[i]
                                if hasattr(value, 'isoformat'):
                                    value = value.isoformat()
                                row_dict[column] = value
                            data.append(row_dict)
                        return MessageFormatter.format_query_result(
                            data=data,
                            total=len(data),
                            query=rewritten
                        )
                except Exception as e2:
                    app_logger.warning(f"自动修复失败: {e2}")
            # 返回用户友好错误，不暴露技术细节
            return MessageFormatter.format_error_response("查询执行遇到问题，请稍后重试或简化查询。")
        finally:
            db.close()

    async def query(self, natural_query: str) -> Dict[str, Any]:
        """处理自然语言查询"""
        try:
            app_logger.info(f"处理查询: {natural_query}")
            
            # 生成SQL
            sql = await self.generate_sql(natural_query)
            
            # 执行SQL
            result = self.execute_sql(sql)
            
            # 添加原始查询信息
            if result.get('success'):
                result['natural_query'] = natural_query
                result['generated_sql'] = sql
            
            return result
            
        except Exception as e:
            app_logger.error(f"查询处理失败: {e}")
            return MessageFormatter.format_error_response(f"查询处理失败: {str(e)}")

    # 工具封装：供 LangGraph 调用
    def get_mschema(self) -> str:
        """公开方法：返回 M-Schema 字符串"""
        return self._get_mschema_str()

    def evaluate_and_fix_sql(self, natural_query: str, sql: str) -> str:
        """公开方法：评估并修复 SQL"""
        return self._review_and_fix_sql(natural_query, sql)

    def summarize_result(self, natural_query: str, sql: str, result: Dict[str, Any], history: Optional[str] = None) -> str:
        """根据查询结果生成自然语言总结。优先使用 LLM，失败则回退规则化总结。"""
        try:
            if not result.get('success'):
                return f"❌ 查询失败：{result.get('error', '未知错误')}"
            # 优先使用直连 LLM
            base_url = getattr(config, 'OPENAI_BASE_URL', '')
            model = getattr(config, 'OPENAI_MODEL', 'gpt-4o-mini')
            api_key = getattr(config, 'OPENAI_API_KEY', '')
            if base_url and model and api_key:
                summary, thoughts = self._llm_summarize_result_openai(natural_query, sql, result, history)
                # 打印思维链内容到控制台
                if thoughts:
                    app_logger.info("思维链内容:\n" + thoughts)
                return summary
        except Exception as e:
            app_logger.warning(f"LLM 总结失败，回退规则化：{e}")
        # 规则化回退
        data = result.get('data', [])
        total = result.get('total', 0)
        if total == 0:
            return "📊 没有查询到相关记录。"
        # 若包含名字/名称列，拼接友好列表
        name_keys = ['staff_id', 'name', 'nickname', 'sales_name', 'operator_name']
        first_row = data[0] if data else {}
        key_for_name = next((k for k in first_row.keys() if any(nk in k.lower() for nk in name_keys)), None)
        if key_for_name:
            names = [str(row.get(key_for_name)) for row in data if row.get(key_for_name) is not None]
            names = [n for n in names if n]
            unique_names = []
            for n in names:
                if n not in unique_names:
                    unique_names.append(n)
            if unique_names:
                joined = '、'.join(unique_names[:10])
                more = f"，等{len(unique_names)}人" if len(unique_names) > 10 else ""
                return f"📣 这周成交的对应销售名字：{joined}{more}。"
        # 否则返回强化版表格摘要
        return self.format_query_response(result)

    def _llm_summarize_result_openai(self, natural_query: str, sql: str, result: Dict[str, Any], history: Optional[str] = None) -> tuple[str, str]:
        """通过 OpenAI/Moonshot 直连接口对结果做自然语言总结，返回 (summary, thoughts_text)。"""
        base_url = getattr(config, 'OPENAI_BASE_URL', '')
        model = getattr(config, 'OPENAI_MODEL', 'gpt-4o-mini')
        api_key = getattr(config, 'OPENAI_API_KEY', '')
        endpoint = base_url.rstrip('/') + '/chat/completions'
        headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        data = result.get('data', [])
        total = result.get('total', 0)
        sample_limit = 10
        sample_rows = data[:sample_limit]
        def _row_to_text(row: Dict[str, Any]) -> str:
            parts = []
            for k, v in row.items():
                if v is None:
                    continue
                if hasattr(v, 'isoformat'):
                    v = v.isoformat()
                parts.append(f"{k}={v}")
            return ", ".join(parts)
        sample_text = "\n".join(_row_to_text(r) for r in sample_rows) or "(无示例数据)"
        system_prompt = (
            "你是群聊里的数据助理。阅读SQL查询结果，用中文输出简洁、自然的总结。"\
            "输出两个部分：\n"\
            "思维链：用1-6行说明你如何分析（不要泄露隐私或SQL细节）。\n"\
            "最终回复：一段适合群聊的简短自然语言，避免技术细节，必要时列出关键名字或数量。"
        )
        user_prompt = (
            f"用户查询：{natural_query}\n"\
            f"SQL：{sql}\n"\
            f"结果行数：{total}\n"\
            f"示例数据（最多{sample_limit}行）：\n{sample_text}\n"\
            f"会话上下文（最近几轮）：\n{history or ''}\n"\
            "请严格以如下格式返回：\n思维链: ...\n最终回复: ..."
        )
        payload = {
            'model': model,
            'temperature': 0.2,
            'messages': [
                {'role': 'system', 'content': system_prompt},
                {'role': 'user', 'content': user_prompt}
            ]
        }
        resp = requests.post(endpoint, headers=headers, json=payload, timeout=30)
        if resp.status_code != 200:
            raise Exception(f"LLM接口错误: {resp.status_code} {resp.text}")
        data_json = resp.json()
        content = (
            data_json.get('choices', [{}])[0]
            .get('message', {})
            .get('content', '')
        )
        if not content:
            raise Exception("LLM无返回内容")
        text = content.strip()
        # 尝试解析两段式输出
        thoughts_text = ""
        summary_text = text
        try:
            m1 = re.search(r"思维链\s*[:：]\s*(.*?)(?:\n\s*最终回复\s*[:：])", text, flags=re.DOTALL)
            m2 = re.search(r"最终回复\s*[:：]\s*(.*)$", text, flags=re.DOTALL)
            if m1:
                thoughts_text = m1.group(1).strip()
            if m2:
                summary_text = m2.group(1).strip()
        except Exception:
            thoughts_text = ""
        return summary_text, thoughts_text
    
    def get_quick_stats(self) -> Dict[str, Any]:
        """获取快速统计信息"""
        try:
            stats = {}
            db = SessionLocal()
            
            # 总家长数
            result = db.execute(text("SELECT COUNT(*) as total FROM parents"))
            stats['total_parents'] = result.fetchone()[0]
            
            # 今日新增
            result = db.execute(text("SELECT COUNT(*) as today FROM parents WHERE DATE(created_at) = CURDATE()"))
            stats['today_new'] = result.fetchone()[0]
            
            # 各状态统计
            result = db.execute(text("""
                SELECT status, COUNT(*) as count 
                FROM parents 
                GROUP BY status
            """))
            status_stats = {}
            for row in result.fetchall():
                status_stats[row[0]] = row[1]
            stats['status_stats'] = status_stats
            
            # 本月成交金额
            result = db.execute(text("""
                SELECT COALESCE(SUM(amount), 0) as total_amount 
                FROM process_logs 
                WHERE action_type = '成交' 
                AND MONTH(created_at) = MONTH(CURDATE())
                AND YEAR(created_at) = YEAR(CURDATE())
            """))
            stats['monthly_revenue'] = float(result.fetchone()[0])
            
            db.close()
            
            return MessageFormatter.format_success_response("统计信息获取成功", stats)
            
        except Exception as e:
            app_logger.error(f"获取统计信息失败: {e}")
            return MessageFormatter.format_error_response(f"获取统计信息失败: {str(e)}")
    
    def format_query_response(self, result: Dict[str, Any]) -> str:
        """格式化查询响应为文本"""
        if not result.get('success'):
            return f"❌ 查询失败：{result.get('error', '未知错误')}"
        
        data = result.get('data', [])
        total = result.get('total', 0)
        
        if total == 0:
            return "📊 查询结果：暂无数据"
        
        # 如果是统计查询（只有一行一列）
        if total == 1 and len(data[0]) == 1:
            key = list(data[0].keys())[0]
            value = data[0][key]
            return f"📊 查询结果：{value}"
        
        # 如果是简单的计数查询
        if total == 1 and 'count' in str(data[0]).lower():
            for key, value in data[0].items():
                if 'count' in key.lower() or key.lower() in ['total', 'num', 'cnt']:
                    return f"📊 查询结果：{value} 条记录"
        
        # 格式化多行数据
        response = f"📊 查询结果（共 {total} 条）：\n"
        
        # 限制显示条数
        display_limit = 10
        display_data = data[:display_limit]
        
        for i, row in enumerate(display_data, 1):
            response += f"\n{i}. "
            row_parts = []
            for key, value in row.items():
                if value is not None:
                    row_parts.append(f"{key}: {value}")
            response += " | ".join(row_parts)
        
        if total > display_limit:
            response += f"\n\n... 还有 {total - display_limit} 条记录未显示"
        
        return response