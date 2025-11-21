"""
M-Schema 助手：使用项目现有的 SQLAlchemy 引擎构建并缓存 M-Schema 字符串。
当无法导入 M-Schema 时，优雅降级为提示文本。
"""
import os
import sys
from typing import Optional, List, Dict, Any
import importlib.util

from config import config
from models.database import engine
from sqlalchemy import inspect
from sqlalchemy import text as sa_text

# 将 M-Schema 目录加入 sys.path，便于直接 import 其内模块
MSCHEMA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "M-Schema")
if os.path.isdir(MSCHEMA_DIR) and MSCHEMA_DIR not in sys.path:
    sys.path.append(MSCHEMA_DIR)

try:
    from schema_engine import SchemaEngine  # 位于 M-Schema/schema_engine.py
except Exception:
    SchemaEngine = None
MSchema = None  # 运行时按需安全导入（避免与项目 utils 包冲突）

_cached_mschema_str: Optional[str] = None


def build_schema_engine() -> "SchemaEngine":
    """构建 SchemaEngine；要求 M-Schema 可导入。"""
    if SchemaEngine is None:
        raise RuntimeError("M-Schema 未安装或无法导入 (schema_engine)。")
    db_name = getattr(config, "MYSQL_DB", "")
    # 不显式传 schema，SchemaEngine 会基于方言与 db_name 做合理过滤
    return SchemaEngine(engine=engine, db_name=db_name)


def _ensure_mschema_class():
    """安全导入 M-Schema 的 MSchema 类，避免与项目自身 utils 包冲突。
    具体做法：临时将 sys.modules['utils'] 指向 M-Schema/utils.py，加载完成后还原。
    """
    global MSchema
    if MSchema is not None:
        return MSchema
    utils_path = os.path.join(MSCHEMA_DIR, "utils.py")
    m_schema_path = os.path.join(MSCHEMA_DIR, "m_schema.py")
    if not (os.path.isfile(utils_path) and os.path.isfile(m_schema_path)):
        raise RuntimeError("本地回退不可用：缺少 M-Schema 源文件。")

    original_utils = sys.modules.get('utils')
    inserted_utils = False
    mschema_utils_module = None
    try:
        # 加载 M-Schema 的 utils.py
        spec_utils = importlib.util.spec_from_file_location("mschema_utils", utils_path)
        mschema_utils_module = importlib.util.module_from_spec(spec_utils)
        assert spec_utils and spec_utils.loader, "无法创建 M-Schema utils 规格"
        spec_utils.loader.exec_module(mschema_utils_module)
        sys.modules['utils'] = mschema_utils_module
        inserted_utils = True

        # 加载 m_schema.py 并获取 MSchema 类
        spec_mschema = importlib.util.spec_from_file_location("mschema_m_schema", m_schema_path)
        mschema_module = importlib.util.module_from_spec(spec_mschema)
        assert spec_mschema and spec_mschema.loader, "无法创建 m_schema 规格"
        spec_mschema.loader.exec_module(mschema_module)
        MSchema = getattr(mschema_module, 'MSchema')
        return MSchema
    finally:
        # 还原 sys.modules['utils']，避免污染项目命名空间
        if original_utils is not None:
            sys.modules['utils'] = original_utils
        else:
            if inserted_utils and 'utils' in sys.modules:
                del sys.modules['utils']


def get_mschema_string(selected_tables: Optional[List[str]] = None,
                       selected_columns: Optional[List[str]] = None,
                       example_num: int = 3,
                       show_type_detail: bool = False) -> str:
    """
    获取并缓存 M-Schema 字符串；可选按表/列过滤。
    失败时返回告警前缀文本，以便在 Prompt 中呈现上下文。
    """
    global _cached_mschema_str
    if _cached_mschema_str:
        return _cached_mschema_str
    try:
        # 优先尝试官方 SchemaEngine
        schema_engine = build_schema_engine()
        mschema = schema_engine.mschema
        mschema_str = mschema.to_mschema(
            selected_tables=selected_tables,
            selected_columns=selected_columns,
            example_num=example_num,
            show_type_detail=show_type_detail,
        )
        _cached_mschema_str = mschema_str
        return mschema_str
    except Exception as e:
        # 回退：使用 SQLAlchemy Inspector 与 MSchema 本地构建，避免 llama_index 依赖
        try:
            mschema_str = _build_mschema_string_fallback(
                selected_tables=selected_tables,
                selected_columns=selected_columns,
                example_num=example_num,
                show_type_detail=show_type_detail,
            )
            _cached_mschema_str = mschema_str
            return mschema_str
        except Exception as e2:
            # 最终降级：返回失败提示，在提示词中照常嵌入
            return f"【M-Schema加载失败】{e2}"


def get_db_schema(selected_tables: Optional[List[str]] = None,
                  selected_columns: Optional[List[str]] = None,
                  example_num: int = 3,
                  show_type_detail: bool = False) -> str:
    """
    获取 db_schema 字符串，等价于 example.py 中的 mschema_str。
    统一入口，内部复用 get_mschema_string 保持缓存与回退机制。
    """
    return get_mschema_string(
        selected_tables=selected_tables,
        selected_columns=selected_columns,
        example_num=example_num,
        show_type_detail=show_type_detail,
    )


def _build_mschema_string_fallback(selected_tables: Optional[List[str]] = None,
                                   selected_columns: Optional[List[str]] = None,
                                   example_num: int = 3,
                                   show_type_detail: bool = False) -> str:
    """
    当无法导入官方 SchemaEngine 时，使用 SQLAlchemy Inspector 构建简化版 M-Schema 字符串。
    不依赖 llama_index，仅依赖本地 MSchema 类。示例数据采样不保证，尽量提供结构与外键信息。
    """
    MSchema_cls = _ensure_mschema_class()

    insp = inspect(engine)
    dialect = engine.dialect.name
    db_name = getattr(config, "MYSQL_DB", "")
    schema = db_name if dialect == 'mysql' else None

    # 收集表名
    try:
        table_names = insp.get_table_names(schema=schema)
    except Exception:
        table_names = insp.get_table_names()

    mschema = MSchema_cls(db_id=db_name or 'Anonymous', schema=schema)

    # 构建表与字段
    for table_name in table_names:
        # 表注释
        try:
            comment_info = insp.get_table_comment(table_name, schema=schema)
            table_comment = comment_info.get('text') if isinstance(comment_info, dict) else ""
        except Exception:
            table_comment = ""
        mschema.add_table(table_name, fields={}, comment=table_comment)

        # 主键
        try:
            pk_info = insp.get_pk_constraint(table_name, schema=schema)
            pks = pk_info.get('constrained_columns', []) if isinstance(pk_info, dict) else []
        except Exception:
            pks = []

        # 字段
        try:
            cols = insp.get_columns(table_name, schema=schema)
        except Exception:
            cols = []
        for col in cols:
            field_name = col.get('name')
            field_type = f"{col.get('type')}"
            nullable = bool(col.get('nullable', True))
            default = col.get('default')
            autoincrement = bool(col.get('autoincrement', False))
            comment = col.get('comment') or ""
            examples = []  # 回退模式不保证采样示例
            mschema.add_field(
                table_name,
                field_name,
                field_type=field_type,
                primary_key=(field_name in pks),
                nullable=nullable,
                default=default,
                autoincrement=autoincrement,
                comment=comment,
                examples=examples,
            )

        # 外键
        try:
            fks = insp.get_foreign_keys(table_name, schema=schema)
        except Exception:
            fks = []
        for fk in fks:
            constrained = fk.get('constrained_columns') or []
            referred_schema = fk.get('referred_schema') or schema or ''
            referred_table = fk.get('referred_table') or ''
            referred_cols = fk.get('referred_columns') or []
            for c, r in zip(constrained, referred_cols):
                mschema.add_foreign_key(table_name, c, referred_schema, referred_table, r)

    return mschema.to_mschema(
        selected_tables=selected_tables,
        selected_columns=selected_columns,
        example_num=example_num,
        show_type_detail=show_type_detail,
    )