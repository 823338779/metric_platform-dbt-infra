"""发布项目仅支持无数据库副作用的 Jinja 表达式宏。"""

from pathlib import Path
from typing import Final

import yaml
from jinja2 import Environment, TemplateSyntaxError, nodes

from dbt_metricflow_service import constants as strings

from ..models.native import NativeProject

PYTHON_PROJECT_UNSUPPORTED_MESSAGE: Final[str] = '发布暂不支持 Python 项目代码'
TEMPLATE_EXTENSION_UNSUPPORTED_MESSAGE: Final[str] = '发布模板包含不支持的扩展语法'
MACRO_NAME_INVALID_MESSAGE: Final[str] = '发布表达式宏名称不符合受控规则'
MACRO_ARGUMENT_RESERVED_MESSAGE: Final[str] = '表达式宏参数不能覆盖内置函数'
TEMPLATE_EXPRESSION_UNSUPPORTED_MESSAGE: Final[str] = '发布模板包含不支持的动态表达式'
TEMPLATE_INTERNAL_ACCESS_MESSAGE: Final[str] = '发布模板不能访问内部属性'
TEMPLATE_CONTEXT_UNSUPPORTED_MESSAGE: Final[str] = '发布模板包含不受支持的上下文变量'
TEMPLATE_DATABASE_CALL_FORBIDDEN_MESSAGE: Final[str] = '发布模板禁止数据库命令或动态函数调用'

UTF8 = strings.UTF8
SUFFIXES = frozenset({strings.SQL_EXTENSION, strings.YML_EXTENSION, strings.YAML_EXTENSION})
IGNORED = frozenset({strings.TARGET_DIRECTORY, strings.LOGS_DIRECTORY, strings.GIT_DIRECTORY})
BUILTINS = frozenset(
    {
        strings.JINJA_REF_FUNCTION,
        strings.JINJA_SOURCE_FUNCTION,
        strings.JINJA_CONFIG_FUNCTION,
        strings.JINJA_VAR_FUNCTION,
        strings.JINJA_ENV_VAR_FUNCTION,
        strings.JINJA_RETURN_FUNCTION,
    }
)
RESERVED = BUILTINS | frozenset(
    {
        strings.JINJA_RUN_QUERY_FUNCTION,
        strings.JINJA_STATEMENT_FUNCTION,
        strings.JINJA_ADAPTER_VARIABLE,
        strings.JINJA_EXECUTE_VARIABLE,
        strings.JINJA_LOAD_RESULT_FUNCTION,
        strings.JINJA_LOG_FUNCTION,
    }
)
PROJECT_FILE = strings.DBT_PROJECT_FILE
TARGET = strings.JINJA_TARGET_VARIABLE
PYTHON_SUFFIX = strings.PYTHON_EXTENSION
ALLOWED = (
    nodes.Template,
    nodes.Output,
    nodes.TemplateData,
    nodes.Call,
    nodes.Name,
    nodes.Const,
    nodes.Getattr,
    nodes.Keyword,
    nodes.Macro,
    nodes.If,
    nodes.Compare,
    nodes.Operand,
    nodes.Add,
    nodes.Sub,
    nodes.Mul,
    nodes.Div,
    nodes.And,
    nodes.Or,
    nodes.Not,
    nodes.List,
    nodes.Tuple,
    nodes.Dict,
    nodes.Pair,
    nodes.Neg,
    nodes.Pos,
    nodes.Concat,
)


def validate_templates(project: Path) -> None:
    """在 dbt 解析之前检查所有项目与依赖模板。"""
    templates, macros, namespaces, arguments = [], set(), set(), set()
    environment = Environment()
    for path in project.rglob("*"):
        if not path.is_file() or IGNORED.intersection(path.relative_to(project).parts):
            continue
        if path.suffix == PYTHON_SUFFIX:
            raise ValueError(PYTHON_PROJECT_UNSUPPORTED_MESSAGE)
        if path.suffix not in SUFFIXES:
            continue
        text = path.read_text(encoding=UTF8)
        if path.name == PROJECT_FILE:
            name = NativeProject.model_validate(yaml.safe_load(text) or {}).name
            if isinstance(name, str):
                namespaces.add(name)
        try:
            tree = environment.parse(text)
        except TemplateSyntaxError as error:
            raise ValueError(TEMPLATE_EXTENSION_UNSUPPORTED_MESSAGE) from error
        templates.append(tree)
        for macro in tree.find_all(nodes.Macro):
            # 首版限制表达式宏命名，防止覆盖 adapter/dbt 内置物化与 DDL 宏。
            if not macro.name.isalnum() or macro.name in RESERVED:
                raise ValueError(MACRO_NAME_INVALID_MESSAGE)
            if any(argument.name in RESERVED for argument in macro.args):
                raise ValueError(MACRO_ARGUMENT_RESERVED_MESSAGE)
            arguments.update(argument.name for argument in macro.args)
            macros.add(macro.name)
    for tree in templates:
        for node in tree.find_all(nodes.Node):
            if not isinstance(node, ALLOWED):
                raise ValueError(TEMPLATE_EXPRESSION_UNSUPPORTED_MESSAGE)
            if isinstance(node, nodes.Getattr) and node.attr.startswith("_"):
                raise ValueError(TEMPLATE_INTERNAL_ACCESS_MESSAGE)
            if isinstance(node, nodes.Name) and node.name not in BUILTINS | macros | namespaces | arguments | {TARGET}:
                raise ValueError(TEMPLATE_CONTEXT_UNSUPPORTED_MESSAGE)
            if not isinstance(node, nodes.Call):
                continue
            target = node.node
            direct = isinstance(target, nodes.Name) and target.name in BUILTINS | macros
            qualified = (
                isinstance(target, nodes.Getattr)
                and isinstance(target.node, nodes.Name)
                and target.node.name in namespaces
                and target.attr in macros
            )
            if not (direct or qualified) or node.dyn_args or node.dyn_kwargs:
                raise ValueError(TEMPLATE_DATABASE_CALL_FORBIDDEN_MESSAGE)
