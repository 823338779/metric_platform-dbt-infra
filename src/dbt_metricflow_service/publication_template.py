"""发布项目仅支持无数据库副作用的 Jinja 表达式宏。"""

from pathlib import Path

import yaml
from jinja2 import Environment, TemplateSyntaxError, nodes

UTF8 = "utf-8"
SUFFIXES = frozenset({".sql", ".yml", ".yaml"})
IGNORED = frozenset({"target", "logs", ".git"})
BUILTINS = frozenset({"ref", "source", "config", "var", "env_var", "return"})
RESERVED = BUILTINS | frozenset({"run_query", "statement", "adapter", "execute", "load_result", "log"})
PROJECT_FILE = "dbt_project.yml"
NAME = "name"
TARGET = "target"
PYTHON_SUFFIX = ".py"
ALLOWED = (nodes.Template, nodes.Output, nodes.TemplateData, nodes.Call, nodes.Name, nodes.Const,
           nodes.Getattr, nodes.Keyword, nodes.Macro, nodes.If, nodes.Compare, nodes.Operand,
           nodes.Add, nodes.Sub, nodes.Mul, nodes.Div, nodes.And, nodes.Or, nodes.Not,
           nodes.List, nodes.Tuple, nodes.Dict, nodes.Pair, nodes.Neg, nodes.Pos, nodes.Concat)


def validate_templates(project: Path) -> None:
    """在 dbt 解析之前检查所有项目与依赖模板。"""
    templates, macros, namespaces, arguments = [], set(), set(), set()
    environment = Environment()
    for path in project.rglob("*"):
        if not path.is_file() or IGNORED.intersection(path.relative_to(project).parts):
            continue
        if path.suffix == PYTHON_SUFFIX:
            raise ValueError("发布暂不支持 Python 项目代码")
        if path.suffix not in SUFFIXES:
            continue
        text = path.read_text(encoding=UTF8)
        if path.name == PROJECT_FILE:
            name = (yaml.safe_load(text) or {}).get(NAME)
            if isinstance(name, str):
                namespaces.add(name)
        try:
            tree = environment.parse(text)
        except TemplateSyntaxError as error:
            raise ValueError("发布模板包含不支持的扩展语法") from error
        templates.append(tree)
        for macro in tree.find_all(nodes.Macro):
            # 首版限制表达式宏命名，防止覆盖 adapter/dbt 内置物化与 DDL 宏。
            if not macro.name.isalnum() or macro.name in RESERVED:
                raise ValueError("发布表达式宏名称不符合受控规则")
            if any(argument.name in RESERVED for argument in macro.args):
                raise ValueError("表达式宏参数不能覆盖内置函数")
            arguments.update(argument.name for argument in macro.args)
            macros.add(macro.name)
    for tree in templates:
        for node in tree.find_all(nodes.Node):
            if not isinstance(node, ALLOWED):
                raise ValueError("发布模板包含不支持的动态表达式")
            if isinstance(node, nodes.Getattr) and node.attr.startswith("_"):
                raise ValueError("发布模板不能访问内部属性")
            if isinstance(node, nodes.Name) and node.name not in BUILTINS | macros | namespaces | arguments | {TARGET}:
                raise ValueError("发布模板包含不受支持的上下文变量")
            if not isinstance(node, nodes.Call):
                continue
            target = node.node
            direct = isinstance(target, nodes.Name) and target.name in BUILTINS | macros
            qualified = (isinstance(target, nodes.Getattr) and isinstance(target.node, nodes.Name)
                         and target.node.name in namespaces and target.attr in macros)
            if not (direct or qualified) or node.dyn_args or node.dyn_kwargs:
                raise ValueError("发布模板禁止数据库命令或动态函数调用")
