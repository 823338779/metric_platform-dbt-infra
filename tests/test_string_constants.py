"""Shared runtime strings stay named; annotations and language metadata stay readable."""

import ast
from pathlib import Path

from dbt_metricflow_service import constants

SOURCE = Path(__file__).resolve().parents[1] / "src" / "dbt_metricflow_service"
CONSTANTS = SOURCE / "constants.py"


def test_shared_constant_references_are_defined():
    errors = []
    for path in sorted(SOURCE.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8-sig"))
        aliases = {
            item.asname or item.name
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module == "dbt_metricflow_service"
            for item in node.names if item.name == "constants"
        }
        for node in ast.walk(tree):
            if (isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name)
                    and node.value.id in aliases and not hasattr(constants, node.attr)):
                errors.append(f"{path.relative_to(SOURCE)}:{node.lineno}: {node.attr}")
    assert not errors, "Undefined shared constants:\n" + "\n".join(errors)


def _constant_assignment(node):
    targets = node.targets if isinstance(node, ast.Assign) else [node.target] if isinstance(node, ast.AnnAssign) else []
    return bool(targets) and all(isinstance(target, ast.Name) and target.id.isupper() for target in targets)


def _call_name(node, imports):
    if isinstance(node, ast.Name):
        return imports.get(node.id, node.id).rsplit(".", 1)[-1]
    if isinstance(node, ast.Attribute):
        return node.attr
    return ""


def _runtime_literals(source, *, allow_named_constants):
    tree = ast.parse(source)
    parents = {child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}
    imports = {
        item.asname or item.name: f"{node.module}.{item.name}"
        for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
        for item in node.names
    }
    excluded = set()

    def exclude(node):
        excluded.update(ast.walk(node))

    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.body and isinstance(node.body[0], ast.Expr):
                value = node.body[0].value
                if isinstance(value, ast.Constant) and isinstance(value.value, str):
                    exclude(value)
        if isinstance(node, ast.arg) and node.annotation is not None:
            exclude(node.annotation)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.returns is not None:
            exclude(node.returns)
        elif isinstance(node, ast.AnnAssign):
            exclude(node.annotation)
            if _call_name(node.annotation, imports) == "TypeAlias" and node.value is not None:
                exclude(node.value)
        if allow_named_constants and _constant_assignment(node):
            scope = parents.get(node)
            while scope is not None and not isinstance(
                scope, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)
            ):
                scope = parents.get(scope)
            if isinstance(scope, (ast.Module, ast.ClassDef)) and node.value is not None:
                exclude(node.value)
        if isinstance(node, ast.JoinedStr):
            # Only formatting text is exempt; expressions inside braces remain checked.
            excluded.update(value for value in node.values if isinstance(value, ast.Constant))
        if isinstance(node, ast.Call):
            name = _call_name(node.func, imports)
            for keyword in node.keywords:
                if (name == "Field" and keyword.arg == "description"
                        or name in {"mapped_column", "Column", "Table"} and keyword.arg == "comment"):
                    exclude(keyword.value)
            if node.args and name in {"TypeVar", "ParamSpec", "TypeVarTuple", "text", "SQL", "exec_driver_sql"}:
                # Named type variables and SQL statement bodies are protocol/language definitions.
                exclude(node.args[0])
    return [
        (node.lineno, node.value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
        and node not in excluded
    ]


def test_shared_runtime_strings_are_not_repeated_as_literals():
    tree = ast.parse(CONSTANTS.read_text(encoding="utf-8-sig"))
    shared_values = {
        value.value
        for node in tree.body if _constant_assignment(node) and node.value is not None
        for value in [node.value]
        if isinstance(value, ast.Constant) and isinstance(value.value, str)
    }
    assert shared_values, "Shared string constants must be present before enforcing references"
    errors = []
    for path in sorted(SOURCE.rglob("*.py")):
        if path == CONSTANTS:
            continue
        for line, value in _runtime_literals(path.read_text(encoding="utf-8-sig"), allow_named_constants=False):
            if value in shared_values:
                errors.append(f"{path.relative_to(SOURCE)}:{line}: {value!r}")
    assert not errors, "Use shared string constants for runtime values:\n" + "\n".join(errors)


def test_runtime_semantic_strings_have_named_constants():
    errors = []
    for path in sorted(SOURCE.rglob("*.py")):
        if path == CONSTANTS:
            continue
        for line, value in _runtime_literals(path.read_text(encoding="utf-8-sig"), allow_named_constants=True):
            if any(character.isalnum() for character in value):
                errors.append(f"{path.relative_to(SOURCE)}:{line}: {value!r}")
    assert not errors, "Name runtime semantic strings before use:\n" + "\n".join(errors)


def test_string_guard_checks_nested_runtime_values_and_formatted_expressions():
    source = '\n'.join([
        'def execute():',
        '    state = "ACTIVE"',
        '    payload = {"state": ["ACTIVE"]}',
        '    LOCAL = "ACTIVE"',
        '    return f"state={str(\'ACTIVE\')}"',
    ])
    literals = _runtime_literals(source, allow_named_constants=True)
    assert [value for _, value in literals if value == "ACTIVE"] == ["ACTIVE"] * 4
    assert "state" in [value for _, value in literals]


def test_string_guard_ignores_definitions_metadata_and_literal_annotations():
    source = '\n'.join([
        '"ACTIVE"',
        'from typing import TypeVar as Variable, Literal, TypeAlias',
        'from pydantic import Field as Property',
        'from sqlalchemy import text as statement',
        'ACTIVE_STATES = {"ACTIVE": ["ACTIVE"]}',
        'PATTERN = re.compile("ACTIVE")',
        'class Record:',
        '    "ACTIVE"',
        '    KIND = "ACTIVE"',
        '    state: Literal["ACTIVE"] = Property(description="ACTIVE")',
        '    column = mapped_column(comment="ACTIVE")',
        'Alias: TypeAlias = Literal["ACTIVE"]',
        'T = Variable("ACTIVE")',
        'def query(value: "ACTIVE") -> "ACTIVE":',
        '    "ACTIVE"',
        '    statement("ACTIVE")',
        '    connection.exec_driver_sql("ACTIVE")',
        '    sql.SQL("ACTIVE")',
        '    return f"ACTIVE {value}"',
    ])
    assert not _runtime_literals(source, allow_named_constants=True)
    assert [value for _, value in _runtime_literals(source, allow_named_constants=False)] == ["ACTIVE"] * 4


def test_string_guard_allows_shared_references_and_ignores_punctuation():
    source = '\n'.join([
        'LOCAL_ACTIVE = strings.ACTIVE',
        'class Record:',
        '    KIND = strings.ACTIVE',
        'def execute():',
        '    return strings.ACTIVE + " | " + ""',
    ])
    assert not [value for _, value in _runtime_literals(source, allow_named_constants=False) if value == "ACTIVE"]
    assert not [value for _, value in _runtime_literals(source, allow_named_constants=True)
                if any(character.isalnum() for character in value)]
    bare = 'def execute():\n    return ["new_state", "42", "新消息"]'
    assert {value for _, value in _runtime_literals(bare, allow_named_constants=True)} == {
        "new_state", "42", "新消息",
    }
