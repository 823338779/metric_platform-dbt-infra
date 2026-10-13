"""Keep service annotations explicit without importing vendor runtime modules."""

import ast
from pathlib import Path

SOURCE = Path(__file__).resolve().parents[1] / "src" / "dbt_metricflow_service"
CONTAINERS = {
    "dict", "list", "tuple", "set", "frozenset", "type",
    "Dict", "List", "Tuple", "Set", "FrozenSet", "Type", "Mapping", "MutableMapping",
    "Sequence", "MutableSequence", "Iterable", "Iterator", "Collection", "Callable",
    "Awaitable", "Coroutine", "Generator", "AsyncIterable", "AsyncIterator", "AsyncGenerator",
}


def _imports(tree):
    aliases = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for item in node.names:
                aliases[item.asname or item.name] = f"{node.module}.{item.name}"
        elif isinstance(node, ast.Import):
            for item in node.names:
                aliases[item.asname or item.name] = item.name
    return aliases


def _name(node, aliases):
    if isinstance(node, ast.Name):
        return aliases.get(node.id, node.id)
    if isinstance(node, ast.Attribute):
        return f"{_name(node.value, aliases)}.{node.attr}"
    return ""


def _bare_annotations(node, aliases):
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        # Forward annotations use the same rules; Literal values are skipped below.
        yield from _bare_annotations(ast.parse(node.value, mode="eval").body, aliases)
    elif isinstance(node, ast.Subscript):
        kind = _name(node.value, aliases).rsplit(".", 1)[-1]
        if kind == "Literal":
            return
        if kind == "Annotated" and isinstance(node.slice, ast.Tuple):
            yield from _bare_annotations(node.slice.elts[0], aliases)
        else:
            yield from _bare_annotations(node.slice, aliases)
    elif isinstance(node, (ast.Name, ast.Attribute)):
        name = _name(node, aliases)
        package, _, short = name.rpartition(".")
        if short in CONTAINERS and package in {"", "typing", "typing_extensions", "collections.abc"}:
            yield name
    else:
        for child in ast.iter_child_nodes(node):
            yield from _bare_annotations(child, aliases)


def _any_annotations(node, aliases):
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return _any_annotations(ast.parse(node.value, mode="eval").body, aliases)
    if isinstance(node, ast.Subscript):
        kind = _name(node.value, aliases).rsplit(".", 1)[-1]
        if kind == "Literal":
            return False
        if kind == "Annotated" and isinstance(node.slice, ast.Tuple):
            return _any_annotations(node.slice.elts[0], aliases)
    if _name(node, aliases) in {"Any", "typing.Any", "typing_extensions.Any"}:
        return True
    return any(_any_annotations(child, aliases) for child in ast.iter_child_nodes(node))


def _annotations(tree, aliases):
    for node in ast.walk(tree):
        if isinstance(node, ast.arg) and node.annotation is not None:
            yield node.annotation
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.returns is not None:
            yield node.returns
        elif isinstance(node, ast.AnnAssign):
            yield node.annotation
            if _name(node.annotation, aliases).rsplit(".", 1)[-1] == "TypeAlias" and node.value is not None:
                yield node.value


def test_service_does_not_import_or_reference_any():
    errors = []
    for path in sorted(SOURCE.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8-sig"))
        aliases = _imports(tree)
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module in {"typing", "typing_extensions"}:
                if any(item.name == "Any" for item in node.names):
                    errors.append(f"{path.relative_to(SOURCE)}:{node.lineno}: imports Any")
            elif isinstance(node, (ast.Name, ast.Attribute)):
                if _name(node, aliases) in {"Any", "typing.Any", "typing_extensions.Any"}:
                    errors.append(f"{path.relative_to(SOURCE)}:{node.lineno}: references Any")
        for annotation in _annotations(tree, aliases):
            if _any_annotations(annotation, aliases):
                errors.append(f"{path.relative_to(SOURCE)}:{annotation.lineno}: Any annotation")
    assert not errors, "Explicit service types required:\n" + "\n".join(errors)


def test_service_container_annotations_declare_element_types():
    errors = []
    for path in sorted(SOURCE.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8-sig"))
        aliases = _imports(tree)
        for annotation in _annotations(tree, aliases):
            for name in _bare_annotations(annotation, aliases):
                errors.append(f"{path.relative_to(SOURCE)}:{annotation.lineno}: bare {name}")
    assert not errors, "Container element types required:\n" + "\n".join(errors)


def test_annotation_scan_distinguishes_forward_types_from_literal_values():
    aliases = {"t": "typing", "Opaque": "typing.Any"}
    assert _any_annotations(ast.parse('list["t.Any"]', mode="eval").body, aliases)
    assert _any_annotations(ast.parse('dict[str, Opaque] | None', mode="eval").body, aliases)
    assert not _any_annotations(ast.parse('t.Literal["Any", "dict"]', mode="eval").body, aliases)
    assert list(_bare_annotations(ast.parse('list["dict"]', mode="eval").body, aliases)) == ["dict"]
    assert not list(_bare_annotations(ast.parse('t.Literal["dict"]', mode="eval").body, aliases))
