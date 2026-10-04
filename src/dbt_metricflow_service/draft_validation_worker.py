"""独立进程中的定义校验；只允许 deps/parse，不提供通用 dbt 命令入口。"""

import json
import sys
from pathlib import Path

import yaml

from .publication_template import validate_templates

UTF8 = "utf-8"
YAML = "YAML"
TEMPLATE = "TEMPLATE"
DBT_PARSE = "DBT_PARSE"
SEMANTIC = "SEMANTIC"
ERROR = "ERROR"
WARNING = "WARNING"
FIX_DEFINITION = "fix_definition"
SEMANTIC_MANIFEST = "target/semantic_manifest.json"
MODEL_PATHS = "model-paths"
PROJECT_FILE = "dbt_project.yml"
DEPS_FILES = ("packages.yml", "dependencies.yml")
SQL_DEFINITION = "SQL_DEFINITION"
COMPILED_MANIFEST = "target/manifest.json"
SQL_RESOURCES = frozenset({"model", "test"})
COMPILE_ARGS = ("compile", "--no-introspect", "--no-populate-cache", "--write-json")
V2_OPTION = "--branch-v2"


class UniqueKeyLoader(yaml.SafeLoader):
    """普通 safe_load 会静默覆盖重复键；校验必须拒绝歧义定义。"""

    def construct_mapping(self, node, deep=False):
        seen = set()
        for key, _ in node.value:
            name = self.construct_object(key, deep=deep)
            if name in seen:
                raise yaml.constructor.ConstructorError(None, None, "duplicate YAML key", key.start_mark)
            seen.add(name)
        return super().construct_mapping(node, deep=deep)


def diagnostic(code, message, *, path=None, line=None, column=None, resource_name=None, severity=ERROR):
    return {"code": code, "message": message, "path": path, "line": line, "column": column,
            "resourceName": resource_name, "severity": severity, "recovery": FIX_DEFINITION}


def result(levels, diagnostics):
    return {"valid": not any(item["severity"] == ERROR for item in diagnostics),
            "checkedLevels": levels, "phase": levels[-1], "diagnostics": diagnostics[:100]}


def validate_project(project: Path, profiles: Path, target: str, *, validate_sql: bool = False) -> dict:
    """dbt 自身负责解析，锁定版本语义验证器负责引用规则，禁止自造规则替代。"""
    from dbt.cli.main import dbtRunner
    from dbt.exceptions import CompilationError, EnvVarMissingError, ParsingError
    from metricflow_semantic_interfaces.validations.semantic_manifest_validator import SemanticManifestValidator
    from metricflow_semantics.model.dbt_manifest_parser import parse_manifest_from_dbt_generated_manifest

    levels, diagnostics = [YAML], []
    config = yaml.safe_load((project / PROJECT_FILE).read_text(UTF8))
    directories = config.get(MODEL_PATHS, ["models"])
    if validate_sql:
        directories = [*directories, "tests"]
    for directory in directories:
        for path in sorted((project / directory).rglob("*")):
            if path.suffix not in (".yml", ".yaml"):
                continue
            try:
                yaml.load(path.read_text(UTF8), Loader=UniqueKeyLoader)
            except yaml.YAMLError as error:
                mark = getattr(error, "problem_mark", None)
                diagnostics.append(diagnostic("invalid_yaml", "YAML 语法错误或存在重复键。",
                    path=path.relative_to(project).as_posix(), line=mark.line + 1 if mark else None,
                    column=mark.column + 1 if mark else None))
    if diagnostics:
        return result(levels, diagnostics)
    levels.append(TEMPLATE)
    try:
        validate_templates(project)
    except ValueError:
        return result(levels, [diagnostic("unsafe_template", "项目模板不符合受控表达式规则。")])
    common = ["--project-dir", str(project), "--profiles-dir", str(profiles), "--target", target,
              "--no-use-colors", "--no-send-anonymous-usage-stats"]
    if any((project / name).exists() for name in DEPS_FILES):
        deps = dbtRunner().invoke(["deps", *common])
        if not deps.success:
            raise RuntimeError("dependency preparation failed")
        try:
            validate_templates(project)
        except ValueError:
            return result(levels, [diagnostic("unsafe_template", "依赖模板不符合受控表达式规则。")])
    levels.append(DBT_PARSE)
    parsed = dbtRunner().invoke(["parse", "--no-partial-parse", "--write-json", *common])
    if not parsed.success:
        # 只有明确的定义异常可以返回 valid=false；profile、网络和未知异常由父进程判为 FAILED。
        if isinstance(parsed.exception, EnvVarMissingError):
            raise RuntimeError("dbt parse infrastructure failed")
        if isinstance(parsed.exception, (CompilationError, ParsingError)):
            node = getattr(parsed.exception, "node", None)
            path = getattr(node, "original_file_path", None)
            return result(levels, [diagnostic("invalid_dbt_definition", "dbt 解析失败，请检查定义及引用。",
                                              path=path, resource_name=getattr(node, "name", None))])
        raise RuntimeError("dbt parse infrastructure failed")
    if validate_sql:
        # 受控模板已排除执行 SQL 的宏；compile 关闭数据库 introspection 和 cache 填充。
        # 只验证模型及测试的单条只读 SQL，绝不调用 run/build/test。
        from .publication_build import validate_execution_policy, validate_readonly_sql

        levels.append(SQL_DEFINITION)
        # parse 产物先核实执行策略，明确不支持的配置不能获得 valid=true。
        parsed_document = json.loads((project / COMPILED_MANIFEST).read_text(UTF8))
        for node in parsed_document["nodes"].values():
            try:
                validate_execution_policy(node)
            except ValueError:
                diagnostics.append(diagnostic("unsupported_execution_policy", "定义包含不支持的执行策略。",
                                              path=node.get("original_file_path")))
        if diagnostics:
            return result(levels, diagnostics)
        compiled = dbtRunner().invoke([*COMPILE_ARGS, *common])
        if not compiled.success:
            if isinstance(compiled.exception, (CompilationError, ParsingError)):
                return result(levels, [diagnostic("invalid_model_sql", "SQL 定义编译失败，请检查引用与表达式。")])
            raise RuntimeError("dbt compile infrastructure failed")
        document = json.loads((project / COMPILED_MANIFEST).read_text(UTF8))
        dialect = document["metadata"]["adapter_type"]
        for node in document["nodes"].values():
            if node.get("resource_type") in SQL_RESOURCES:
                try:
                    validate_readonly_sql(node.get("compiled_code", ""), dialect)
                except ValueError:
                    diagnostics.append(diagnostic("invalid_model_sql", "模型和测试必须为单条受支持的只读查询。",
                                                  path=node.get("original_file_path")))
        if diagnostics:
            return result(levels, diagnostics)
    levels.append(SEMANTIC)
    manifest = parse_manifest_from_dbt_generated_manifest((project / SEMANTIC_MANIFEST).read_text(UTF8))
    validation = SemanticManifestValidator().validate_semantic_manifest(manifest, multi_process=False)
    for issue in validation.all_issues:
        context = getattr(issue, "context", None)
        file_context = getattr(context, "file_context", None)
        diagnostics.append(diagnostic("semantic_definition_invalid", issue.message[:2000],
            path=getattr(file_context, "file_name", None), line=getattr(file_context, "line_number", None),
            severity=ERROR if issue in validation.errors else WARNING))
    return result(levels, diagnostics)


def main():
    # 参数只由父 worker 生成；失败不输出异常或 profile 正文。
    project, profiles, target, output = sys.argv[1:5]
    try:
        value = validate_project(Path(project), Path(profiles), target, validate_sql=sys.argv[5:] == [V2_OPTION])
        Path(output).write_text(json.dumps(value, ensure_ascii=False), encoding=UTF8)
    except Exception:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
