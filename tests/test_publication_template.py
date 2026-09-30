"""受控模板在 dbt 加载项目之前拒绝可能直接执行数据库命令的代码。"""

import pytest

from dbt_metricflow_service.publication_template import validate_templates


@pytest.mark.parametrize("code", [
    "{{ run_query('drop table old') }}",
    "{{ adapter.execute('drop table old') }}",
    "{% materialization unsafe, default %}select 1{% endmaterialization %}",
    "{% macro get_create_table_as_sql() %}drop table old{% endmacro %}",
    "{% set execute_sql = run_query %}{{ execute_sql('drop table old') }}",
])
def test_unsafe_template_is_rejected_before_dbt(tmp_path, code):
    (tmp_path / "unsafe.sql").write_text(code, encoding="utf-8")
    with pytest.raises(ValueError):
        validate_templates(tmp_path)


def test_pure_expression_macro_remains_supported(tmp_path):
    (tmp_path / "amount.sql").write_text("{% macro amount(value) %}{{ value }}::numeric{% endmacro %}")
    (tmp_path / "orders.sql").write_text("select {{ amount('10.25') }} as revenue")
    validate_templates(tmp_path)
