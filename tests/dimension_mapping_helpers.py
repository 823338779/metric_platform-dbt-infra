"""A real dbt project for display mapping integration tests."""

from pathlib import Path

import yaml


def write_mapping_project(project: Path) -> None:
    models = project / "models"
    models.mkdir()
    (project / "dbt_project.yml").write_text(
        "name: mapping_test\nversion: '1.0'\nconfig-version: 2\nprofile: mapping_test\n",
        encoding="utf-8",
    )
    (project / "profiles.yml").write_text(
        yaml.safe_dump(
            {
                "mapping_test": {
                    "target": "dev",
                    "outputs": {
                        "dev": {
                            "type": "duckdb",
                            "path": str(project / "mapping.duckdb"),
                            "threads": 1,
                        }
                    },
                }
            }
        ),
        encoding="utf-8",
    )
    (models / "requests.sql").write_text(
        """
select * from (values
  (1,200,404,1,date '2026-01-01',100),
  (2,404,502,1,date '2026-01-01',20),
  (3,502,200,2,date '2026-01-01',10),
  (4,599,200,2,date '2026-01-01',4)
) t(id,status,target_status,channel,ds,amount)
""",
        encoding="utf-8",
    )
    (models / "status_codes.sql").write_text(
        "select * from (values (200,'成功'),(404,'失败'),(502,'失败')) t(code,label)", encoding="utf-8"
    )
    (models / "channels.sql").write_text(
        "select * from (values (1,'web'),(2,'mobile')) t(code,label)", encoding="utf-8"
    )
    (models / "time_spine.sql").write_text("select date '2026-01-01' as ds", encoding="utf-8")
    mapping = {"semantic_model": "status_codes", "value_dimension": "raw_value", "display_dimension": "display_value"}
    schema = {
        "version": 2,
        "models": [
            {
                "name": "time_spine",
                "time_spine": {"standard_granularity_column": "ds"},
                "columns": [{"name": "ds", "granularity": "day"}],
            }
        ],
        "semantic_models": [
            {
                "name": "requests",
                "model": "ref('requests')",
                "defaults": {"agg_time_dimension": "ds"},
                "entities": [{"name": "request", "type": "primary", "expr": "id"}],
                "dimensions": [
                    {"name": "status", "type": "categorical", "value_mapping": mapping},
                    {"name": "target_status", "type": "categorical", "value_mapping": mapping},
                    {
                        "name": "channel",
                        "type": "categorical",
                        "value_mapping": {**mapping, "semantic_model": "channels"},
                    },
                    {"name": "ds", "type": "time", "type_params": {"time_granularity": "day"}},
                ],
                "measures": [{"name": "requests_count", "agg": "sum", "expr": "amount", "create_metric": True}],
            },
            *[
                {
                    "name": name,
                    "model": "ref('" + name + "')",
                    "entities": [{"name": name + "_key", "type": "primary", "expr": "code"}],
                    "dimensions": [
                        {"name": "raw_value", "type": "categorical", "expr": "code"},
                        {"name": "display_value", "type": "categorical", "expr": "label"},
                    ],
                }
                for name in ("status_codes", "channels")
            ],
        ],
        "saved_queries": [
            {
                "name": "status_summary",
                "query_params": {
                    "metrics": ["requests_count"],
                    "group_by": ["Dimension('request__status')"],
                },
            }
        ],
    }
    (models / "schema.yml").write_text(yaml.safe_dump(schema, allow_unicode=True), encoding="utf-8")
