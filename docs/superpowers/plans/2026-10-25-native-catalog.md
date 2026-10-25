# 直接读取原生 Manifest 目录

用户要求：去掉统一资源模型，直接返回 dbt 原生指标、维度与依赖。

实现与契约：
- 继续按固定 buildId 读取产物存储中的 Manifest；压缩解压、摘要校验、历史产物保留规则不变。
- catalog 返回 metadata、metrics、semantic_models、nodes、sources、parent_map、child_map 原生片段。
- 维度保持 semantic_models 内嵌结构，不制造维度 ID；不补推指标依赖或折叠 ephemeral 节点。
- 节点详情及 native-details 直接返回原始 JSON，lineage 返回该节点原生 parent_map/child_map 条目。
- 删除 ResourceCatalog / PublishedResource 和各类统一 attributes/capabilities 模型、转换函数与分页包装。
- 查询通过原生指标 ID 获取名称；预览通过原生节点类型判断。MetricFlow 维度选项继续按构建与指标集合隔离，使用原生 name、semanticModelName 说明来源，不再反查统一资源 ID。
- 对外 URL 保留，返回格式变更记录在 README；调用方需要更新原 items/attributes 读取逻辑。

验证：
1. 先加入原生片段和详情精确相等、依赖不推导测试，确认旧实现失败。
2. 修改实现并更新历史读取、查询映射、OpenAPI 示例；针对测试 42 项通过。
3. 独立静态审查无阻断问题；完整数据库回归和真实构建查询集成结果见后续记录。

最终验证：
- 全量非 integration 测试：526 passed、2 skipped、3 failed。两项是已知环境问题：vendor/dbt 当前提交与审计固定值不同、核心包为 editable 安装；另一项为新测试误写未知视图 HTTP 400，已改为既有契约 422。
- 修正后重跑目录/API、历史查询、查询计划、内存 Manifest 与真实 PostgreSQL 构建集成：52 passed。真实集成覆盖 QUERY、EXPLAIN、PREVIEW、DIMENSION_VALUES 和构建替换后的历史读取。
- Ruff 与 git diff --check 通过；独立审查未发现阻断问题。
