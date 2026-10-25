# Manifest 目录简化实施计划

按用户已选择的单 Manifest 方案在当前工作区执行，保留先前修改。

1. 为 Manifest 直读、声明列、血缘和不生成目录文件补充失败测试。
2. 合并 platform/catalog.py 和 sealed_catalog.py 的必要映射，返回仅含资源与血缘的 ResourceCatalog；删除仅用于落盘的中间模型。
3. CatalogService 改读 Manifest，构建仅验证目录，预览直接定位 Manifest 节点；移除产物白名单/完成校验中的目录文件及数据库映射中的旧摘要字段。
4. 更新测试夹具与文档；运行目录、查询、构建、存储及真实 SDK 回归，检查 Ruff 和 diff。

Review focus: 历史 gzip Manifest 与旧数据库兼容；同名维度保持模型归属；派生指标血缘和 ephemeral 链；表列声明缺失；成功构建无需 published_catalog.json 且原有物理验证仍生效。

## 完成与验证

已完成四步。新目录读取只依赖 Manifest；删除 sealed_catalog.py、NativeCatalog/NativeResource、RelationBinding 和独立目录文件；表预览直接读取 Manifest 的物理节点。旧数据库中残留的 catalog_digest 不再参与当前代码读写。

先确认 10 个新测试失败，再实现。全量非 integration 回归首次结果：578 passed、2 skipped、4 failed；其中两个新字符串规范失败已修正，并通过包含相关规范与目录测试的 29 项定向回归。另两个依赖环境检查仍失败：vendor/dbt 当前提交与审计锁定提交不同，以及本地采用 editable 安装而测试要求非 editable；本轮没有修改这两项环境配置。真实 PostgreSQL 临时 schema 中的完整构建、部署、历史读取、QUERY/EXPLAIN/PREVIEW/DIMENSION_VALUES 集成测试通过。gzip Manifest 经数据库读回并派生目录的测试通过。Ruff、diff whitespace 检查通过，独立静态审查未发现阻塞项。
