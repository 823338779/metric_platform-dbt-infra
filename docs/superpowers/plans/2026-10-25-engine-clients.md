# dbt 与 MetricFlow client 重构

**目标：** 两个工具的 SDK 调用分别封装在 delegates/clients/dbt_client.py 与 metric_flow_client.py，方法接收必要上下文，不让上层解释 Manifest 字段。

**结构：** DbtClient 接收可选 Manifest 原文字节，暴露 build、invoke、catalog、resource、lineage、source_path、write_semantic_manifest、preview。MetricFlowClient 接收原文字节以及 project/profiles 路径，惰性初始化引擎，提供 available_dimensions 与 query（现有 QUERY、EXPLAIN、DIMENSION_VALUES）。按用户追加要求，移除 platform/metricflow 中转；业务入口通过 Runtime 上下文组装直接调用对应 client，执行层保留串行、日志与超时保护。首次 build 需要源工程，不从 Manifest 重新构建源码。

- [x] 用真实构建测试新 client 接口；删除业务源码后验证历史 Manifest 仍可查询。
- [x] 迁移 dbt 和 MetricFlow 调用与专用查询辅助函数，更新调用点，移除旧 dbt_manifest 模块。
- [x] 更新单元测试的 SDK 替身边界；验证参数、错误分类、日志脱敏、并发保护及 API。
- [x] 更新文档，完成独立审查和静态检查。

不改 API 请求/返回格式，不新增通用 client 基类、资源模型或连接配置模型，不改持久化结构。

上下文细化：CatalogService.read 取得构建与原文字节后调用 DbtClient；Runtime._query_context 提供 (manifest_bytes, project, profiles)，PREVIEW 直接调用 DbtClient.preview，其余数据操作调用 MetricFlowClient.query。构建上下文 CommandSpec 绑定 DbtClient.build 操作，执行层只负责保护与日志回调。


验证记录：
- 全量非集成测试：539 passed、2 skipped；架构检查因新增client边界失败1项，另2项为此前已有vendor审计提交号与editable安装环境检查失败。
- 架构检查已改为验证client边界及SDK入口归属，不允许业务层/上下文层直接导入dbt CLI；DbtResult是原生结果类型的别名，不新增结果模型。
- 最终受影响测试（含真实PostgreSQL构建与查询、日志脱敏、取消/超时、目录、原生SDK）：137 passed。
- 真实维度映射集成：2 passed。
- Ruff及主仓库/vendor diff检查通过；独立审查无重要问题。


后续：血缘改为 dbt 原生 ls。
- 删除自定义 dbt lineage 命令及其专用task；保留Manifest返回与portable序列化等独立改动。
- DbtClient.lineage(selector, project, profiles) 注入历史Manifest，返回ls的原生JSON节点数组；不映射unique_id到selector，不计算parent_map/child_map。
- API改为GET /builds/{build_id}/lineage?selector=...。旧资源lineage视图不再提供；未匹配返回[]，图深度与资源选择完全采用dbt语法。
- Runtime._engine_context统一准备临时上下文；数据查询额外持有query_run保护，血缘只读元数据无需该锁，支持物化表清理后的历史读取。
- 用真实引擎验证原目录销毁、禁止parse和仓库连接时，ls仍从内存Manifest工作；验证空匹配、HTTP原生响应、CLEANED、超时及目录/环境清理。
- 验证：最终受影响测试及真实PostgreSQL集成79项通过；Ruff、主仓库及vendor diff检查通过。独立审查发现的原生InvalidSelectorError误映射503已修复为422，并补充client/HTTP回归；不新增选择器前置校验。
