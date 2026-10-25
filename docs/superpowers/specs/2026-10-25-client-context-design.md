# 服务内 Client 的 Manifest 上下文

用户明确选择：Client 留在 dbt-service，保持薄适配。服务业务层保存、读取并传递 manifest 字节，Client 调用 dbt/MetricFlow 原生 SDK 解释内容；不自行实现字段解析、资源模型、依赖图或指标语义。

保留 dbt 原生 WritableManifest/Manifest 加载、expect、dbtRunner 注入和语义转换。ManifestCapture 继续通过 dbt 原生序列化保存构建产物，不改 Postgres/HTTP 协议。

MetricFlow Client 为每个请求缓存一个 dbt 上下文和一个 SemanticManifestLookup，维度和查询使用同一语义快照。执行时仅从 project/profiles 加载 adapter；不再生成或读取 semantic_manifest.json。维度发现继续无需工作目录或仓库连接。

验收：真实 build 后删除本地 manifest 和源码，禁止 parse，资源、维度、explain、查询仍能使用传入快照；不同快照隔离，损坏快照失败。业务层不得导入 Manifest 加载和语义转换模块。
