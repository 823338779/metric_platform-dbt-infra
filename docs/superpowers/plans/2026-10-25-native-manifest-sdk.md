# 历史 Manifest 交给原生 SDK

用户要求：服务只传递 Manifest，不解释指标、relation、节点或依赖字段；历史读取不重新 parse 原业务 YAML。

实现：
- ManifestCapture 只负责原生序列化、保存和传递字节；移除自定义字段读取和 metric_names/preview_relation。
- dbt_manifest 是 SDK 调用适配：完整 JSON 输入交给 WritableManifest.from_dict，随后调用 Manifest.from_writable_manifest、expect、Linker/Graph 和 SDK 序列化；服务不遍历或索引 Manifest 的业务字段。源码文件路径使用 dbt 节点查询结果。
- 查询不依赖 CatalogService，不通过资源 ID 推导指标名。输入改为 metrics（MetricFlow 原生名称），排序指标也使用原生名称。
- MetricFlow 输入由 dbt SemanticManifest.write_json_to_file 从保存的 Manifest 生成，不使用旧语义文件或原业务 YAML。
- PREVIEW 输入改为 selector，注入历史 Manifest 后调用 dbt show。排除 seed，避免 SeedRunner 物化；查询结果仍通过现有有界行列协议返回。
- 目录返回 dbt SDK 完整 artifact；节点与依赖使用 SDK 结果，不新增统一资源模型。

兼容变化：metrics 替代 metricResourceIds；selector 替代 datasetResourceId。dbt show 执行模型 SQL，与旧版读取物化表的预览语义不同。README 和契约示例已更新。库表和历史 Manifest 无需迁移。

验证：
- 新增 test_native_manifest_sdk：真实 DuckDB build 后删除业务 YAML/SQL，破坏旧 semantic_manifest，禁用 requires.parse_manifest 和 ManifestLoader.get_full_manifest；元数据、维度、QUERY、EXPLAIN、PREVIEW 仍通过。
- 初次完整非 integration 回归：527 passed、2 skipped、4 failed；两项为已有 vendor 提交/安装方式环境检查，另两项为测试仍使用旧指标资源 ID。已改为原生指标名。
- 修正后受影响的 API、查询、历史读取与真实 PostgreSQL 构建集成回归 76 passed；原生 SDK、MetricFlow 异常分类和字符串规范等 26 passed。
- 独立审查指出普通 QUERY/EXPLAIN 无效指标名被归类为 503；已修复为原生选择错误 INVALID_QUERY/422，并增加两种模式回归测试。初始化与连接错误保持原分类。
- Ruff、git diff --check 通过；未修改 vendor 当前工作区及已有依赖环境。


后续血缘入口调整：
- 用户要求 lineage 经 dbt 客户端统一入口，避免服务直接调用 dbt 内部图算法。
- 当前 dbt 没有对应离线命令；按此前允许修改 dbt 源码的方向，新增 vendored `lineage` 命令，保留现有一层父子结果。
- 服务仅注入 Manifest、调用 dbtRunner.invoke 并透传结果；Linker/Graph 位于 dbt task 内。命令不初始化项目、profile、适配器或解析器。
- CLI 支持 --manifest-path；文件通过 WritableManifest.read 加载，与内存路径采用相同反序列化方式，不进入依赖项目 flags 的状态比较升级流程。
- 缺失资源仍为404，图依赖损坏为503；历史目录在数据库物化表清理后仍可读取。
- 验证：客户端、独立CLI、无源码解析、目录HTTP、错误分类及字符串规范共30项通过；其中包含真实DuckDB构建后的历史SDK回归。
- 独立审查后：服务传递 resource_id 前增加 `--`，离线客户端调用跳过每次 invoke 的 dotenv 加载；新增入口参数与环境隔离回归。最终上述测试集32项通过，Ruff与两个仓库diff检查通过，审查复验通过。
