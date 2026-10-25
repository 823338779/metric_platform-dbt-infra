# 从 Manifest 读取资源目录

用户要求移除 published_catalog.json，直接从已保存的 manifest.json 获取指标、维度、血缘，并选择表列仅显示 Manifest/YAML 声明，不补读 catalog.json。

## 方案

选择单一 Manifest 来源，读取时派生现有接口模型。另一方案是补读 catalog.json 保留数据库实际列，用户明确不采用。取消发布目录文件和两层中间目录转换；保留必要的指标引用、维度归属、血缘与资源详情映射。

## 边界

- CatalogService 按构建 output_set_id 读取 Manifest，复用 ArtifactStore 的解压和摘要校验；不读当前部署、不调用引擎。
- 保持公开资源 ID、接口结构和查询权限检查。表列只来源于 Manifest 的 columns，空声明返回空列，不推测实际结构。
- 构建仍验证模型执行、测试、物理对象和查询能力，目录可解析性通过同一内存适配函数验证；不生成新目录文件。
- 保留原生 catalog.json、semantic_manifest.json 供构建验证及 dbt/MetricFlow 使用。
- 删除仅服务于封存目录的中间模型、关系绑定记录和文件摘要字段。旧数据库允许保留未使用的列/触发器，旧产物不改写，读取忽略旧 published_catalog.json。
- 数据预览直接使用对应 Manifest 节点的 relation_name。历史资源详情可以离线查看，实际查询仍依赖物理对象。

## 验证

无 published_catalog.json / catalog.json / semantic_manifest.json 时，目录仍能读指标、维度、血缘和声明列；损坏或缺失 Manifest 返回产物不可用。验证派生/比率/累计/转化指标、同名维度、ephemeral 链、source、Windows 路径、历史构建隔离和 gzip 读回。数据库测试只使用临时隔离 schema。
