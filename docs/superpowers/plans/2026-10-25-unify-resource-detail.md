# 合并资源详情与源码接口

用户要求将资源详情和源码收敛为同一个接口，继续以简化 dbt-service 为目标。

## 接口决定

- 只保留 `GET /builds/{build_id}/resources/{resource_id}`，不增加 view 或 include 入参。
- 固定返回 `buildId`、`resourceId`、`resource`、`source`。resource 保留完整 dbt 原生节点；source 为 `{path, content}` 或 null。
- 删除独立 source 路由、应用方法及路由常量；native-details 继续不存在。
- 无源码路径或输入快照时 source=null，仍返回资源定义；源码快照读取失败保持 SOURCE_UNAVAILABLE/503，避免掩盖损坏。
- 源码仍来自同一构建的封存输入，返回整个文件，不访问本地工程或重新解析。
- 此次只修改 dbt-service；响应由原生节点改为固定结构，调用方需改读 resource/source。

## 执行与验证

- [x] 修改契约回归并观察旧实现失败。
- [x] 合并路由、应用方法和模型，更新样例、文档与相关测试。
- [x] 运行相关回归、全量测试和 Ruff；按仓库要求提交本计划的变更。

## 记录

只在现有工作区开发；不修改 vendor/dbt 的既有变更，不改业务库，不迁移跨产品调用方。

- RED：旧实现下合并响应相关回归 7 项失败；GREEN：应用层 12 项全部通过。
- HTTP、OpenAPI、契约样例及应用层相关回归：49 passed。
- Ruff 与 git diff --check 通过。
- 独立只读审查未发现新增可操作缺陷；未审查 vendor 的已有修改和跨产品调用方迁移，二者均在本次范围之外。
- 验证使用本任务临时 PostgreSQL 数据库和角色，每次运行后删除，不修改业务数据。
- 全量结果：554 passed、4 skipped、2 failed；失败仍为 test_dependencies 的 vendor 提交号和 editable 安装模式审计，与上一轮一致，未修改或弱化这些测试。
