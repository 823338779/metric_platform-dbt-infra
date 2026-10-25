# 直接 build 实施计划

1. 新增失败测试：只有一次 build、宏和业务文件不修改、无需 docs/freshness/查询证明、内存 Manifest 入库、异常写入日志。
2. 缩短构建执行链，移除模板/schema/SQL/结果校验及孤立的验收函数；保留连接与任务基础设施。
3. 存储仅要求 Manifest，删除新的人工验证证据写入；更新相关测试和说明。
4. 验证真实 dbt 宏/custom schema/build 成败与日志、完整历史读取与查询集成、存储事务及租约回归。数据库测试只用隔离 schema。

已知环境基线：vendor 提交号与依赖审计锁不同，editable 安装与依赖测试预期不同；本轮不修改环境。

## 完成记录

已完成上述四步。构建仅一次 execute_build，删除模板/schema/SQL/freshness/物理及 MetricFlow 验收路径，不注入宏。完成存储仅要求 Manifest，无人工验证字段；SDK 异常通过脱敏入口写入执行日志。

独立审查指出两项问题并已修复：SDK 返回的 SystemExit/KeyboardInterrupt 不能再次抛出；Git 快照必须包含默认或自定义的已安装依赖目录。不再安装依赖，因此也删除了远端包必须携带 lock 的服务前置校验。新增测试先失败再通过。

全量非集成回归：534 passed、2 skipped，仅两项已知依赖环境检查失败。随后针对 SDK 返回异常、日志和源依赖修复的 39 项回归通过；构建/存储/租约/日志/目录 62 项回归通过，另补充字符串和构建日志 17 项通过。真实 PostgreSQL 临时 schema 的完整构建、部署、历史读取及四种查询模式最终通过；维度映射两项真实集成测试通过。真实 dbt build 覆盖自定义 schema/alias 宏成功，以及 SQL、模型测试、宏解析、缺失依赖四种失败日志。Ruff 和 diff 检查通过。
