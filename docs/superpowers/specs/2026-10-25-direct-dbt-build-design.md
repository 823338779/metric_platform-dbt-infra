# 直接执行 dbt build

用户明确要求删除服务额外的模板、目标、SQL、freshness、物理结果和查询探测校验，只还原工程、执行 dbt build、保存该次调用返回的 Manifest；错误通过现有日志表记录。

采用一次 build 的方案，不保留原多阶段验收。dbt 自己处理工程/适配器宏、解析、编译和模型测试；服务不注入 schema/alias 宏，不执行 deps、parse、compile、source freshness、docs generate 或 MetricFlow 探测。依赖包须由工程提供，缺失时由 dbt 报错。成功表示 dbt build 成功，不再承诺额外查询探测或服务限定的物化/schema 策略。

保留完整工程还原、临时 profile 与受保护凭据、任务租约/并发/超时、日志脱敏、Manifest 大小限制及产物完整性。命令固定 target 输出目录；成功后仅同步执行副本的 target-path，保证存储与历史读取使用同一位置。输入源码不改写。

存储完成要求改为只有内存 Manifest；semantic_manifest 如由 dbt 生成则正常保留，不再要求 docs catalog 或人工验证证据。删除已失去用途的构建校验和探测代码。历史错误详情字段可保留以读旧记录。

dbt 返回异常或直接抛出异常时，在引擎环境内通过现有脱敏日志入口补记错误，保证没有结构化 dbt 事件时仍能查日志。日志写入失败保持现有故障隔离。实际查询仍使用独立的 MetricFlow 查询路径。
