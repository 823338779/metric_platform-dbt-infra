# 构建详细日志读取接口

> 后续决定：用户要求删除旧阶段日志接口及其重复存储；本文件中的兼容旧 /logs 要求已由 [移除方案](../plans/2026-10-25-remove-stage-log-api.md) 替代，详细日志接口设计仍有效。

用户选择新增 GET /builds/{buildId}/execution-logs，返回结构化 dbt 日志，保留现有 /logs 阶段文本接口。当前仓库交付后端接口和前端对接文档，不新增前端工程。

相比直接替换 /logs，新增接口不破坏原有返回类型和游标语义。采用 HTTP 游标轮询，复用项目分页编码和异常处理，不引入 SSE 或 WebSocket。

输入 buildId、cursor、limit（默认 100，1–200）；输出 items、nextCursor、hasMore。items 包含字符串 logId（避免 JavaScript bigint 精度问题）、任务/attempt 标识、阶段、命令、UTC 时间、级别、事件名/代码、invocationId、nodeId、threadName、完整脱敏消息。nextCursor 在空页和末页也返回，用于持续轮询；hasMore 只表示查询时已有更多记录，不表示构建完成或日志无损。

存储按 build_id 及 log_id 升序查询 limit+1 条，返回类型化快照。游标绑定构建和日志类型，不接受其他构建或阶段日志游标；非法参数为 422，构建不存在为 404，数据库异常沿用 503。

同一构建可能在不同 worker 上出现重试和旧 attempt 的迟到写入。为使 log_id 可作为本构建的增量游标，写入先取得构建级 PostgreSQL 事务 advisory lock，再分配日志序号；该锁只用于日志写入，不使用业务事实计数器锁。其他构建仍并行。

验证实际 HTTP JSON、空页续读、跨构建隔离、超大日志 ID、错误码、现有接口兼容，以及并发写入不会越过未提交批次。前端以 message 作为文本渲染，不作为 HTML；说明轮询、翻页、状态及日志丢失边界。
