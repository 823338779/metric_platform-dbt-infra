# 指标平台：构建详细日志接入

调用 `GET /builds/{buildId}/execution-logs` 查看 service 发起的 dbt parse/build 等命令产生的详细日志。数据来自 PostgreSQL `runtime_execution_log`，服务重启后仍可读取已提交内容。原 `GET /builds/{buildId}/logs` 已删除，返回 404；旧游标不可用于新接口。

阶段、状态和错误码属于执行事实：前端通过 `GET /builds/{buildId}` 的 `phase`、`buildStatus`、`error.code` 和 `error.phase` 展示。服务从持久任务同步构建详情，不再将状态变更复制为阶段日志。日志页面中每条 dbt 事件仍携带 phase，作为定位该事件的上下文。

## 请求和返回

```http
GET /builds/6242c315-2fca-4561-922d-22ffd636214a/execution-logs?limit=100
```

| 参数 | 说明 |
| --- | --- |
| `buildId` | URL 中的构建 UUID，切换构建时清空游标和显示列表 |
| `cursor` | 首次不传；后续原样传回 `nextCursor`，不要自行解析或构造 |
| `limit` | 默认 100，最小 1，最大 200；后续可调整 |

返回结构（日志值仅为示例）：

```json
{
  "items": [
    {
      "logId": "123",
      "jobId": "6859f913-d744-40e5-8ced-6ad860ed56f4",
      "attemptId": "cd6108f0-e6cd-4f80-92ec-598b14a67836",
      "phase": "PARSING",
      "command": "parse",
      "eventTime": "2026-10-25T06:48:05.760409Z",
      "level": "info",
      "eventName": "MainReportVersion",
      "eventCode": "A001",
      "invocationId": "96bc41ac-f267-4dc1-96d1-7d7da6240a87",
      "nodeId": "",
      "threadName": "MainThread",
      "message": "Running with dbt=1.12.5",
      "createdAt": "2026-10-25T06:48:06Z"
    }
  ],
  "nextCursor": "<opaque cursor>",
  "hasMore": false
}
```

`logId` 是十进制字符串，以免 JavaScript Number 丢失 bigint 精度。日志已按 ID 升序返回；使用 `logId` 作为列表 key 和重试去重键，不按事件时间重新排序。`attemptId` 可用于区分重试，`invocationId` 可用于区分 dbt 命令调用；普通 Python 日志可能没有 invocationId。`nodeId` 可定位模型节点，命令级事件为空字符串。

## 增量轮询

1. 打开构建详情后不带 cursor 请求第一页。
2. 将 items 追加到显示列表，成功处理后保存 nextCursor。
3. hasMore=true 时立即请求下一页；hasMore=false 时表示当前已读到尾部，可每 1 秒继续用同一个游标轮询。
4. items=[] 时保留 nextCursor；不要把空页视为构建结束，也不要清空游标从头读取。
5. 构建状态仍通过 `/builds/{buildId}` 获取。观察到正常终态后再读一次日志并取完 hasMore=true 的页面。OUTCOME_UNKNOWN 需要继续核查状态，不能据此宣称日志已经全部到齐；页面保留刷新入口以接收可能迟到的记录。
6. 切换构建或离开页面时取消旧请求，避免旧构建响应追加到新列表。同一构建的分页请求应串行发出；错误重试沿用最后成功处理的游标。

空表也会返回有效的 nextCursor，因此排队中的构建可以从空页持续轮询到第一条日志。游标只表示已读取的持久记录位置，不保证未入库事件完整；写库故障、队列满或进程强制终止可能导致日志缺失。接口不返回虚假的 `truncated=false` 或“日志已完整”标记。

最小 TypeScript 请求函数（API_BASE 使用指标平台现有后端代理地址）：

```ts
async function readExecutionLogs(
  apiBase: string,
  buildId: string,
  cursor?: string,
  signal?: AbortSignal,
) {
  const query = new URLSearchParams({ limit: "100" });
  if (cursor !== undefined) query.set("cursor", cursor);
  const response = await fetch(
    `${apiBase}/builds/${encodeURIComponent(buildId)}/execution-logs?${query}`,
    { signal },
  );
  if (!response.ok) throw new Error(`读取构建日志失败：HTTP ${response.status}`);
  return response.json();
}
```

前端以文本渲染 message（如 React `{row.message}` 或 DOM `textContent`），用 `white-space: pre-wrap` 保留换行。不要直接使用 `innerHTML`；ANSI 转义序列可移除后显示。展示事件时间、级别、阶段、节点及消息即可；`warn` 与 `warning` 都按警告样式处理。时间是 UTC，展示时再转换为用户时区。

## 错误与部署

- 404 / BUILD_NOT_FOUND：构建不存在。
- 422 / INVALID_CURSOR：游标无效、属于其他构建或来自阶段日志接口。
- 422 / INVALID_REQUEST：UUID、limit 或其他 HTTP 参数不符合约束。
- 503 / STORAGE_UNAVAILABLE：数据库暂不可用，保留当前游标后退避重试。

访问部署边界沿用当前服务的只读接口约定，由指标平台后端按已有身份和构建权限访问；本次没有添加浏览器直连配置或跨域策略。尚未有详细日志表的旧库先执行 [日志表升级脚本](../src/dbt_metricflow_service/storage/add_execution_log.sql)；仍有旧阶段日志结构的数据库需停服并执行 [清理脚本](../src/dbt_metricflow_service/storage/remove_build_stage_logs.sql)，该脚本会删除旧阶段历史，可按需要先备份。历史上未持久化的 dbt 详细日志不会补回。

为保证多 worker/attempt 并发时按 logId 续读不漏记录，所有日志写入进程需要使用当前带构建级日志事务锁的版本；滚动升级期间不要混用旧写入器作为可靠游标来源。该锁不占用构建事实计数器，不串行化不同构建的日志。
