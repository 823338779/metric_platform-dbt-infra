# 待办事项

1. 当前同一进程内的 dbt/MetricFlow 引擎调用只能串行执行。因为运行参数、adapter 注册与连接、环境变量及日志状态由多个线程共享，并发调用可能互相覆盖配置或清理对方的连接，因此使用 `ENGINE_LOCK` 保护。多个 worker 只是异步任务槽位，进入引擎时仍需排队；改用内存加载 YAML 也不能消除这些共享状态。
2. metricFlow无法消费offset字段，对于分页的能力比较弱。
3. dbt现在做出的semantic_manifest.json会让metricFlow消费，但是它们是两个独立的环节。所以现在需要做的事情是，让dbt构建时候的产物都要持久化到数据库，metricFlow不能通过磁盘读取。