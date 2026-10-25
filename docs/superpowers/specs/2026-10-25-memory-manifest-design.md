# Manifest 从内存入库

用户已选择并接受：为 vendored `dbtRunnerResult` 增加 Manifest 返回字段，保持每条命令原有解析流程；服务保存最终 docs generate 命令使用的 Manifest，而不是 parse 阶段快照。

比较方案：复用 parse 返回值并传给后续命令会改变重新解析行为；采用返回字段可保留现有执行语义，因此选择后者。

dbt 返回内存对象；执行层通过可选回调直接传递 Manifest，由独立的 platform/manifest_capture.py 中的 ManifestCapture 序列化 WritableManifest 并校验字节大小。捕获器保留 dbt 原生 Manifest 对象，不定义或转换为自建 Manifest 模型。回调仍在引擎环境退出前执行。每个需要 Manifest 的命令独立捕获，构建校验从捕获字节生成已有 NativeManifest 投影，不再读取本地 manifest.json。最终只将 docs generate 的内容交给 ArtifactStore.capture，以内存字节计算大小和摘要并写入原路径。其他文件仍按既有流程读取，执行产物仍经原有租约检查与事务封存。内存缺失或序列化失败不能回退到磁盘或发布成功。

范围不包含删除本地文件、改造其他产物来源、数据库结构变化、移除源码快照或直接让 dbt 连接业务数据库。

用户追加要求：Manifest 使用 ensure_ascii=False、separators=(",", ":") 输出紧凑 UTF-8 JSON，并在存储层用 gzip level 6、mtime=0 压缩。压缩不节省空间时保留 raw，避免膨胀和触发已有存储大小限制。读取复用现有有界解压与摘要校验；旧 raw 文件兼容，其他产物和历史记录不重写。

验证包括真实 parse/build/docs 调用、无 Manifest 命令、失败调用、禁止磁盘 manifest 读取、容量限制、最终 docs 快照传递与现有发布验证。
