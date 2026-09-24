# 推送仓库

当前三个仓库的工作分支都是 `develop`。先分别提交子模块中的修改，再在父仓库提交更新后的子模块引用和其他修改。从本仓库根目录依次执行：

```shell
git -C vendor/dbt push origin -f
git -C vendor/metricflow push origin -f
git push origin -f
```
