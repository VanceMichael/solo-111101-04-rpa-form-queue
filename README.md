# 表单录入队列的断点与人工接管

离线采集工程使用 Python 标准库和 SQLite。adapter 读取本项目的真实来源格式，sources 提供可重复响应序列，capture 原子归档解析后的来源记录。已有 --capture 只记录原始观察，不作业务投影；--run 工作流已实现：为每条申报维护可恢复的执行阶段、提交前后证据与人工接管原因。

## 环境与准备

推荐 Python 3.12.12，支持 Python 3.11+；requirements.lock 为空表示没有第三方包，运行、测试和验收均不下载数据或依赖。Dockerfile 提供同一 Linux 环境。需要容器时先在正式运行之外准备：

```sh
docker build -t q004-rpa-form-queue:initial .
docker run --rm q004-rpa-form-queue:initial
```

## 基线测试与原始采集

```sh
python3 -m unittest discover -s tests -v
python3 -m compileall -q collector_04 tests acceptance
scratch=$(mktemp -d)
python3 -m collector_04.cli --database "$scratch/raw.sqlite3" --capture fixtures --route /tasks
python3 -m collector_04.cli --database "$scratch/raw.sqlite3" --capture fixtures --route /tasks
```

第一次 decoded=2、inserted=2，重抓同一观测 inserted=0。临时库不提交。tests 是已实现采集基础能力的回归测试，不代表扩展功能完成。

## 来源协议

这里只操作本地内部表单替身，不需要访问任何第三方业务。portal.LocalForm 提供 read_form、submit、lookup，状态存在独立 portal.sqlite3，业务调度状态存在 --database；两库不能靠一个事务伪造原子提交。task id 是申报幂等线索，服务器允许重复提交，因此防重责任在调度器。空 supplier 是字段校验失败 needs_input，不能阻塞 D1。

提交前记录 prepared、表单字段及 rule_version；成功保存后才标 confirmed 并保存 receipt。commit_then_timeout 表示服务端已保存而客户端没有收到回执，resume 时先 lookup，找到唯一同内容回执则确认；hide_receipt 表示读回不可验证，进入 manual/unverifiable_commit，不得重发。pause_before_submit 只准备不提交。next_rule_version 模拟确认后规则变化，不能把已确认任务重新录入；未提交任务则重新校验并保留旧证据。resume 必须经过新建执行对象而不是仅在一个内存对象重试。

FixtureTransport 每次进程创建从 routes.json 首条响应开始，同一路由多次 request 依次推进，最后一条可重复；calls 保存请求证据。源数据相对 fixture_root 读取，不能越出该目录。所有响应是本地合成数据，不发出公网请求。单场景 JSON 的 fixture_root 相对当前工作目录解析；在仓库根目录执行即可。run_id 是采集批次标识，name 仅作展示标签。

## 扩展接口与可观察输出

`python3 -m collector_04.cli --run 场景文件 --database 状态库 --output 结果文件`

场景文件含 name、run_id、sources（有序根来源）、options、fixture_root；name 只作日志标签。expected 是公开最小预期，产品不应读取它。所有来源和选项应独立支持；四个场景不能替代全部边界测试。业务结果投影：jobs（含 id、state、reason、rule_version、提交前后 evidence）、form_submissions（替身库真实行数）、receipts、handoffs。数组顺序不影响检查，公开预期数组是子集，但空数组要求确实为空。内部 SQLite 表、架构和调度实现由开发者选择。每个场景在同一状态库上用两个独立进程执行，以检查重启和重复输入；业务投影应保持一致，notifications 为数量时表示此次新建通知数，第二次应为0，为数组时表示持久化待处理通知清单。

## 分步运行、暂停与恢复

执行阶段持久化在 --database：received → prepared → submitting → confirmed；needs_input 是提交前的字段校验分流，submit_unverified/manual 是提交后的不确定分流，两类绝不互相转换。jobs 的 evidence.pre_submit / evidence.post_submit 分别保存提交前后证据；handoffs 是待人工接管清单（含原因），receipts 是已确认回执。

```sh
# 1) 暂停：只准备不提交（fixtures/pause.json 自带暂停选项；任意场景可用 --pause-before-submit 强制）
python3 -m collector_04.cli --run fixtures/confirmation_lost.json --database collector_04.sqlite3 --output out.json --pause-before-submit
#    → D1 prepared、D2 needs_input，form_submissions=0
# 2) 解除暂停继续：提交后超时（commit_then_timeout），恢复阶段新建执行对象 lookup 核对后确认
python3 -m collector_04.cli --run fixtures/confirmation_lost.json --database collector_04.sqlite3 --output out.json
#    → D1 confirmed（receipts[0].via=lookup），portal.sqlite3 仅 1 行
# 3) 重启后重复执行：已确认任务绝不重录，投影保持一致
python3 -m collector_04.cli --run fixtures/confirmation_lost.json --database collector_04.sqlite3 --output out.json
#    → form_submissions 仍为 1
# 4) 查询最终处理清单（不执行流程，portal 路径取状态库记录，可用 --portal-database 覆盖）
python3 -m collector_04.cli --database collector_04.sqlite3 --status
```

--pause-before-submit / --no-pause-before-submit 与 --resume / --no-resume 覆盖场景 options；不传时以场景文件为准。判断是否重录可对投影的 form_submissions 与 portal.sqlite3 实际行数：`python3 -c "import sqlite3; print(sqlite3.connect('portal.sqlite3').execute('SELECT count(*) FROM submissions').fetchone()[0])"`。恢复语义由独立进程测试覆盖：

```sh
python3 -m unittest tests.test_process_recovery -v
```

## 验收与正式展示

```sh
python3 acceptance/check.py --contracts-only
python3 acceptance/check.py
```

contracts-only 仅检查材料结构，不能当成功能验收。第二条是功能验收入口：四个场景均应 PASS；每个场景独立临时目录，执行两次、每次进程超时5秒，八次总预算40秒，无固定 sleep。正式展示从干净状态执行基线测试和第二条命令，预算小于90秒；依赖和镜像预先准备，不能用旧日志代替运行。参数变化及边界回归见 tests/test_scheduler.py 与 tests/test_process_recovery.py（独立进程恢复），不修改预期来迁就实现。
