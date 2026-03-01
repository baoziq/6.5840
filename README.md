## 你需要实现的模块与行为

### 1) Worker：会干两类活（Map 任务 / Reduce 任务）

worker 的主循环基本是：

- **向 coordinator 请求一个任务**
- coordinator 可能返回：Map 任务 / Reduce 任务 / 让你等待 / 全部完成退出
- worker 执行后把结果写到规定位置，并 **RPC 通知 coordinator：我完成了（或失败了）**

**Map 任务要做的事**

- 读取 coordinator 指定的输入文件
- 调用课程提供的 `mapf(filename, contents)` 得到一堆 `KeyValue`
- 按照 key 做分区：把每个 kv 分到某个 reduce bucket（通常是 `ihash(key) % NReduce` 的思路）
- 把每个 bucket 写成**中间文件**（后面 reduce 会读）

**Reduce 任务要做的事**

- 读取属于自己 reduce id 的所有中间文件
- 把所有 kv 按 key **排序/分组**
- 对每个 key 调用 `reducef(key, []string)` 得到输出字符串
- 写出最终结果文件（实验会拿这个和正确答案比对）

> 这里的“中间文件命名规范、最终输出文件命名规范”通常在 lab 文档/框架代码里写死；你只要严格按它来。

------

### 2) Coordinator：任务调度器 + 容错（最关键）

coordinator 要管理全局状态：

- 输入文件列表（每个文件对应一个 Map task）
- reduce 数量（对应多个 Reduce task）
- 每个 task 的状态：未开始 / 进行中 / 已完成
- 每个 task 被分配给哪个 worker（或者分配时间）

它要提供 RPC 接口，典型包括：

- **分配任务**：worker 来要活时，你给它一个尚未完成的 Map/Reduce 任务（或告诉它等一等/退出）
- **接收完成汇报**：worker 做完后告诉你 “task X done”，你标记完成

**阶段切换规则（Map -> Reduce）**

- **必须等所有 Map 任务都完成后，才能开始分配 Reduce 任务**
   （因为 reduce 需要所有 map 的中间结果）

**失败/超时重试（实验的“分布式味道”主要在这）**

- 如果某个 worker 拿了任务后“失联/卡死”，coordinator 不能永远等：
  - 超过一个固定超时时间（lab 通常会指定一个数量级，比如 10 秒），就把该任务标记回“可重新分配”
  - 让其他 worker 重新做同一个任务
- 这就是 lab 文档里说的“coordinator copes with failed workers”。 

> 注意：这并不要求你真的实现心跳系统；通常做法是“分配时记录开始时间 + 后台定期扫描超时任务”。

------

### 3) 正确性要求（测试主要卡这些点）

你写完后，测试通常会检查：

- **并发安全**：多个 worker 同时 RPC，要保证 coordinator 的任务状态不会乱（Go 里要用 mutex 或 channel 做同步）
- **不会重复计入完成**：同一任务可能被重试两次（第一次的 worker 后来“复活”上报 done），coordinator 要能处理这种重复/过期汇报
- **中间文件写法可靠**：Map 输出的中间文件如果被并发/重试覆盖，很容易出错
   常见策略是：先写临时文件，再原子 rename 到目标文件名（避免读到半截文件）
- **Reduce 输入要全**：Reduce 必须读到所有 map 产生的属于自己 bucket 的文件
- **最终输出格式对**：输出文件名、每行格式、排序/分组逻辑符合要求（测试会 diff）

------

## 你“没接触过 Go / 分布式”时的最小学习路线（只为做完 Lab1）

你不需要把 Go 学全，够用就行：

1. **RPC 基本用法**：Go 的 `net/rpc` 怎么定义参数/返回值结构体、怎么注册方法、怎么 call
2. **并发与同步**：至少理解 `mutex`（或 channel 方案）来保护 coordinator 内部状态
3. **文件 IO**：读文件、写文件、创建临时文件、rename
4. **排序**：对 `[]KeyValue` 按 key 排序（reduce 前要用）

------

## 交付物是什么

通常就是把 `mr/coordinator.go`、`mr/worker.go`（以及可能的 `rpc.go` 里补充的结构/常量）补全到能通过脚本测试即可——你不需要改应用层的 `mapf/reducef`，也不需要实现真正的分布式文件系统；输入输出都是本地文件。 

---

### coordinator

## 1) Coordinator 先“注册”自己能被远程调用的方法

在 coordinator 启动时通常会做类似这些事：

- 创建一个 `Coordinator` 对象 `c`
- 调用 `rpc.Register(c)`（或 `rpc.RegisterName("Coordinator", c)`）

注册后，**Coordinator 上满足 RPC 规则的方法**就能被远程调用，例如：

- 方法必须是导出的（首字母大写）
- 形如：`func (c *Coordinator) RequestTask(args *X, reply *Y) error`
- `args` 和 `reply` 都是指针、类型可被编码传输

这样 worker 才能用字符串 `"Coordinator.RequestTask"` 访问到这个方法。

------

## 2) Coordinator 开一个“RPC 服务器”，监听 unix socket

实验里一般是 unix domain socket：

- coordinator 在某个路径创建 socket 文件（比如 `/var/tmp/5840-mr-xxx`）
- 然后 `net/rpc`（通常配合 `rpc.HandleHTTP()` + `http.Serve`）开始监听

从这一步开始：

> 只要有进程连接这个 socket 并发 RPC 请求，coordinator 都能收到。

------

## 3) Worker 发起 `rpc.DialHTTP("unix", sock)` 并 `Call(...)`

你 worker 的 `call()` 做了两件关键事：

1. `DialHTTP`：连上 coordinator 的 socket
2. `c.Call("Coordinator.RequestTask", &args, &reply)`：发请求并等待返回

这里的 `args` 会被编码发送过去；`reply` 会在返回时被填充。

------

## 4) Coordinator 收到请求后：定位“要调用哪个方法”

RPC 服务器会解析请求里的字符串：

- `"Coordinator.RequestTask"`

然后找到：

- 目标对象：`Coordinator`
- 目标方法：`RequestTask`

------

## 5) Coordinator 并发执行这个 RPC handler（重要）

Go 的 RPC/HTTP 服务通常会 **为每个请求开一个 goroutine 来处理**。

所以如果你同时起了多个 worker：

- Worker1、Worker2、Worker3 同时请求任务
- Coordinator 可能同时并发进入 `RequestTask(...)`

这就是为什么你必须在 coordinator 里用 **mutex/锁** 保护共享状态（任务表、阶段机、计时信息），否则会出现：

- 两个 worker 拿到同一个任务
- 状态被并发写坏

------

## 6) 在 handler 里做“业务逻辑”：分配任务 / 更新状态

以“请求任务”为例，`RequestTask` 里通常会：

- 加锁
- 根据当前阶段（Map / Reduce / Done）
- 找一个 `Idle` 的任务
- 标记为 `InProgress`、记录开始时间
- 填充 `reply`（比如 `Type=MapTask, MapID=..., FileName=..., NReduce=...`）
- 解锁
- 返回 `nil`（表示 RPC 成功）

“汇报完成”的 handler 同理：更新任务状态、可能推进阶段。

------

## 7) 返回：reply 被编码回传，worker 的 `reply` 被填上

handler 返回后，RPC 框架会把 `reply` 编码，通过 socket 回到 worker。

worker 这边 `Call` 返回后：

- `call()` 返回 `true`
- 你传进去的 `reply` 结构体已经有值了
- worker 根据 `reply.Type` 决定执行 Map / Reduce / Wait / Exit