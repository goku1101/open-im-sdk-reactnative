# 会话同步 V2：服务端实现与联调说明

日期：2026-09-08。状态：服务端源码已实现，本地真实依赖联调通过；未部署、未提交远程，SDK 尚未接入。

## 实现范围

不是直接合并官方 PR #3740，而是在现有定制服务端上实现独立 V2 协议。原 Conversation/Msg RPC、HTTP API 和 WebSocket 消息序号入口保持原有语义。没有账号灰度、白名单或全局开关。

- `pkg/syncv2/pb/sync.proto`：本仓库自有协议与已生成的 Go 文件，沿用当前 `github.com/openimsdk/protocol`，不依赖未发布的官方提交或第三方 fork。
- `pkg/syncv2/backend.go`：Mongo primary 批量读取所属会话、个人/全局序号、群成员权限及消息时间；Redis pipeline 读取最大/已读序号，miss 不逐条回源。
- `pkg/syncv2/snapshot.go`：Redis 跨实例固定列表、分页、配额、TTL、显式释放；先记录增量基准再扫描，续页使用 `LRANGE`，不反复读取整个快照。
- `pkg/syncv2/message.go`：显式 ID 的状态/详情和消息拉取；拉取前后重新读取权限边界，拒绝状态已变的在途结果。
- Conversation RPC、Msg RPC 和 API 已完成注册。V2 批量日志仅输出数量/模式，不输出完整 ID 列表、状态令牌或消息内容。

## HTTP 契约

全部为 POST，沿用 API 的 `token`、`operationID` 等现有请求头和 `{errCode, errMsg, errDlt, data}` 响应包装。下表为 `data` 内容。完整字段以 `.proto` 为准。

| 路径 | 请求 | 成功响应 |
| --- | --- | --- |
| `/conversation/sync_capabilities` | `userID` | `conversationSyncV2`, `policyVersion`, `maxBatchSize`, `maxPageSize`, `snapshotTTLSeconds`, `maxRequestBytes` |
| `/conversation/sync_snapshot` | `userID`, `pageSize`, `scope`；续页再传 `snapshotID`, `cursor` | 固定集合页、`total`, `hasMore`, 下一页 `cursor`, `expiresAt`, 增量 `versionID/version`, `mode`, `scope`, `includeSelf` |
| `/conversation/sync_snapshot_release` | `userID`, `snapshotID` | 空对象；释放自身已过期快照也成功 |
| `/msg/sync_seqs_v2` | `userID`, `conversationIDs`, `includeDetails`, `notifications`, `includeSelf` | `states[]`, `unavailableIDs[]` |
| `/msg/sync_messages_v2` | `userID`, `conversationID`, `seqs`, `stateToken`, `notifications`, `self` | 最新 `state`、允许读取的 `messages[]`、`unresolvedSeqs[]` |

`scope` 默认为 `chat`；`notifications` 页返回未应用闲置过滤的**父会话 ID**。两个 scope 的游标分开保存，不能互换。`mode` 为 `full`（总数 ≤ 5000）或 `filtered`（> 5000），不是旧接口模式。即使 `full` 也使用 V2 的显式范围、权限与分页。

例：

```json
{"userID":"alice","pageSize":300,"scope":"chat"}
```

```json
{"userID":"alice","snapshotID":"<返回的 ID>","pageSize":300,"scope":"chat","cursor":"<返回的 cursor>"}
```

```json
{"userID":"alice","conversationIDs":["si_alice_bob"],"includeDetails":true}
```

```json
{"userID":"alice","conversationID":"si_alice_bob","seqs":[8,9,10],"stateToken":"<该 state 的 stateToken>"}
```

通知状态请求使用父会话 ID 和 `notifications=true`，返回的 `state.conversationID` 为实际 `n_…` 流 ID；消息拉取仍传**父会话 ID**与 `notifications=true`，不能将返回的 `n_…` 当作父 ID。自通知单独调用 `notifications=true, includeSelf=true, conversationIDs=[]`；拉取传 `notifications=true, self=true, conversationID=""`。

推荐自通知单独请求：`includeSelf` 占一个批次名额，不能和 500 个父 ID 同批。普通空 ID 集合返回空，不查询全部；通知父集合为空时也只有显式 `includeSelf=true` 才查询自通知。

状态字段：`minSeq`、`maxSeq`、`permissionMaxSeq`、`hasReadSeq`、`unreadCount`、`lastMessageTime`、`known`、`isPinned`、`stateToken`；`includeDetails=true` 时附带 `conversation`。通知不允许附带普通会话详情。当前策略版本为 `conversation-sync-v2.2`；`permissionMaxSeq=0` 表示没有额外权限上限，不代表实时尾序号为零。

## 权限和一致性

- 请求所属用户必须与认证上下文匹配，或具备现有管理员权限；不能用 body 的 `userID` 自行声明身份。快照绑定用户，不替代读取授权。
- 每次按 ID 查询均重新读取 primary 上的所属会话、可见上下限；置顶不绕过权限。`unavailableIDs` 只表示本次不可用，不是删除凭据，SDK 不应据此删除本地历史。
- 普通聊天有效下界为 `max(1, 全局 MinSeq, 用户 MinSeq, 会话 MinSeq)`；有效上界取全局最大与所有正值个人/会话 MaxSeq 上限的较小值，零上限沿用“不限制”的存储约定。通知使用通知流自己的序号边界，不能拿普通消息序号截断通知。详情内的原始序号不能覆盖 `state` 的有效边界。
- 群成员移除先于序号冻结：成员已不存在但两个历史上限尚未就绪（含零值）时不返回聊天状态；已有正值上限时可以读取封顶的历史，不能读取退出后的新聊天。
- 退群/被踢/解散通知在 MsgTransfer 分配序号后、发布存储通知前，持久化离开用户的通知截止序号。离开用户可补到终止事件本身，但不能读取其后的群通知；重新入群者按当前成员权限读取。通知父集合不受闲置过滤，仍校验所属会话和最新权限。旧事件没有截止记录时保持不可用，不猜测或放开权限；SDK 仍须执行现有群成员/好友增量与自通知同步。
- 未读使用连续序号基础计数 `max(0, U - max(R, L-1))`，`L>U` 时为零；不额外扣除撤回/单条删除导致的序号空洞。通知不计入普通聊天未读，总未读继续遵守 SDK 既有免打扰等规则。
- Redis 的 `TIME` 是分配缓存时间，不用于闲置判断。只在实际最大可见消息存在且未对该用户删除时采用持久化 `send_time`（毫秒）。尾消息缺失、预分配空洞、消息时间未知时 `known=false`，保守保留；非法负序号返回错误，不静默过滤。
- `known=false` 时最大序号可能是预分配上界，未读不是已确认最终值。SDK 必须保留待校验状态；不能凭该结果确认“空/全部已读/历史范围完整”。预分配空洞可能长期存在，不应无限阻塞整个登录界面，也不能无限即时重试；初始展示与后台历史完整性分别管理。未知比例高时保守保留会降低过滤收益。
- 旧存储层会将缺失消息合成为已删除占位，有时还会附带相邻消息时间；V2 将这种没有消息身份的占位改为 `unresolvedSeqs`，不作为删除凭据。SDK 保留待校验缺口，真实清空/删除依赖权限边界或明确事件，不因此清除本地数据。
- `stateToken` 是可见权限边界相等性令牌，**不是单调递增事件版本**，只包含流 ID、最小可见序号及权限上限。新消息、已读推进、尾消息变为已知不会阻断历史拉取；清空、权限上限变化或请求序号因实际尾序号收缩而越界会触发重查。SDK 收到清空/退群事件后仍须作废此前在途响应，再读状态。快照不是跨表事务，也不保证其元数据版本覆盖消息和已读变化。

## 错误和重试

V2 主动返回的错误同时兼容 OpenIM `CodeError` 与 gRPC 状态。HTTP `errCode` 保留下列数值，`errMsg` 用 `SYNC_V2_…` 区分原因；现有鉴权错误继续使用项目原错误码。

| errCode | 典型原因 | SDK 行为 |
| --- | --- | --- |
| 3 | 非法参数、游标、页大小/通知选项 | 修正请求，不重试同样的错误参数 |
| 7 | 他人快照或消息会话当前不可用 | 停止该读取；不据此删本地历史 |
| 8 | 配额、并发、批量数量、响应/数据库读取预算 | 退避；批次过大则缩小批次；完成/取消时释放快照 |
| 9 | `SNAPSHOT_EXPIRED`、策略不兼容、非法存储状态 | 仅快照过期/策略变化重建；数据损坏应告警，不盲目重跑 |
| 10 | `STATE_CHANGED`、构建提交失效 | 重查状态或重建本轮任务，不沿用旧 token |
| 12 | 下游不支持相同协议版本/原生 RPC 不存在 | 重新协商并停止本轮 V2，受控进入旧流程 |
| 14 | Redis 不可用 | 可重试故障，不视为协议不支持 |

其他数据库/传输/鉴权错误按现有错误机制处理。超时、限流和空成功响应不得自动触发无过滤全量回退。旧版 API 的路由不存在也需与普通网络故障区分。

快照完成和取消后调用 release；继续翻页前不要释放。首请求响应丢失时残留快照由 TTL 回收；不要无限重复创建。创建接口目前没有客户端幂等键。

## 资源和部署

`config/openim-rpc-conversation.yml` 的 `syncV2`：阈值 5000、30 天、每批/页 500、TTL 600 秒、单账号扫描上限 100000、每用户 2 个/全局 64 个快照、构建超时 60 秒。旧配置未提供整个块时使用默认值；显式块各字段必须为合法正值，不支持零值关闭。

额外硬限制：请求 64 KiB、响应 protobuf 3 MiB、状态每进程 16 个并发（与消息拉取共用）、每次状态/消息读取 15 秒；扫描 ID 字符串总长 ≤ 4 MiB，单快照序列化预算 ≤ 8 MiB，单批 Mongo 结果解码预算 ≤ 8 MiB。默认快照载荷预算最高约 512 MiB，需另计 Redis 列表/键及进程工作内存开销。配额键使用相同 Redis Cluster hash tag，集中在一个 slot，部署需为该 slot 留足容量。

快照内部遇到数据库读取或状态响应字节超限时递归拆批；单条仍超限则仅在筛选阶段按未知保留，后续详情/消息读取仍检查权限和预算。其他错误不触发拆批。尾消息投影只返回序号、时间和当前用户是否在删除列表中，避免加载整个删除用户列表。

新增 Mongo 集合 `sync_v2_notification_boundary`，以用户和通知流的哈希为 `_id`，使用内置主键索引和 `$max` 幂等更新截止序号，不改旧 `seq_user`。终止通知写入采用 majority 确认；写失败会阻塞当前工作批次并重试，需监控 Mongo 故障带来的消息积压。解散受众在通知发布触发成员清理前，从 primary 流式读取。其余查询复用现有索引，本地 explain 已验证主要查询；生产需复核索引和负载。本次没有历史截止记录回填任务，升级前已离群且无截止记录的用户不能依靠 V2 补到旧终止事件，需将此列为迁移验收限制。

部署顺序：所有 MsgTransfer → 所有 Msg RPC → 所有 Conversation RPC → 所有 API；先确保终止通知写入端全部升级，再开放读取端。旧 WebSocket 入口保持原样，SDK V2 应使用本次 HTTP 入口。回滚反序，保留截止集合，SDK 确认不支持后再作受控补齐。此实现未向任何环境发布。

监控：`sync_v2_snapshot_build_seconds`、`sync_v2_selection_total{kind="input|selected|unknown"}`、`sync_v2_state_batch_size`，以及现有 RPC/API 错误计数。指标无用户 ID 标签。

## 本地验证

测试用独立、临时的 Mongo 7 / Redis 7 容器，不复用现有业务 Redis；集成测试创建随机 `syncv2_test_*` 数据库并在结束后删除。命令中的地址必须指向专用临时容器。

```sh
go test -race ./pkg/syncv2/... ./internal/api ./internal/rpc/conversation ./internal/rpc/msg
go vet ./pkg/syncv2/... ./internal/api ./internal/rpc/conversation ./internal/rpc/msg ./pkg/common/prommetrics
go build ./cmd/openim-api ./cmd/openim-rpc/openim-rpc-conversation ./cmd/openim-rpc/openim-rpc-msg
SYNCV2_TEST_MONGO_URI=mongodb://127.0.0.1:<mongo-port> SYNCV2_TEST_REDIS_ADDR=127.0.0.1:<redis-port> go test -tags=integration ./pkg/syncv2 -run TestRealMongoRedisGRPC -v -count=1
```

已验证：5000/5001 阈值；置顶/空/已清空/已读闲置/未读/未知；固定分页和 scope 游标；HTTP 请求字节上限、错误透传；真实 gRPC/OpenIM 拦截器；消息真实存储读取；跨用户访问、快照过期/释放；多请求原子配额；已读缓存编码；个人历史上限、退群零上限窗口、自通知；清空使旧 token 失效；竞态检测。

本次修复回归覆盖安全尾增长/已读推进、权限变化、内部递归拆批/单条保留，以及 MsgTransfer 的序号分配→截止持久化→Mongo 队列→Push 队列调用顺序。真实 Mongo/Redis/gRPC 验证退群、被踢、解散终止序号可拉取，之后的序号和非受众访问被拒绝，旧序号重放不降低截止值。尚未覆盖真实 Kafka 故障、重平衡及完整消息投递链路。

5 万会话测试夹具：每个会话 10 的最大序号、一个已持久化尾消息；每 1000 个分别保留一个置顶、一个未读、一个近期活跃，共选中 150 个。Redis 序号冷/热缓存均为 **402 次 Mongo find**（非逐会话），通知分页完整遍历 50000 个父 ID。本次修复后 race 运行快照约 8.37 / 8.36 秒。时间包含服务/RPC/快照存储，不包含造数；“冷”指 Redis 序号缓存缺失，不是清空 Mongo/操作系统页缓存。

这些结果证明批量查询与正确性，不是生产性能承诺。尚需生产等规模稠密消息文档/大群负载、多账号整轮登录 P50/P95/P99、实际 SDK/RN 端到端、通知事件收敛、混合版本和发布回滚验收。未构建 RN 原生 SDK，也未证明只上线服务端就能改善旧客户端登录。
