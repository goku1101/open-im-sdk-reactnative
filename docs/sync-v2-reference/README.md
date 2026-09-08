# 服务端 V2 契约来源与接入记录

核对日期：2026-09-08。

- 仓库：`git@gitea-ssh.yhdmit.com:onecode/openim-server.git`
- 分支：`master`，核对固定提交：`dafe810da6405162a23066462728d4f8821db455`，不跟随分支漂移。
- 镜像：`hugoliao/openim-server:vlink-syncv2-dafe810d`
- 策略：`conversation-sync-v2.2`
- 协议依赖：`github.com/openimsdk/protocol v0.0.73-alpha.19`

## 状态更正

用户本轮确认最新版镜像已经部署到测试环境。原附件的“未部署、未提交远程”属于发布前记录，归档原文不作改写。源码提交已读取验证；用户已确认使用 App 当前 staging API/WSS；测试账号位于 VLink 主工作区被 Git 忽略的 `docs/testing/test-accounts.local.md`，其中指定账号作为大账号。凭据只在本地读取，不复制到归档或版本库；实际环境能力和账号规模尚未现场验证。

## 原文归档

- [接口联调说明](../server-conversation-sync-v2-implementation.md)
- [服务端方案](../server-conversation-sync-plan.md)
- [SDK 方案](../sdk-conversation-sync-plan.md)
- [协议](sync.proto)：源路径 `pkg/syncv2/pb/sync.proto`
- [集成测试夹具](integration_test.go)：源路径 `pkg/syncv2/integration_test.go`

以上四份后端附件与固定提交对应文件逐字节一致。集成测试是源码参考，不能从此归档目录直接运行，也不是可登录的 RN 测试账号。

## 已锁定的 SDK 接入约束

1. 使用能力协商、快照、状态/详情、消息和快照释放五类 HTTP 接口，保留旧协议兼容；`full` 仍走 V2。
2. 同一快照含 chat/notifications 两套集合，复用 snapshotID，分别记录 scope 游标和页大小。两套分页完成或任务取消后释放快照。
3. 聊天详情使用状态请求 `includeDetails=true`，以 state 的有效权限边界为准。通知不带普通会话详情；父会话 ID 与通知流 ID 分开保存，自通知单独请求。
4. stateToken 只比较权限边界，不排序事件。清空/权限事件使旧在途任务失效；STATE_CHANGED 后重查。
5. unavailableIDs、known=false、unresolvedSeqs 均不是删除凭据。保留本地历史/草稿；初始可用与后台完整性分别处理。
6. 错误必须结合 errCode 与具体 SYNC_V2 原因处理，资源限制不等于协议不支持；请求数量、字节及并发均遵循服务端限制。
7. 5 万会话夹具选中 150 个聊天会话，通知仍遍历 50000 个父 ID；不把 SDK 方案中的 5000 个保留会话示例当作该夹具预期。

## 尚待补齐的验收条件

- 测试环境与账号来源已确认；下一步验证能力响应、指定大账号实际会话规模和双端登录。
- `/msg/search_msg` 的关键词搜索契约与权限验收仍未交付；`sync_messages_v2` 是指定序号读取，不替代关键词搜索。保留该需求为待完成，不擅自取消。
- Go SDK Core 已开始二开，协议请求、快照、分批和本地检查点基础已提交；运行入口及双端绑定尚未接入，当前 RN fork 的桥接补丁和 Promise 修复不等于 V2 已接入。

## 本地开发基线

Go SDK Core 已在独立目录建立 `codex/native-conversation-sync-v2` 分支，基线 `v3.8.3-patch.15`（`d6e0b549db904d0327d76ef7c9c283203879a095`）。该基线不是支持 V2 的版本。首批本地实现提交 `32275d70d2e2cbde226436c14177c83d98d78e0d` 将 protocol 从 `v0.0.73-alpha.12` 升至服务端使用的 `v0.0.73-alpha.19`，完成以下基础：

- 五类 HTTP 请求和错误分类；按实际后端 JSON binding 编码 int64 序号，兼容释放成功省略 data 的响应。
- 同一快照的聊天/通知分页、完整性校验、取消释放、数量与字节分批、自通知独立读取。
- 独立 SQLite 检查点及旧安装迁移，准备阶段不写同步完成标记。
- 定向测试、竞态检查、Go vet 以及会话/消息模块编译通过；5 万会话合成用例确认聊天详情只请求选中 150 个，通知遍历完整集合。

这些是 Go 请求层和数据库验证，不是双端原生包或真实大账号验收。测试环境业务鉴权接口已连通，图形验证码校验未通过，尚未取得用于 V2 联调的 IM 会话。Core 提交当前仅在本地独立仓库，尚未发布二进制，App 不会因该提交自动启用 V2。

执行顺序：协议/错误分类与定向测试 → 快照和本地进度持久化 → 约束详情、序号与普通消息三个入口 → 通知和事件失效/恢复 → 双端绑定、测试环境大账号及旧设备回归。服务器关键词搜索保持待交付项，不阻塞上述同步改造，也不视为已完成。
