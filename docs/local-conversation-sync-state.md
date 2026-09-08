# 本地原生同步状态查询

`getConversationSyncState(operationID?)` 返回 Promise，成功值为：

- `mode`: `uninitialized` / `negotiating` / `legacy` / `v2`
- `phase`: `idle` / `syncing` / `ready` / `failed`
- `progress`: 当前完整同步的整数进度。

它读取 Core 原子状态，不等待同步完成。`getLoginStatus() === 3` 或本地资源可读不能替代这个状态。只有明确 `mode=v2, phase=ready` 能证明 V2 初始化完成；协商中不能推断旧协议。

此接口需要本地二开的 Core 导出 `GetConversationSyncState`，不能配合原有远程 AAR/Pod 或仅发 JS 热更新。App 本地接入方式见其 `docs/openim-local-sdk-development.md`。
