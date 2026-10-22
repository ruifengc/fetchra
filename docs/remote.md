# 远端下载与多节点

Fetchra 的下载任务属于执行它的 daemon。CLI/MCP 可以退出、重连或在其他位置运行；远端任务不会随客户端退出。每个节点提供相同的任务 RPC，取回文件使用独立的 HTTP 接口。

## 单节点：HTTPS

下载机器启动：

```sh
umask 077
openssl rand -hex 32 > fetchra.token
fetchra serve --listen 0.0.0.0:8179 --token-file ./fetchra.token \
  --tls-cert ./server.crt --tls-key ./server.key
```

证书必须匹配客户端使用的主机名或 IP。公开 CA 的证书使用系统信任库；自建 CA 或自签证书通过客户端 `--ca-file` 加入信任库。程序不提供关闭证书校验的开关。

令牌至少 32 个非空白字符，配置文件应是权限为 `0600` 的普通文件。令牌赋予该节点的完整任务控制和文件取回权限，不提供多租户权限划分。令牌通过 Authorization header 发送，不能放在 endpoint URL 中。节点拒绝重定向和浏览器 Origin 请求。

下载来源的 `credential_ref` 与节点访问令牌是两套认证。凭据引用由目标节点解析；自动选择时应让候选节点具备对应凭据，特定账号来源可以显式选节点。

客户端用同一令牌：

```sh
fetchra --endpoint https://node.example.org:8179 --token-file ./fetchra.token \
  --ca-file ./private-ca.pem task add --start --request-key image-v1 https://example.org/image.iso
fetchra --endpoint https://node.example.org:8179 --token-file ./fetchra.token \
  --ca-file ./private-ca.pem task wait --id TASK_ID
fetchra --endpoint https://node.example.org:8179 --token-file ./fetchra.token \
  --ca-file ./private-ca.pem task fetch --id TASK_ID --file-index 0
```

省略 `--ca-file` 即使用系统信任库。endpoint 只接受源站地址，不接受路径、查询参数或内嵌用户名密码。监听地址和 URL 使用的端口应一致；`--listen 127.0.0.1:0` 可以让系统分配端口，实际 endpoint 会打印到 stderr。

## 单节点：SSH 隧道

这条路径复用已有 SSH 身份与加密通道，适合尚未部署 HTTPS 证书的个人下载机器。

下载机器只监听回环地址：

```sh
fetchra serve --listen 127.0.0.1:8179 --token-file ./fetchra.token
```

本机建立隧道：

```sh
ssh -N -L 127.0.0.1:8179:127.0.0.1:8179 USER@HOST
fetchra --endpoint http://127.0.0.1:8179 --token-file ./fetchra.token \
  task add --start --request-key archive-v1 https://example.org/archive.zip
```

HTTP 仅允许回环地址，令牌仍然需要安全复制到本机。Fetchra 不负责管理 SSH 会话；隧道断开时节点继续下载，客户端重新连接后查询同一个任务即可。

## 本地与远端路径

`task add /path/file.torrent` 中的路径属于 CLI/MCP 所在机器。控制层上传最多 8 MiB 的元信息，远端不需要同一个文件路径。MCP 也可直接提交 base64 的 `torrent_data`。直接调用节点 RPC 时，普通 source 文件路径属于节点，跨机器调用应使用上传字段。

`task_get`、`task_files`、`task_wait` 返回的文件 `path` 属于下载节点。需要本机文件时调用 `task_fetch`；其结果是本机路径、大小和 SHA-256。取回根目录由全局 `--fetch-dir` 配置，默认 `STATE/fetched`。每个文件的检查点目录由 endpoint、task_id、file_index 的散列确定；并发 CLI/MCP 通过文件锁共用同一个检查点。

取回是当前客户端调用的传输。退出或取消调用会中断取回；再次调用可恢复已验证的数据块。取回失败、超时或取消不会暂停或删除远端任务。发布文件不会覆盖已有文件；本地完成文件若损坏，返回 `LOCAL_DATA_CHANGED`，可选择新的 `--fetch-dir` 重新取回。

下载完成的文件由 Fetchra 管理，不应在原路径编辑。节点发现尺寸或时间戳变化会拒绝导出，返回 `LOCAL_DATA_CHANGED`；`task resume` 可重新验证。HTTP/FTP 若已发布文件内容损坏，应使用 `task restart` 从来源重新下载，原文件会保留。

每个入口同时最多服务八个文件请求；同一客户端对节点的数据请求共用四个请求额度。只有已验证就绪的文件可取回。Torrent 可以取回单个就绪文件，无需等待其他所选文件全部下载完。`task remove` 保留节点数据，但移除接口中的任务记录，取回需要在移除前完成。

## 可选多节点

节点配置示例：

```json
{
  "default": "auto",
  "nodes": [
    {"name": "home", "endpoint": "https://home.example.org:8179", "token_file": "home.token"},
    {"name": "cloud", "endpoint": "https://cloud.example.org:8179", "token_file": "cloud.token", "ca_file": "cloud-ca.pem"}
  ]
}
```

令牌和 CA 的相对路径相对于配置文件目录解析。节点名字区分大小写，只允许字母、数字、下划线和连字符；`auto` 是保留值。最多配置 32 个节点。

```sh
fetchra --state ./.fetchra/controller --nodes ./nodes.json nodes --json
fetchra --state ./.fetchra/controller --nodes ./nodes.json task add --start \
  --request-key dataset-v1 https://example.org/dataset.bin
fetchra --state ./.fetchra/controller --nodes ./nodes.json task wait --id home:TASK_ID
fetchra --state ./.fetchra/controller --nodes ./nodes.json task fetch --id home:TASK_ID
fetchra --state ./.fetchra/controller --nodes ./nodes.json --node cloud task list
fetchra --state ./.fetchra/controller --nodes ./nodes.json --node home policy get
```

使用实际返回的 `节点:任务ID`，而非固定选择示例中的节点。自动提交通过占用与队列相对于节点额度的比例选择，探测任务使用探测额度；并列时用请求键散列打散。这个分数反映当前任务负载，不能预测远端带宽或某个来源的最快节点。有节点适合特定来源时可显式选择它。

`auto` 提交必须带 `request_key`。节点归属与请求指纹在发送 RPC 前写入 `STATE/node-routes.db`。同一键不同参数返回 `REQUEST_CONFLICT`；控制端重启、回复丢失或节点离线后，重试仍然投递给原节点。原节点不可用时返回错误，其下载状态可能仍在变化，不能据此在其他节点重复创建。

节点配置删除或更改了某个已分配节点的 endpoint 时返回 `NODE_CONFIG_CHANGED`。可以恢复原配置，或明确用新请求键创建新任务。`node_list` 保留不可用节点，并给出它们的错误；不会假装这些节点没有任务。

同一组 CLI/MCP 客户端应共用控制状态目录与节点配置。多个独立控制状态目录之间不协调全局去重。每个节点独立管理调度、事件游标、限速和文件；文件导出与 Torrent 做种共享该节点的 upload_rate 额度。事件、列表、策略等非任务 ID 操作需要指定节点。单任务操作从完整 ID 自动路由，MCP 也可以通过 `node` 参数指定节点。

## MCP 配置

远端单节点：

```json
{
  "mcpServers": {
    "fetchra": {
      "command": "/absolute/path/fetchra",
      "args": ["--endpoint", "https://node.example.org:8179", "--token-file", "/private/fetchra.token", "--ca-file", "/private/ca.pem", "--fetch-dir", "/data/fetched", "mcp"]
    }
  }
}
```

多节点：

```json
{
  "mcpServers": {
    "fetchra": {
      "command": "/absolute/path/fetchra",
      "args": ["--state", "/private/controller-state", "--nodes", "/private/nodes.json", "--fetch-dir", "/data/fetched", "mcp"]
    }
  }
}
```

典型流程：`node_list` → `task_add`（稳定请求键）→ `task_wait` → `task_files` → `task_fetch`。工具输出一致的 result/error envelope，查询工具带只读提示。`task_wait` 最多等待 30 秒；`task_fetch` 的传输生命周期与调用绑定，大文件可多次续传。

## 为什么先按完整任务分布

节点下载完整任务，就能独立完成校验、恢复和交付。跨节点切分同一文件还需要处理共同来源标识、分片所有权、节点离线后的接管和最终合并；合并还会产生节点间传输。现在没有测量证明这些成本能改善目标场景，因此先保留独立节点的并发下载与显式选择。

当前不提供跨节点抢占、任务迁移、共享文件系统或一致性集群。新增这些能力需要能重现的瓶颈与恢复实验，不应仅因为“分布式”听起来性能更高而引入。
