# ServerStatus (Go)

一个用 Go 实现的轻量 ServerStatus 风格监控项目：  
- `serverstatus-server`：接收客户端上报并通过 WebSocket 推送到前端  
- `serverstatus-client`：采集机器状态并按秒上报  

前端展示包含：CPU、内存、硬盘、负载、网络速率、流量统计（`vnstat`）、延迟探测、进程 Top、网卡明细等。

---
demo：
<img width="3749" height="1848" alt="image" src="https://github.com/user-attachments/assets/de5802ae-b50f-4ffa-8d31-c441a0d22ad0" />


## 1. 功能概览

- 实时上报（默认 1 秒）
- WebSocket 实时刷新
- 行点击展开详情
- 客户端离线自动剔除（服务端 TTL）
- 启动页注入初始化数据，减少空白等待
- 流量统计支持 `today/month/total` 及上下行拆分

---

## 2. 目录结构

- `server.go`：服务端
- `client.go`：客户端采集器
- `index.html`：前端页面（内嵌）
- `go.mod` / `go.sum`：依赖

---

## 3. 环境要求

- Go 1.20+（建议）
- Linux（客户端读取 `/proc`、执行 `df/ps/ping/vnstat`）
- 客户端依赖命令：
  - `ping`
  - `ps`
  - `df`
  - `vnstat`（用于流量统计）

> 注：`vnstat` 未安装或数据库未初始化时，月流量/总流量可能显示为 `0`。
apt install vnstat
---

## 4. 编译

在项目根目录执行：

```bash
make build
```
---

## 5. 运行

### 5.1 启动服务端

```bash
./serverstatus-server 9092
```

默认监听：`0.0.0.0:9092`  
页面地址：`http://<server-ip>:9092/`

可选环境变量（生产建议）：

```bash
ALLOWED_ORIGINS="https://status.example.com,https://ops.example.com" ./serverstatus-server 9092
```

- `ALLOWED_ORIGINS`：WebSocket `Origin` 白名单（逗号分隔）
- 未配置时：仅允许与请求主机同源的浏览器 `Origin`；无 `Origin` 的非浏览器客户端仍可连接

### 5.2 启动客户端

```bash
./serverstatus-client http://<server-ip>:9092 <node-name>
```

示例：

```bash
./serverstatus-client http://192.168.1.40:9092 test
```

当前默认延迟探测目标：

- 联通：`210.22.70.3`
- 电信：`202.96.209.5`
- 移动：`211.136.112.50`

---

## 6. API / WebSocket

### HTTP

- `GET /`：监控页面
- `POST /api/report`：客户端上报
- `GET /api/data`：当前全部节点
- `GET /api/history`：历史 ring buffer

### WebSocket

- `GET /ws`
- 推送类型：
  - `update`：单节点更新
  - `snapshot` / `history` / `tick`：节点数组

---

## 7. 常见问题

### Q1: 客户端断开后页面还在

已实现服务端离线剔除（TTL）。  
当节点超过 TTL 未上报，会自动从页面移除。

### Q2: 月流量 / 总流量为 0

常见原因：
- `vnstat` 没安装
- `vnstat` 数据库刚初始化，还没累计
- 某些版本 `--json d` 输出字段不完整

项目已做兼容与兜底逻辑，但首次部署建议确认：

```bash
vnstat --json
```

### Q3: 行点击展开偶发失效

根因通常是高频 WS 刷新与 click 时序冲突。  
当前实现已改为更稳的事件处理（pointerdown + click 兜底）并优化了渲染策略。

### Q4: 电信/联通/移动 ping 总是超时

常见原因：
- 目标 IP 在当前机房被 ICMP 限制
- 系统 `ping` 实现差异导致解析不到时延

项目已兼容不同 `ping` 输出格式（含 stderr 输出、`time=`/`time<`），并做了参数兜底重试。  
如果仍超时，建议替换为你网络内实际可达的探测 IP。

---

## 8. 生产部署建议

- 服务端已支持优雅停机（`SIGINT/SIGTERM`）
- 已设置 HTTP 超时（`Read/Write/Idle Timeout`）
- `/api/report` 已限制请求体大小（1MB）并启用严格 JSON 字段校验
- WebSocket 广播改为非阻塞，广播队列满时丢弃并计数，避免上报链路被反压拖死
- 客户端连接与广播流程已修复并发 map 读写风险

---

## 9. systemd 示例（可选）

服务端 `/etc/systemd/system/serverstatus-server.service`：

```ini
[Unit]
Description=ServerStatus Server
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/serverstatus-server 9092
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
```

客户端 `/etc/systemd/system/serverstatus-client.service`：

```ini
[Unit]
Description=ServerStatus Client
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/serverstatus-client http://<server-ip>:9092 <node-name>
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
```

启用：

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now serverstatus-server
sudo systemctl enable --now serverstatus-client
```

---

## 10. 备注

- 当前前端是单文件（`index.html`）方案，便于快速部署。
- 若后续要做复杂前端（主题、筛选、排序、搜索、权限），建议拆分为独立前端项目并用静态资源托管。
