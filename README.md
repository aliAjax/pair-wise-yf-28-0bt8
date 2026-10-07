# 临床试验随机分配与盲法服务

仅使用 Python 3.11+ 标准库的独立随机化服务。支持分层区组随机、试验方案锁定、隐藏分组、外部编号并发幂等、中心隔离、双人揭盲和审计。

在实时发号之上增加了一本**号源账**，把「中心号段预占 → 受试者入组发号 → 药品备货发药」接成一条链，支持入组高峰前提前备药。

## 运行

```bash
python3 app.py --init --seed
python3 app.py
```

访问 <http://127.0.0.1:8104>，默认数据库 `randomization.db`。测试：

```bash
python3 -m unittest -v
```

演示用户：`site1`、`site2`（研究中心，中心号 S001/S002），`coord`（协调员），`monitor1`、`monitor2`（监查员/审计员）。

## 号源账模型

```
中央池（按分层的计划人数）
   │  中心按分层申请号段，写下失效时刻（ttl_minutes，1~1440 分钟）
   ▼
segment_reservations 有效号段  ──同事务──▶  drug_shipments/drug_kits 备货（一号一盒）
   │  受试者入组从本中心号段按序取号
   ▼
participants 发号（随机号+分组保持隐藏）  ──▶  药盒 dispensed
```

- **先落库者拿到**：预占在 `BEGIN IMMEDIATE` 事务内完成，两人同时提交同一层时，先拿到写锁的成功；后到的收到 `409 segment_conflict`，响应里带剩余连续号数、剩余总数和冲突编号（占用方的 reservation_id/号段范围/失效时刻）。
- **计划写满就排队**：中央池没有空闲号时申请进入 `reservation_queue`（FIFO），返回 `409 plan_full_queued` 与队列位置。
- **过期退回重算**：号段到失效时刻仍未发完，未发的号解绑退回中央池（药盒置 `released`），并按 FIFO 尝试满足排队申请；**已经发给受试者的随机号和分组原样保留**。入组与预占时会自动清扫过期号段，也可手动 `POST .../sweep-expired`。
- **越权直接拒绝**：中心用户只能给自己中心申请号段，带他人 `site_id` 返回 `403 cross_site_forbidden`；协调员可代任意中心申请但必须显式给 `site_id`。
- **写盘失败不留半条**：预占、号绑定、备货、药盒、审计在同一事务，任一步失败整体回滚；可用 `client_token` 幂等重试，成功后重放返回同一号段。
- **审计对账**：`GET .../ledger` 同时给出预占、发号、药品三类记录，逐分层校验 `已发+有效占用+空闲+未生成=计划`、逐号段校验 `备货数=号段大小、发药数=发号数`，返回 `balanced` 与明细 `checks`。监查员可见分组，中心视图保持盲态并做中心隔离。

## 主要接口

试验与方案：

- `POST /api/trials`：创建草稿试验，指定分组、分层因素、区组长度和随机种子。
- `POST /api/trials/{id}/protocol`：入组前修改方案；一旦入组即锁定。
- `POST /api/trials/{id}/start`：开始入组。
- `POST /api/trials/{id}/plans`：协调员按分层下达计划人数，随即确定性生成中央池随机表（末区组按计划截断）。

号源账：

- `POST /api/trials/{id}/reservations`：按分层申请号段。请求体：`{factors, size, ttl_minutes, site_id?, client_token?}`。
- `GET  /api/trials/{id}/reservations`：查看号段与排队（中心用户只看本中心，可带 `?site_id=` 过滤）。
- `POST /api/trials/{id}/sweep-expired`：立即清扫过期号段并触发排队重算。
- `POST /api/reservations/release`：提前归还未发完的号段 `{reservation_id}`。
- `POST /api/trials/{id}/enroll`：从本中心当前有效号段按序发号入组，同时发药盒；响应只给编号与药盒标签，不返回分组。未先预占的中心收到 `409 reservation_required`（无计划层保留旧的实时取号兼容模式）。
- `GET  /api/trials/{id}/participants`：分中心返回数据，中心用户看不到其他中心。
- `GET  /api/trials/{id}/ledger`：审计员拿预占、发号、药品记录对账。
- `GET  /api/trials/{id}/summary`：中心级汇总和审计记录。

揭盲：

- `POST /api/participants/{id}/unblinding-requests`：发起揭盲。
- `POST /api/unblinding-requests/{id}/approve`：两人独立审批；同一人不能审批两次。

随机表按「试验种子 + 分层因素 + 区组号」确定性生成，每个区组为分组数的整数倍并打乱，末区组按计划人数截断；号段预占与发号在 SQLite `BEGIN IMMEDIATE` 事务中原子落库。实现适合作为流程原型，不替代经认证的临床试验随机化系统。
