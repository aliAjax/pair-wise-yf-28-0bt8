# 临床试验随机分配与盲法服务

仅使用 Python 3.11+ 标准库的独立随机化服务。支持分层区组随机、试验方案锁定、隐藏分组、外部编号并发幂等、中心隔离、双人揭盲、号段预占、药品备货对账和审计。

## 运行

```bash
python3 app.py --init --seed
python3 app.py
```

访问 <http://127.0.0.1:8104>，默认数据库 `randomization.db`。测试：

```bash
python3 -m unittest -v
```

演示用户：`site1`、`site2`（研究中心），`coord`（协调员），`monitor1`、`monitor2`（监查员）。

## 号源账：预占、发号、备货

入组高峰前，中心可按分层预占号段并写下失效时刻，把号段预占、受试者入组和药品备货接成一本号源账：

- **按分层申请号段**：`POST /api/trials/{id}/reservations`，请求体 `factors`（分层因素）、`count`（号段数量）、`expires_at`（失效时刻，ISO 8601 带时区）、可选 `seq_start`（指定起始号）。号段在 SQLite `BEGIN IMMEDIATE` 事务中原子落库；两人同时申请同一分层同一号段，先落库的拿到，后到的收到 `range_conflict`（含 `conflict_reservation_id` 冲突编号与 `remaining` 剩余号段）。
- **计划写满排队**：每个分层有 `planned_count`（计划人数，建试验时用 `stratum_planned` 指定，默认 50）。当 `已发号 + 有效预占 + 申请量 > planned_count` 时，申请进入排队（`status='queued'`，带 `queue_no`）；号段过期退回后按 FIFO 自动补号。
- **过期退回中央池**：`expires_at` 到期后，未发完的号退回中央池（清除预占标记，可被同分层重新申请/排队补号），已发给受试者的随机号和分组保持不变；对应未发放的药品包装退回（`status='returned'`）。
- **越权直接拒绝**：预占、备货都只能操作本中心的号段；跨中心操作返回 `site_isolation`。
- **写盘失败不留半条**：预占与备货均为单事务，任何异常整体回滚，失败后可直接重试，不产生半条记录。
- **药品备货**：`POST /api/trials/{id}/reservations/{rid}/drug-stock`，请求体 `packs`（包装号数组，按顺序占用预占号段）。受试者入组发号时自动发放对应包装（`status='dispensed'`）。
- **审计对账**：`GET /api/trials/{id}/reconciliation` 汇总预占、发号、药品三本账，给出 `totals` 与 `discrepancies`（如已发号未备货 `issued_without_stock`、已备货未退库等）。
- **剩余号段**：`GET /api/trials/{id}/availability?factors={json}` 返回本分层计划人数、已发号、有效预占、剩余号段。

## 主要接口

- `POST /api/trials`：创建草稿试验，指定分组、分层因素、区组长度、随机种子和 `stratum_planned`（分层计划人数，可选）。
- `POST /api/trials/{id}/protocol`：入组前修改方案；一旦入组即锁定。
- `POST /api/trials/{id}/start`：开始入组。
- `POST /api/trials/{id}/enroll`：按当前用户中心入组；响应只返回分配编号，不返回分组；若本中心有生效预占则优先消耗预占号并自动发放对应药品包装。
- `GET /api/trials/{id}/participants`：分中心返回数据，中心用户看不到其他中心。
- `POST /api/participants/{id}/unblinding-requests`：发起揭盲。
- `POST /api/unblinding-requests/{id}/approve`：两人独立审批；同一人不能审批两次。
- `POST /api/trials/{id}/reservations`：按分层预占号段（冲突返回 409 + 冲突编号 + 剩余号段；计划写满返回排队）。
- `GET /api/trials/{id}/reservations`：列出预占记录（中心用户仅本中心）。
- `GET /api/trials/{id}/availability`：本分层剩余号段。
- `POST /api/trials/{id}/reservations/{rid}/drug-stock`：为预占号段备货药品。
- `POST /api/trials/{id}/strata/{sid}/plan`：协调员调整分层计划人数。
- `GET /api/trials/{id}/reconciliation`：审计员拿预占、发号、药品记录对账。
- `GET /api/trials/{id}/summary`：中心级汇总和审计记录。

随机表按“试验种子 + 中心 + 分层因素”确定性生成，每个区组为分组数的整数倍并打乱；分配在 SQLite `BEGIN IMMEDIATE` 事务中原子占用。实现适合作为流程原型，不替代经认证的临床试验随机化系统。
