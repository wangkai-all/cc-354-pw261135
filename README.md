# 范围采集控制台（Scope Crawler）

按规则决定哪些子栏目需要采集的可运行演示项目。三个服务通过一张持久队列协作；
规则变化时，**已抓取的页面**与**正在排队的链接**都会重新判断，但**证据永不删除**。

本项目从空目录实现，使用本地站点演示真实 HTTP 请求与范围迁移，不涉及绕过访问限制。

## 快速开始

```bash
python3 -m venv .venv
.venv/bin/pip install flask pytest
.venv/bin/python -m scope_crawler.cli serve
# 打开 http://127.0.0.1:8080/
```

`serve` 会同时启动：

* 演示站点 `http://127.0.0.1:<随机端口>/site/`（含别名、跨栏目引用、越界链接、
  403、robots 禁止、循环链接等情形）；
* 控制台 `http://127.0.0.1:8080/`。

其它入口：

```bash
.venv/bin/python -m pytest -q                       # 全量测试
.venv/bin/python -m scope_crawler.cli preview --rules rules.json
.venv/bin/python -m scope_crawler.cli crawl --rules rules.json --publish
.venv/bin/python -m scope_crawler.cli export --state published --out catalog.json
.venv/bin/python -m scope_crawler.worker download --data demo-data   # 独立服务
```

## 核心概念

| 概念 | 含义 |
| --- | --- |
| 站点边界 | `site_root` 的 scheme/host/port + 路径前缀；`..`、`%2e%2e`、`%2f`、跨主机、跨端口一律拒绝 |
| 栏目 | 一个路径前缀（含别名）及启停状态；最长前缀优先，嵌套子栏目可预测 |
| 别名 | 同一栏目的另一种写法；规范化到栏目路径，同一资源只保存一次；不得与任何栏目路径/别名重叠 |
| 跨栏目引用 | 来源栏目 `allow_refs_to` 显式允许 + 全局跳数上限 + 每栏目引用深度 |
| 可达性 | 从启用栏目的种子出发，沿“允许的”链接能到达；仅落在栏目里但不可达 => 不发布 |
| 范围外链接 | 记录在 `links` 表，不请求；控制台“边界外链接”页可查 |

## 规则变化时发生了什么

`Crawler.apply_rules()` 的顺序是固定的：

1. **校验并落库**：新版本写入 `rules_versions`，`parent_id` 指向旧版本，不覆盖历史。
2. **身份重算**：按新规则重算每个 URL 的 `canonical_url`；别名改道产生碰撞时，
   loser 行合并进 winner（`merged_into`），证据与发布历史保留。身份是**派生状态**：
   若后续规则（含撤销）使两行不再碰撞，已合并的行会恢复为独立主行，无需重新抓取。
3. **可达性重算**：从种子出发，基于**已记录的链接**与**已存证据重新解析出的链接**
   做图遍历；这一步不访问网络。
4. **回放发现**：对仍可达的页面重放链接判定，把此前被拒、现在允许的引用补入队列。
5. **发布判断**：可达且范围内 → 重新入库（`ingest`）；否则保留证据、停止发布。
6. **队列协调**：范围内缺证据 → 重新入队；范围外 → `parked`；范围放宽后自动 `reopen`。

**收窄**：证据与文件保留，目录条目转 `withheld` 并记录原因，排队中的下载转为
`parked`。**放宽**：只补齐新纳入的部分——已抓取页面直接复用已有证据，不会重抓整站
（`tests/test_engine.py::test_widening_backfills_only_new_parts` 用请求计数验证）。

可达性遍历对每个页面只保留**首次到达**时的上下文（来源栏目、跨栏目跳数、栏内深度），
遍历是确定性的；同一页面被多条路径指及时，后续路径不会重复扩展它的出链。

## 三个服务与持久队列

```
discover ──enqueue──▶ download ──evidence──▶ ingest
     ▲                    │
     └─── evidence ───────┘
```

* 所有任务在 `queue` 表中，`dedupe` 唯一键保证同一单元只存在一次。
* `claim()` 用 `BEGIN IMMEDIATE` 原子取任务，附租约；`recover_stale()` 回收崩溃
  工作者的租约。
* 状态：`pending → leased → done`，另有 `parked`（范围暂停）、`blocked`（永久失败）、
  `cancelled`。
* 三个服务可分别以进程运行（`python -m scope_crawler.worker <kind>`），共享同一 DB。

## 晚到结果

每次下载先写入 `inflight`（含 `requested_at` 与实际字节），再决定“提交”：

* 正常情况立即提交；
* “暂存”模式下留在 `inflight`，可先改规则再交付。

交付时按**当时生效的规则**判断：范围内且可达 → 入队入库；范围已收窄 → 直接
`withheld`；已被更新的同 URL 证据覆盖 → `superseded`。三种情况下字节都保留在
`evidence` 表与磁盘上。若该 URL 在此期间已被合并到另一行，交付会落到存活的主行上
（证据按整个 cluster 计算，不会被已合并的行吞掉）；同一条暂存结果只提交一次。

## 控制台

| 页面 | 作用 |
| --- | --- |
| 总览 | 计数、栏目表、最近变更、运行队列 |
| 规则 | 编辑 JSON、**预览影响**、发布 |
| 已发布目录 / 已停止发布 | 目录状态与原因；可进入 URL 详情 |
| URL 详情 | 证据（sha256、请求时间、存储路径）、出链判定、发布历史 |
| 边界外链接 | 被记录但从未请求的链接，按 boundary/scope/ref_denied 过滤 |
| 队列 | 三类任务的状态、暂停/失败原因、规则版本 |
| 变更历史 | 发布与撤销留痕，可**撤销一次变更** |
| 晚到结果 | 暂存一次下载 → 改规则 → 交付，观察按当前规则判定 |

JSON 接口：`GET /api/state`、`POST /api/preview`。

## 规则文档

```json
{
  "site_root": "http://127.0.0.1:8000/site/",
  "follow_query": false,
  "max_cross_column_hops": 2,
  "columns": [
    {
      "id": "news",
      "title": "新闻",
      "path": "/site/news/",
      "aliases": ["/site/latest/"],
      "enabled": true,
      "allow_refs_to": ["docs"],
      "max_ref_depth": 1
    },
    {
      "id": "docs",
      "title": "文档",
      "path": "/site/docs/",
      "enabled": true
    }
  ],
  "exclude_patterns": ["/site/news/private/*"]
}
```

校验会在发布前拒绝：越界路径、重叠别名、重复栏目路径、未知引用目标、越界排除模式、
自引用栏目。错误信息直接展示在控制台。若排除模式写在别名路径上，会规范到对应栏目
路径（别名是同一页面的另一种写法，不能只排除其中一种拼写）。

## 礼貌抓取与边界

* 只发 `GET`，固定 User-Agent，超时与最大响应体限制。
* 每个 origin 读取一次 `robots.txt` 并缓存；被禁止的 URL 记为 `robots_disallowed`，
  永不请求。`robots.txt` 返回 5xx 或无法读取时按“全部禁止”处理（RFC 9309），
  不会把读取失败当成许可；失败结果不缓存，下次重试，并记为可重试的
  `robots_unavailable`（不是永久拒绝）。
* 逐跳检查重定向；每一跳在**发出请求之前**同时校验站点边界与 robots.txt，
  任一跳越界即中止并记为 `redirect_out_of_scope`。
* 401/403/407/451 记为 `access_restricted`，不尝试任何绕过。
* 网络错误、HTTP 错误与 `robots_unavailable` 视为**可重试**：任务会短暂退避后重试，
  规则变更也会重新打开已阻塞的任务；永久拒绝（`robots_disallowed`、`access_restricted`）
  不会被规则发布“洗白”。若页面已有存储证据，一次失败的重抓不会撤下目录条目。
* 默认不跟随查询串；需要时用 `follow_query: true`（并注意同一路径不同参数会被视为
  不同页面）。

## 代码结构

```
scope_crawler/
  urls.py      边界与 URL 规范化（纯函数）
  rules.py     规则文档校验、栏目匹配、别名规范化、跨栏目预算
  db.py        SQLite schema 与查询助手
  queue.py     持久队列：原子领取、租约、暂停/重开
  fetch.py     urllib 抓取、robots、逐跳重定向检查
  engine.py    发现/下载/入库、可达性图、规则应用、预览、撤销、晚到结果
  worker.py    三类服务的独立进程入口
  demosite.py  本地演示站点
  app.py       Web 控制台
  cli.py       命令行入口
tests/
  test_rules.py    边界/别名/预算/校验（无网络）
  test_queue.py    队列语义
  test_engine.py   端到端：真实 HTTP、范围迁移、晚到结果、撤销
  test_console.py  控制台路由与完整点击流程
```

## 已知取舍

* 单机 SQLite + WAL；足以演示多进程协作，但不是分布式调度器。
* 一次规则发布由多个步骤组成（版本落库、身份重算、可达性、回放发现、发布判断），
  步骤之间是独立事务；进程内用互斥锁串行化，跨进程依靠 SQLite 写事务。若进程在
  发布中途崩溃，active 版本已切换，但图与目录可能停在中间状态——再次发布或撤销
  一次即可收敛。
* 只解析 `<a>/<area>/<link rel=stylesheet>`，不执行 JavaScript。
* 正文抽取与索引不在范围内：本项目关注“范围判断与迁移”，不是搜索引擎。
