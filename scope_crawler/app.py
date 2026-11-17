"""Web console: preview impact, publish rules, inspect boundaries, undo, and
simulate a late download result.

Run with::

    python -m scope_crawler.app

It starts the demo site and the console together so everything can be tried
locally against real HTTP requests.  Nothing here tries to bypass access
controls; restricted URLs are reported as restricted.
"""

from __future__ import annotations

import json
import os
import threading
from typing import Any

from flask import Flask, jsonify, redirect, render_template_string, request, url_for

from . import rules as rules_mod
from .db import ENTRY_PUBLISHED, ENTRY_WITHHELD, Store
from .demosite import Site
from .engine import (
    HOLD_DOWNLOAD_FOR,
    HOLD_NEXT_DOWNLOAD,
    SCOPE_REASON_TEXT,
    Crawler,
    effective_reason,
)
from .fetch import HttpFetcher
from .queue import Queue

BASE_CSS = """
:root { color-scheme: light dark; }
body { font-family: ui-sans-serif, system-ui, -apple-system, "Segoe UI", sans-serif;
       margin: 0; background: #f6f7f9; color: #111; }
@media (prefers-color-scheme: dark) { body { background:#111417; color:#e6e6e6; } }
header { background:#1e293b; color:#fff; padding:14px 22px; display:flex; gap:22px; align-items:center; }
header a { color:#cbd5e1; text-decoration:none; font-size:14px; }
header a:hover { color:#fff; }
header .brand { font-weight:650; color:#fff; font-size:15px; margin-right:8px; }
main { max-width: 1180px; margin: 24px auto; padding: 0 22px 60px; }
h1 { font-size: 20px; margin: 0 0 4px; }
h2 { font-size: 16px; margin: 26px 0 10px; }
.muted { opacity:.7; font-size: 13px; }
.card { background:#fff; border:1px solid #e2e8f0; border-radius:10px;
        padding:16px 18px; margin:12px 0; }
@media (prefers-color-scheme: dark) { .card { background:#181c20; border-color:#2b3238; } }
.stats { display:flex; flex-wrap:wrap; gap:14px; }
.stat { background:#eef2ff; border-radius:8px; padding:10px 14px; min-width:110px; }
@media (prefers-color-scheme: dark) { .stat { background:#1f2937; } }
.stat b { display:block; font-size:22px; }
table { width:100%; border-collapse: collapse; font-size: 13px; }
th, td { text-align:left; padding:7px 9px; border-bottom:1px solid #e5e7eb; vertical-align:top; }
@media (prefers-color-scheme: dark) { th,td { border-color:#2b3238; } }
th { font-weight:600; opacity:.8; }
code, .mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size:12px; }
.pill { display:inline-block; border-radius:999px; padding:2px 9px; font-size:11.5px; }
.ok { background:#dcfce7; color:#166534; }
.no { background:#fee2e2; color:#991b1b; }
.mid { background:#fef3c7; color:#92400e; }
form.inline { display:inline; }
textarea { width:100%; min-height: 340px; font-family:ui-monospace, monospace; font-size:12.5px;
           border-radius:8px; border:1px solid #cbd5e1; padding:10px; }
button { background:#2563eb; color:#fff; border:0; border-radius:8px; padding:9px 14px;
         font-size:13.5px; cursor:pointer; }
button.secondary { background:#475569; }
button.warn { background:#b91c1c; }
button:disabled { opacity:.5; cursor:not-allowed; }
select, input[type=text] { padding:7px 9px; border-radius:7px; border:1px solid #cbd5e1;
       font-size:13px; }
.error { background:#fee2e2; color:#7f1d1d; border-radius:8px; padding:10px 14px; }
.notice { background:#ecfdf5; color:#065f46; border-radius:8px; padding:10px 14px; }
details { margin-top:6px; }
summary { cursor:pointer; font-size:13px; opacity:.8; }
"""

LAYOUT = """
<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">
<title>{{ title }} · 范围采集控制台</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>{{ css|safe }}</style></head><body>
<header>
  <span class="brand">范围采集控制台</span>
  <a href="{{ url_for('index') }}">总览</a>
  <a href="{{ url_for('rules_page') }}">规则</a>
  <a href="{{ url_for('catalog') }}">已发布目录</a>
  <a href="{{ url_for('boundaries') }}">边界外链接</a>
  <a href="{{ url_for('queue_page') }}">队列</a>
  <a href="{{ url_for('changes_page') }}">变更历史</a>
  <a href="{{ url_for('late_page') }}">晚到结果</a>
</header>
<main>
{% if error %}<div class="error">{{ error }}</div>{% endif %}
{% if notice %}<div class="notice">{{ notice }}</div>{% endif %}
{{ body|safe }}
</main></body></html>
"""


def page(title: str, body: str, *, error: str | None = None, notice: str | None = None) -> str:
    return render_template_string(
        LAYOUT, title=title, body=body, css=BASE_CSS, error=error, notice=notice
    )


def esc(value: Any) -> str:
    from markupsafe import escape

    return str(escape("" if value is None else value))


def status_pill(reason: str) -> str:
    if reason in (rules_mod.OK, ""):
        return f'<span class="pill ok">{esc(reason or "ok")}</span>'
    if reason in ("unreachable", "column_disabled", "excluded", "no_column"):
        return f'<span class="pill mid">{esc(reason)}</span>'
    return f'<span class="pill no">{esc(reason)}</span>'


class Console:
    def __init__(self, data_dir: str, *, site_root: str | None = None, port: int = 8080):
        self.data_dir = os.path.abspath(data_dir)
        os.makedirs(self.data_dir, exist_ok=True)
        self.store = Store(os.path.join(self.data_dir, "console.db"))
        self.store.init()
        self.site = Site(port=0)
        self.site.start()
        self.crawler = Crawler(
            self.store,
            os.path.join(self.data_dir, "storage"),
            fetcher=HttpFetcher(timeout=6.0, user_agent="ScopeCrawlerDemo/0.1"),
            worker_id="console",
        )
        self.lock = threading.RLock()
        self._bootstrap()
        self.port = port

    # -- bootstrap -----------------------------------------------------
    def _bootstrap(self) -> None:
        if self.store.active_rules_version() is None:
            doc = rules_mod.default_document(self.site.site_root)
            doc["columns"].append(
                {
                    "id": "store",
                    "title": "商店",
                    "path": "/site/store/",
                    "aliases": [],
                    "enabled": False,
                    "allow_refs_to": [],
                    "max_ref_depth": 0,
                }
            )
            doc["exclude_patterns"] = ["/site/news/private/*"]
            self.crawler.apply_rules(doc, note="demo bootstrap")
            return
        # The demo site listens on an ephemeral port.  If a previous console
        # run left rules pointing at a now-dead origin, re-point the document
        # (paths and columns unchanged) so the demo works after a restart.
        rules = self.crawler.active_rules()
        if rules is None or rules.boundary.origin() == self.site.base:
            return
        doc = rules.to_dict()
        old_root = rules.boundary.root_path
        doc["site_root"] = self.site.base.rstrip("/") + old_root
        try:
            self.crawler.apply_rules(doc, note="demo re-point site root")
        except (rules_mod.RuleError, ValueError) as exc:  # pragma: no cover - defensive
            print(f"could not re-point demo site ({exc}); publish rules manually")

    # -- helpers -------------------------------------------------------
    def run_pending(self, *, max_tasks: int = 400) -> int:
        with self.lock:
            return self.crawler.run_until_idle(max_tasks=max_tasks)

    def counts(self) -> dict[str, int]:
        store = self.store
        return {
            "published": store.scalar(
                "SELECT COUNT(*) FROM entries WHERE state=?", (ENTRY_PUBLISHED,)
            )
            or 0,
            "withheld": store.scalar(
                "SELECT COUNT(*) FROM entries WHERE state=?", (ENTRY_WITHHELD,)
            )
            or 0,
            "evidence": store.scalar("SELECT COUNT(*) FROM evidence") or 0,
            "urls": store.scalar("SELECT COUNT(*) FROM urls WHERE merged_into IS NULL") or 0,
            "in_scope": store.scalar(
                "SELECT COUNT(*) FROM urls WHERE in_scope=1 AND reachable=1"
            )
            or 0,
            "held": store.scalar("SELECT COUNT(*) FROM inflight WHERE state='held'") or 0,
            "links_denied": store.scalar(
                "SELECT COUNT(*) FROM links WHERE outcome IN ('boundary','scope','ref_denied')"
            )
            or 0,
        }

    def queue_counts(self) -> dict[str, dict[str, int]]:
        return Queue(self.store).counts()


def create_app(console: Console) -> Flask:
    app = Flask(__name__)
    app.config["console"] = console

    def c() -> Console:
        return app.config["console"]

    @app.get("/")
    def index():
        con = c()
        notice = request.args.get("notice")
        counts = con.counts()
        rules = con.crawler.active_rules()
        version = con.store.active_rules_version()
        if rules is None:
            body = "<h1>尚未发布规则</h1><p>前往 <a href='/rules'>规则页</a> 发布第一份规则。</p>"
            return page("总览", body, notice=notice)
        columns = "".join(
            f"<tr><td class='mono'>{esc(col.id)}</td><td>{esc(col.title)}</td>"
            f"<td class='mono'>{esc(col.path)}</td>"
            f"<td>{'启用' if col.enabled else '停用'}</td>"
            f"<td class='mono'>{esc(', '.join(col.aliases) or '—')}</td>"
            f"<td class='mono'>{esc(', '.join(col.allow_refs_to) or '—')}</td>"
            f"<td>{col.max_ref_depth}</td></tr>"
            for col in rules.ordered
        )
        recent = "".join(
            f"<tr><td>#{row['id']}</td><td>{esc(row['kind'])}</td>"
            f"<td>v{esc(row['from_version'])} → v{esc(row['to_version'])}</td>"
            f"<td>{esc(row['note'])}</td><td class='muted'>{esc(row['created_at'])}</td></tr>"
            for row in con.store.all("SELECT * FROM changes ORDER BY id DESC LIMIT 6")
        )
        body = f"""
        <h1>总览</h1>
        <p class="muted">站点 <span class="mono">{esc(rules.boundary)}</span> ·
        当前规则版本 <b>v{version}</b> · 跨栏目跳数上限 {rules.max_cross_column_hops}</p>
        <div class="stats">
          <div class="stat"><b>{counts['published']}</b>已发布</div>
          <div class="stat"><b>{counts['withheld']}</b>已停止发布</div>
          <div class="stat"><b>{counts['in_scope']}</b>范围内 URL</div>
          <div class="stat"><b>{counts['urls']}</b>已发现 URL</div>
          <div class="stat"><b>{counts['evidence']}</b>证据条目</div>
          <div class="stat"><b>{counts['held']}</b>暂存待交付下载</div>
          <div class="stat"><b>{counts['links_denied']}</b>边界外/拒绝链接</div>
        </div>
        <form method="post" action="{url_for('run_worker')}" class="inline" style="margin-top:18px">
          <button type="submit">运行采集队列</button>
        </form>
        <span class="muted">按 发现 → 下载 → 入库 的顺序处理当前队列，直到空闲。</span>
        <h2>栏目</h2>
        <div class="card"><table>
          <tr><th>id</th><th>标题</th><th>路径</th><th>状态</th><th>别名</th><th>允许引用</th><th>引用深度</th></tr>
          {columns}
        </table></div>
        <h2>最近变更</h2>
        <div class="card"><table>
          <tr><th>#</th><th>类型</th><th>版本</th><th>备注</th><th>时间</th></tr>
          {recent or '<tr><td colspan="5" class="muted">暂无</td></tr>'}
        </table></div>
        """
        return page("总览", body)

    @app.get("/rules")
    def rules_page():
        con = c()
        rules = con.crawler.active_rules()
        doc_text = json.dumps(
            rules.to_dict() if rules else rules_mod.default_document(con.site.site_root),
            ensure_ascii=False,
            indent=2,
        )
        version = con.store.active_rules_version()
        body = f"""
        <h1>规则</h1>
        <p class="muted">当前生效版本 v{version}。修改后先「预览影响」，确认后再发布。
        预览不会写入任何状态。</p>
        <form method="post" action="{url_for('preview')}">
          <textarea name="doc" spellcheck="false">{esc(doc_text)}</textarea>
          <div style="margin-top:10px; display:flex; gap:10px; flex-wrap:wrap">
            <button type="submit">预览影响</button>
          </div>
        </form>
        <h2>当前规则说明</h2>
        <div class="card">
          <ul style="margin:0; padding-left:18px; font-size:13px; line-height:1.75">
            <li>每个 URL 归入最长前缀匹配的栏目；不在任何栏目内即 <code>no_column</code>。</li>
            <li>别名会被规范化为栏目路径，同一资源只保存一次；别名不得与任何栏目路径重叠。</li>
            <li>跨栏目链接需在来源栏目的 <code>allow_refs_to</code> 中显式允许，并受跳数上限与引用深度约束。</li>
            <li><code>exclude_patterns</code> 优先于栏目归属。</li>
            <li>规则变更只重新判断，不删除证据；收窄范围停止发布，放宽范围补齐新纳入部分。</li>
          </ul>
        </div>
        """
        return page("规则", body)

    @app.post("/preview")
    def preview():
        con = c()
        raw = request.form.get("doc") or request.values.get("doc") or ""
        try:
            doc = json.loads(raw)
            if not isinstance(doc, dict):
                raise ValueError("规则文档必须是 JSON 对象")
            impact = con.crawler.preview_rules(doc)
        except (json.JSONDecodeError, rules_mod.RuleError, ValueError) as exc:
            return page(
                "规则预览",
                f"<h1>规则预览</h1><p>请回到<a href='{url_for('rules_page')}'>规则页</a>修改。</p>",
                error=f"规则无效：{exc}",
            )
        body = render_impact(impact, raw)
        return page("规则预览", body)

    @app.post("/publish")
    def publish():
        con = c()
        raw = request.form.get("doc") or ""
        note = request.form.get("note") or "console publish"
        try:
            doc = json.loads(raw)
            if not isinstance(doc, dict):
                raise ValueError("规则文档必须是 JSON 对象")
            with con.lock:
                summary = con.crawler.apply_rules(doc, note=note)
            con.run_pending()
            return redirect(
                url_for(
                    "changes_page",
                    notice=(
                        f"已发布 v{summary['version']}：范围内 {summary['proposed_in_scope']}，"
                        f"撤回 {len(summary['withdraw'])}，新增/待补 {len(summary['enqueue_downloads'])}"
                    ),
                )
            )
        except (json.JSONDecodeError, rules_mod.RuleError, ValueError) as exc:
            return page(
                "发布失败",
                f"<h1>发布失败</h1><p>请回到<a href='{url_for('rules_page')}'>规则页</a>修改。</p>",
                error=f"规则无效：{exc}",
            )

    @app.get("/catalog")
    def catalog():
        con = c()
        state = request.args.get("state", ENTRY_PUBLISHED)
        rows = con.store.all(
            "SELECT e.*, u.canonical_url, u.column_id, u.scope_reason, u.fetch_error,"
            " (SELECT COUNT(*) FROM evidence ev WHERE ev.url_id=e.url_id) AS ev_count"
            " FROM entries e JOIN urls u ON u.id=e.url_id WHERE e.state=?"
            " ORDER BY u.canonical_url",
            (state,),
        )
        items = "".join(
            f"<tr><td><a href='{url_for('detail', url_id=row['url_id'])}' class='mono'>"
            f"{esc(row['canonical_url'])}</a><br><span class='muted'>{esc(row['title'])}</span></td>"
            f"<td class='mono'>{esc(row['column_id'])}</td>"
            f"<td>{status_pill(row['reason'])}</td>"
            f"<td>{status_pill(effective_reason(row)) if effective_reason(row) != row['reason'] else '—'}</td>"
            f"<td>{esc(row['ev_count'])}</td>"
            f"<td class='muted'>{esc(row['updated_at'])}</td></tr>"
            for row in rows
        )
        link = (
            f"<a href='{url_for('catalog', state=ENTRY_WITHHELD)}'>查看已停止发布</a>"
            if state == ENTRY_PUBLISHED
            else f"<a href='{url_for('catalog', state=ENTRY_PUBLISHED)}'>查看已发布</a>"
        )
        body = f"""
        <h1>目录 · {'已发布' if state == ENTRY_PUBLISHED else '已停止发布'}</h1>
        <p class="muted">{link}</p>
        <div class="card"><table>
          <tr><th>URL / 标题</th><th>栏目</th><th>目录原因</th><th>抓取限制</th><th>证据</th><th>更新时间</th></tr>
          {items or '<tr><td colspan="6" class="muted">暂无条目</td></tr>'}
        </table></div>
        """
        return page("目录", body)

    @app.get("/url/<int:url_id>")
    def detail(url_id: int):
        con = c()
        row = con.store.url_row(url_id)
        if row is None:
            return page("详情", "<h1>未找到</h1>", error="未知 URL 行")
        evidence = con.store.all(
            "SELECT * FROM evidence WHERE url_id=? ORDER BY requested_at DESC, id DESC",
            (url_id,),
        )
        links = con.store.all(
            "SELECT * FROM links WHERE from_url_id=? ORDER BY id", (url_id,)
        )
        events = con.store.all(
            "SELECT * FROM publication_events WHERE url_id=? ORDER BY id DESC", (url_id,)
        )
        entry = con.store.one("SELECT * FROM entries WHERE url_id=?", (url_id,))
        display_title = (entry["title"] if entry and entry["title"] else row["title"]) or ""
        entry_html = ""
        if entry is not None:
            entry_html = (
                f"<p>目录状态：{status_pill(entry['reason'])} "
                f"<span class='muted'>{esc(entry['state'])} · 更新于 {esc(entry['updated_at'])}</span></p>"
            )
        ev_rows = "".join(
            f"<tr><td>#{esc(ev['id'])}</td><td>{esc(ev['status_code'])}</td>"
            f"<td class='mono'>{esc(ev['content_type'])}</td><td>{esc(ev['size'])}</td>"
            f"<td class='mono'>{esc(ev['sha256'])}</td>"
            f"<td class='muted'>{esc(ev['requested_at'])}</td>"
            f"<td class='mono'>{esc(ev['storage_path'])}</td></tr>"
            for ev in evidence
        )
        link_rows = "".join(
            f"<tr><td class='mono'>{esc(link['raw_url'])}</td>"
            f"<td class='mono'>{esc(link['canonical_url'])}</td>"
            f"<td>{status_pill(link['reason'])}</td>"
            f"<td>{esc(link['outcome'])}</td><td>{esc(link['hits'])}</td></tr>"
            for link in links
        )
        event_rows = "".join(
            f"<tr><td>{esc(ev['action'])}</td><td>{status_pill(ev['reason'])}</td>"
            f"<td>v{esc(ev['rules_version'])}</td><td class='muted'>{esc(ev['created_at'])}</td></tr>"
            for ev in events
        )
        body = f"""
        <h1 class="mono">{esc(row['canonical_url'])}</h1>
        <p class="muted">标题 {esc(display_title or '—')} · 抓取地址
          <span class="mono">{esc(row['fetch_url'])}</span> ·
          栏目 {esc(row['column_id'])} · 范围 {status_pill(row['scope_reason'])} ·
          抓取限制 {status_pill(row['fetch_error']) if row['fetch_error'] else '—'} ·
          可达 {esc(row['reachable'])}</p>
        {entry_html}
        <h2>证据（{len(evidence)}）</h2>
        <div class="card"><table>
          <tr><th>#</th><th>状态</th><th>类型</th><th>字节</th><th>sha256</th><th>请求时间</th><th>存储</th></tr>
          {ev_rows or '<tr><td colspan="7" class="muted">暂无证据</td></tr>'}
        </table></div>
        <h2>出链</h2>
        <div class="card"><table>
          <tr><th>原始链接</th><th>规范 URL</th><th>结果</th><th>判定</th><th>次数</th></tr>
          {link_rows or '<tr><td colspan="5" class="muted">暂无</td></tr>'}
        </table></div>
        <h2>发布历史</h2>
        <div class="card"><table>
          <tr><th>动作</th><th>原因</th><th>规则版本</th><th>时间</th></tr>
          {event_rows or '<tr><td colspan="4" class="muted">暂无</td></tr>'}
        </table></div>
        """
        return page("详情", body)

    @app.get("/boundaries")
    def boundaries():
        con = c()
        outcome = request.args.get("outcome", "all")
        clauses = "l.outcome IN ('boundary','scope','ref_denied')"
        params: tuple = ()
        if outcome != "all":
            clauses = "l.outcome=?"
            params = (outcome,)
        rows = con.store.all(
            f"SELECT l.*, u.canonical_url AS source FROM links l"
            f" LEFT JOIN urls u ON u.id=l.from_url_id WHERE {clauses}"
            f" ORDER BY l.id DESC LIMIT 300",
            params,
        )
        row_html = "".join(
            f"<tr><td class='mono'>{esc(row['source'])}</td>"
            f"<td class='mono'>{esc(row['raw_url'])}</td>"
            f"<td class='mono'>{esc(row['resolved_url'])}</td>"
            f"<td>{status_pill(row['reason'])}<br><span class='muted'>"
            f"{esc(SCOPE_REASON_TEXT.get(row['reason'], row['reason']))}</span></td>"
            f"<td>{esc(row['outcome'])}</td></tr>"
            for row in rows
        )
        filters = " · ".join(
            f"<a href='{url_for('boundaries', outcome=o)}'>{esc(label)}</a>"
            for o, label in (
                ("all", "全部"),
                ("boundary", "边界外"),
                ("scope", "范围外"),
                ("ref_denied", "跨栏目被拒"),
            )
        )
        body = f"""
        <h1>边界外 / 拒绝的链接</h1>
        <p class="muted">这些链接被记录但从未请求。筛选：{filters}</p>
        <div class="card"><table>
          <tr><th>来源页</th><th>原始链接</th><th>解析后</th><th>原因</th><th>结果</th></tr>
          {row_html or '<tr><td colspan="5" class="muted">暂无记录</td></tr>'}
        </table></div>
        """
        return page("边界外链接", body)

    @app.get("/queue")
    def queue_page():
        con = c()
        notice = request.args.get("notice")
        tasks = con.store.all(
            "SELECT q.*, u.canonical_url FROM queue q LEFT JOIN urls u ON u.id=q.url_id"
            " ORDER BY q.id DESC LIMIT 200"
        )
        rows = "".join(
            f"<tr><td>#{esc(t['id'])}</td><td>{esc(t['kind'])}</td>"
            f"<td class='mono'>{esc(t['canonical_url'])}</td>"
            f"<td>{esc(t['state'])}</td>"
            f"<td class='mono'>{esc(t['parked_reason'] or t['last_error'])}</td>"
            f"<td>v{esc(t['rules_version'])}</td><td>{esc(t['attempts'])}</td>"
            f"<td class='muted'>{esc(t['updated_at'])}</td></tr>"
            for t in tasks
        )
        counts = con.queue_counts()
        summary = " ".join(
            f"<span class='pill mid'>{esc(kind)}: "
            + ", ".join(f"{esc(state)}={count}" for state, count in sorted(states.items()))
            + "</span>"
            for kind, states in sorted(counts.items())
        )
        body = f"""
        <h1>持久队列</h1>
        <p class="muted">发现、下载、入库三个服务通过同一张持久队列表协作。
          <code>parked</code> 表示被范围收窄暂停，规则放宽后会重新入队。</p>
        <div class="card">{summary or '空'}</div>
        <form method="post" action="{url_for('run_worker')}" class="inline">
          <input type="hidden" name="next" value="queue">
          <button type="submit">运行队列</button>
        </form>
        <div class="card"><table>
          <tr><th>#</th><th>类型</th><th>URL</th><th>状态</th><th>原因/错误</th>
              <th>规则版本</th><th>尝试</th><th>更新时间</th></tr>
          {rows or '<tr><td colspan="8" class="muted">队列为空</td></tr>'}
        </table></div>
        """
        return page("队列", body, notice=notice)

    @app.get("/changes")
    def changes_page():
        con = c()
        notice = request.args.get("notice")
        rows = con.store.all("SELECT * FROM changes ORDER BY id DESC LIMIT 50")
        items = []
        for row in rows:
            summary = json.loads(row["summary"] or "{}")
            items.append(
                f"<tr><td>#{esc(row['id'])}</td><td>{esc(row['kind'])}</td>"
                f"<td>v{esc(row['from_version'])} → v{esc(row['to_version'])}</td>"
                f"<td>{esc(row['note'])}</td>"
                f"<td>范围内 {esc(summary.get('proposed_in_scope', '—'))} ·"
                f" 撤回 {len(summary.get('withdraw', []))} ·"
                f" 待补 {len(summary.get('enqueue_downloads', []))}</td>"
                f"<td class='muted'>{esc(row['created_at'])}"
                f"{' · 已撤销' if row['reverted_at'] else ''}</td></tr>"
            )
        body = f"""
        <h1>变更历史</h1>
        <p class="muted">每次发布与撤销都留痕；撤销只恢复规则并重新判断，不删除证据。</p>
        <form method="post" action="{url_for('undo')}" class="inline">
          <button type="submit" class="warn">撤销最近一次变更</button>
        </form>
        <div class="card"><table>
          <tr><th>#</th><th>类型</th><th>版本</th><th>备注</th><th>影响</th><th>时间</th></tr>
          {''.join(items) or '<tr><td colspan="6" class="muted">暂无变更</td></tr>'}
        </table></div>
        """
        return page("变更历史", body, notice=notice)

    @app.post("/undo")
    def undo():
        con = c()
        try:
            with con.lock:
                summary = con.crawler.undo_last_change(note="console undo")
            con.run_pending()
            return redirect(
                url_for(
                    "changes_page",
                    notice=f"已撤销变更 #{summary['reverted_change_id']}，恢复为 v{summary['version']}",
                )
            )
        except ValueError as exc:
            return page("撤销失败", "<h1>撤销失败</h1>", error=str(exc))

    @app.get("/late")
    def late_page():
        con = c()
        candidates = con.store.all(
            "SELECT u.id, u.canonical_url, u.column_id, u.fetch_url,"
            " (SELECT COUNT(*) FROM evidence e WHERE e.url_id=u.id) AS ev_count"
            " FROM urls u WHERE u.merged_into IS NULL AND u.kind='page'"
            " ORDER BY u.canonical_url LIMIT 200"
        )
        options = "".join(
            f"<option value='{row['id']}'>{esc(row['canonical_url'])} "
            f"({esc(row['column_id'])})</option>"
            for row in candidates
        )
        held = con.store.all(
            "SELECT i.*, u.canonical_url FROM inflight i LEFT JOIN urls u ON u.id=i.url_id"
            " WHERE i.state='held' ORDER BY i.id"
        )
        held_rows = "".join(
            f"<tr><td>#{esc(h['id'])}</td><td class='mono'>{esc(h['canonical_url'])}</td>"
            f"<td>v{esc(h['rules_version'])}</td>"
            f"<td class='muted'>{esc(h['requested_at'])}</td></tr>"
            for h in held
        )
        body = f"""
        <h1>模拟旧下载结果晚到</h1>
        <p class="muted">流程：先暂存一次下载（请求已完成但结果未提交），再修改并发布规则，
        最后交付晚到结果。交付时会按当前规则判断是否发布，证据始终保留。</p>
        <div class="card">
          <h2 style="margin-top:0">1. 暂存一次下载</h2>
          <form method="post" action="{url_for('late_hold')}">
            <select name="url_id">{options}</select>
            <button type="submit" class="secondary">暂存下载结果</button>
          </form>
          <p class="muted">会重新请求该 URL 但暂不提交；之后的规则变更不会删除这次请求。</p>
        </div>
        <div class="card">
          <h2 style="margin-top:0">2. 修改并发布规则</h2>
          <p class="muted">在<a href="{url_for('rules_page')}">规则页</a>停用相关栏目或移除允许的引用后再发布。</p>
        </div>
        <div class="card">
          <h2 style="margin-top:0">3. 交付晚到结果（{len(held)} 个）</h2>
          <form method="post" action="{url_for('late_deliver')}">
            <button type="submit" class="warn" {'disabled' if not held else ''}>交付暂存结果</button>
          </form>
          <table style="margin-top:12px">
            <tr><th>#</th><th>URL</th><th>暂存时规则版本</th><th>请求时间</th></tr>
            {held_rows or '<tr><td colspan="4" class="muted">暂无暂存结果</td></tr>'}
          </table>
          <p class="muted">交付后可在<a href="{url_for('catalog')}">目录</a>与
          <a href="{url_for('changes_page')}">变更历史</a>看到结果。</p>
        </div>
        """
        return page("晚到结果", body)

    @app.post("/late/hold")
    def late_hold():
        con = c()
        url_id = int(request.form.get("url_id") or 0)
        row = con.store.url_row(url_id)
        if row is None:
            return page("暂存失败", "<h1>暂存失败</h1>", error="未知 URL")
        rules = con.crawler.active_rules()
        if rules is None:
            return page("暂存失败", "<h1>暂存失败</h1>", error="尚未发布规则")
        decision = rules.evaluate(row["fetch_url"])
        if not decision.in_scope:
            return page(
                "暂存失败",
                "<h1>暂存失败</h1>",
                error=f"该 URL 当前不在范围内（{decision.reason}），不会请求它",
            )
        queue = con.crawler.queue
        dedupe = f"download:{url_id}"
        existing = queue.by_dedupe("download", dedupe)
        with con.lock:
            if existing is not None and existing["state"] != "leased":
                queue.reopen(
                    "download",
                    dedupe,
                    url_id=url_id,
                    priority=5,
                    rules_version=rules.version or 0,
                    payload={"url_id": url_id},
                )
            elif existing is None:
                queue.enqueue(
                    "download",
                    dedupe,
                    url_id=url_id,
                    priority=5,
                    rules_version=rules.version or 0,
                    payload={"url_id": url_id},
                )
            else:
                return page(
                    "暂存失败",
                    "<h1>暂存失败</h1>",
                    error="该 URL 的下载任务正被其他 worker 占用，请稍后重试",
                )
            # Process until *this* URL's result has been staged.  The hold is
            # scoped to the chosen URL and cleared on every exit path, so it can
            # never leak onto an unrelated download.
            try:
                for _ in range(200):
                    con.store.set_setting(HOLD_NEXT_DOWNLOAD, "1")
                    con.store.set_setting(HOLD_DOWNLOAD_FOR, row["fetch_url"])
                    task = con.crawler.run_once()
                    if task is None:
                        break
                    if con.store.scalar(
                        "SELECT COUNT(*) FROM inflight WHERE state='held' AND url_id=?",
                        (url_id,),
                    ) > 0:
                        break
            finally:
                con.store.set_setting(HOLD_NEXT_DOWNLOAD, "0")
                con.store.set_setting(HOLD_DOWNLOAD_FOR, "")
        return redirect(url_for("late_page", held="1"))

    @app.post("/late/deliver")
    def late_deliver():
        con = c()
        with con.lock:
            delivered = con.crawler.deliver_held()
            con.run_pending()
        if not delivered:
            return page("交付", "<h1>没有暂存的下载结果</h1>", error=None)
        rows = "".join(
            f"<tr><td>{esc(d['url'])}</td><td>{esc(d['outcome'])}</td>"
            f"<td class='muted'>{esc(d['requested_at'])}</td></tr>"
            for d in delivered
        )
        body = f"""
        <h1>已交付晚到结果</h1>
        <p class="muted">结果按交付时的生效规则判断；证据已入库。</p>
        <div class="card"><table>
          <tr><th>URL</th><th>交付结果</th><th>请求时间</th></tr>{rows}
        </table></div>
        <p><a href="{url_for('catalog')}">查看目录</a> ·
           <a href="{url_for('late_page')}">返回</a></p>
        """
        return page("已交付", body)

    @app.post("/run")
    def run_worker():
        con = c()
        processed = con.run_pending()
        target = request.form.get("next") or "index"
        if target == "queue":
            return redirect(url_for("queue_page", notice=f"已处理 {processed} 个队列任务"))
        return redirect(url_for("index", notice=f"已处理 {processed} 个队列任务"))

    # JSON API -------------------------------------------------------
    @app.get("/api/state")
    def api_state():
        con = c()
        rules = con.crawler.active_rules()
        return jsonify(
            {
                "version": con.store.active_rules_version(),
                "rules": rules.to_dict() if rules else None,
                "counts": con.counts(),
                "queue": con.queue_counts(),
            }
        )

    @app.post("/api/preview")
    def api_preview():
        con = c()
        payload = request.get_json(force=True, silent=True) or {}
        doc = payload.get("doc")
        if not isinstance(doc, dict):
            return jsonify({"ok": False, "error": "doc must be a JSON object"}), 400
        try:
            return jsonify({"ok": True, "impact": con.crawler.preview_rules(doc)})
        except (rules_mod.RuleError, ValueError) as exc:
            return jsonify({"ok": False, "error": str(exc)}), 400

    return app


def render_impact(impact: dict[str, Any], raw_doc: str) -> str:
    def table(title: str, items: list[dict[str, Any]], cols: list[tuple[str, str]]) -> str:
        if not items:
            return f"<h2>{esc(title)}（0）</h2><p class='muted'>无</p>"
        header = "".join(f"<th>{esc(label)}</th>" for _key, label in cols)
        rows = []
        for item in items:
            cells = []
            for key, _label in cols:
                value = item.get(key)
                cls = "mono" if isinstance(value, str) and value.startswith("http") else ""
                cells.append(f"<td class='{cls}'>{esc(value if value is not None else '—')}</td>")
            rows.append("<tr>" + "".join(cells) + "</tr>")
        return (
            f"<h2>{esc(title)}（{len(items)}）</h2><div class='card'><table>"
            f"<tr>{header}</tr>{''.join(rows)}</table></div>"
        )

    diff = impact.get("diff", {})
    changed = "、".join(
        f"{kind}: {', '.join(ids)}" for kind, ids in diff.items() if ids
    ) or "无结构变化"
    actions = "".join(
        f"<span class='pill mid'>{esc(kind)}: {esc(', '.join(ids))}</span> "
        for kind, ids in diff.items()
        if ids
    )
    body = f"""
    <h1>规则预览</h1>
    <p class="muted">范围 {impact['current_in_scope']} → {impact['proposed_in_scope']}
      （不变 {impact['unchanged']}）。此预览尚未写入数据库。</p>
    <div class="card"><b>结构变化</b>：{esc(changed)}<br>{actions}</div>
    {table("将发布 / 补齐抓取", impact.get("publish", []),
           [("url", "URL"), ("column_id", "栏目"), ("action", "动作"), ("reason", "原因")])}
    {table("将停止发布（保留证据）", impact.get("withdraw", []),
           [("url", "URL"), ("column_id", "栏目"), ("reason", "原因")])}
    {table("将暂停的下载任务", impact.get("park_downloads", []),
           [("task_id", "任务"), ("url", "URL"), ("state", "当前状态"), ("reason", "原因")])}
    {table("将新入队下载", impact.get("enqueue_downloads", []),
           [("url", "URL"), ("column_id", "栏目"), ("action", "动作")])}
    <p class="muted">被拒绝的引用链接（跨栏目/边界）共 {impact['denied_references']} 个。</p>
    <form method="post" action="{url_for('publish')}">
      <input type="hidden" name="doc" value="{esc(json.dumps(json.loads(raw_doc), ensure_ascii=False))}">
      <input type="hidden" name="note" value="console publish">
      <button type="submit">发布这份规则</button>
    </form>
    <p><a href="{url_for('rules_page')}">返回继续编辑</a></p>
    """
    return body


def main() -> None:
    data_dir = os.environ.get("SCOPE_DATA", "demo-data")
    port = int(os.environ.get("PORT", "8080"))
    console = Console(data_dir, port=port)
    app = create_app(console)
    print(f"demo site:  {console.site.base}/site/")
    print(f"console:    http://127.0.0.1:{port}/")
    app.run(host="127.0.0.1", port=port, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()
