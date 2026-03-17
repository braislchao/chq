"""HTML output formatter for chq reports."""

from __future__ import annotations

import html
import re
import sys
from datetime import datetime, timedelta, timezone

CATEGORY_ORDER = [
    "cost_attribution",
    "top_n",
    "anti_patterns",
    "cluster_health",
    "anomalies",
]

CATEGORY_TITLES = {
    "cost_attribution": "Cost Attribution",
    "top_n": "Top N — Most Expensive Queries",
    "anti_patterns": "Anti-Patterns Detected",
    "cluster_health": "Cluster Health — Merges, Parts & Ingestion",
    "anomalies": "Anomalies — Week-over-Week Changes",
}

RECOMMENDATIONS: dict[tuple[str, str], str] = {
    ("top_n", "by_duration"): (
        "Review the slowest queries for missing WHERE clauses, inefficient JOINs, "
        "or suboptimal ORDER BY keys. Consider materialized views for repeated "
        "aggregations or pre-computed lookups."
    ),
    ("top_n", "by_memory"): (
        "High-memory queries often involve large JOINs, ORDER BY on unbounded sets, "
        "or GROUP BY with high cardinality. Consider setting per-query "
        "<code>max_memory_usage</code> limits and restructuring to use streaming aggregation."
    ),
    ("top_n", "by_read_bytes"): (
        "Total bytes scanned is the primary driver of ClickHouse Cloud compute cost. "
        "Project only needed columns, add partition-pruning predicates, and consider "
        "materialized views to pre-aggregate hot paths."
    ),
    ("top_n", "by_weighted_cost"): (
        "Weighted cost highlights queries that are moderately expensive but run very often. "
        "Caching, deduplication, or query batching at the application layer can reduce "
        "cumulative load significantly."
    ),
    ("anomalies", "wow_duration_regressions"): (
        "Regressions often correlate with schema changes, new data patterns, or increased "
        "data volume. Compare the query plan from both periods to identify the root cause."
    ),
    ("anomalies", "new_expensive_patterns"): (
        "New expensive patterns may come from recently deployed features. Engage the owning "
        "team early — it is much easier to optimize a query before it becomes entrenched."
    ),
    ("cost_attribution", "by_user"): (
        "Use this breakdown for chargeback and capacity planning. Consider per-user "
        "quotas (<code>max_concurrent_queries</code>, <code>max_memory_usage</code>) "
        "for the heaviest consumers."
    ),
    ("anti_patterns", "full_scans"): (
        "A high scan ratio means the query reads far more rows than it returns. "
        "Add a WHERE clause aligned with the table's primary key or partition key, "
        "or create a materialized view that pre-filters the data."
    ),
    ("anti_patterns", "select_star_wide_tables"): (
        "Replace <code>SELECT *</code> with an explicit column list. ClickHouse's "
        "columnar storage means unused columns still cost disk I/O and network bandwidth."
    ),
    ("anti_patterns", "missing_partition_filter"): (
        "Add a predicate on the table's partition key (typically a date column) to "
        "enable partition pruning. This can reduce the number of parts scanned by "
        "orders of magnitude."
    ),
    ("anti_patterns", "unbounded_results"): (
        "Add a <code>LIMIT</code> clause or aggregate results before returning. "
        "Unbounded result sets consume excessive server and client memory, and often "
        "indicate a missing pagination layer."
    ),
    ("anti_patterns", "repeated_identical"): (
        "Excessive repetition of the same SELECT/query pattern suggests a missing cache "
        "layer, a polling loop with too-short intervals, or an application retry storm. "
        "Consider application-level caching or increasing poll intervals. "
        "Note: INSERT queries are excluded from this check — streaming ingestion tools "
        "(e.g. Kafka Connect) repeat the same INSERT pattern by design."
    ),
    ("anti_patterns", "small_insert_batches"): (
        "ClickHouse is optimized for bulk ingestion. Batch inserts to at least "
        "10,000–100,000 rows per INSERT, use <code>async_insert=1</code> to let the "
        "server coalesce small writes, or place a Buffer table in front of the target."
    ),
    ("cluster_health", "insert_part_fragmentation"): (
        "Tables with many tiny parts experience excessive merge overhead and risk hitting "
        "the 'too many parts' threshold. Increase batch sizes at the source, enable "
        "<code>async_insert</code>, or use Buffer tables."
    ),
    ("cluster_health", "insert_duration_by_engine"): (
        "High insert latency typically indicates write contention or merges blocking new "
        "parts. Monitor merge pressure alongside this check. Consider switching "
        "high-volume tables to SharedMergeTree if not already."
    ),
    ("cluster_health", "merge_pressure"): (
        "High merge pressure (SLOW/PAINFUL) means the cluster spends significant "
        "resources on background merges. Reduce part creation rate (batch bigger), "
        "review partition granularity, and consider increasing merge-related settings."
    ),
    ("cluster_health", "partition_health"): (
        "Oversized or fragmented partitions lead to slow queries and merge inefficiency. "
        "Re-evaluate the partition key — aim for partitions that are neither too large "
        "(>10 GB) nor too many (>1,000 active)."
    ),
}

CROSS_REFERENCES: dict[tuple[str, str], list[tuple[str, str]]] = {
    ("anti_patterns", "small_insert_batches"): [
        ("cluster_health", "insert_part_fragmentation"),
        ("cluster_health", "merge_pressure"),
        ("anti_patterns", "repeated_identical"),
    ],
    ("cluster_health", "insert_part_fragmentation"): [
        ("anti_patterns", "small_insert_batches"),
        ("cluster_health", "merge_pressure"),
    ],
    ("cluster_health", "merge_pressure"): [
        ("cluster_health", "insert_part_fragmentation"),
        ("anti_patterns", "small_insert_batches"),
    ],
    ("anti_patterns", "repeated_identical"): [
        ("anti_patterns", "small_insert_batches"),
    ],
    ("top_n", "by_read_bytes"): [
        ("cost_attribution", "by_user"),
    ],
    ("top_n", "by_weighted_cost"): [
        ("anti_patterns", "repeated_identical"),
    ],
}

SEVERITY_MAP = {
    "🟥 VERY SMALL": ("critical", "Very Small"),
    "🟧 SMALL": ("warning", "Small"),
    "🟩 NORMAL": ("ok", "Normal"),
    "🟦 LARGE": ("info", "Large"),
    "🟪 X LARGE": ("info", "X-Large"),
    "🟫 XX LARGE": ("info", "XX-Large"),
    "🟢 FAST": ("ok", "Fast"),
    "🟡 MODERATE": ("warning", "Moderate"),
    "🟠 SLOW": ("warning", "Slow"),
    "🔴 PAINFUL": ("critical", "Painful"),
    "🔵 FAST": ("ok", "Fast"),
    "🟣 SEVERE (30-120 min)": ("critical", "Severe"),
    "🛑 CRITICAL (>= 2 h)": ("critical", "Critical"),
    "🔴 PAINFUL (2-30 min)": ("critical", "Painful"),
    "⚪ LIGHT LOAD": ("ok", "Light Load"),
    "🟡 MEDIUM LOAD": ("warning", "Medium Load"),
    "🟠 HEAVY LOAD": ("warning", "Heavy Load"),
    "🔴 EXTREME LOAD": ("critical", "Extreme Load"),
    "🟢 MODERATE": ("ok", "Moderate"),
    "🟡 SLOW": ("warning", "Slow"),
}


def _esc(value: object) -> str:
    return html.escape(str(value))


def _section_id(category: str, name: str) -> str:
    return f"{category}--{name}"


def _col_idx(columns: list, name: str) -> int | None:
    try:
        return columns.index(name)
    except ValueError:
        return None


def _extract_query_summary(text: str) -> str:
    """Extract a meaningful short summary from a query, emphasising the target table."""
    text = text.strip()

    m = re.match(r"(INSERT\s+INTO\s+\S+)", text, re.IGNORECASE)
    if m:
        return m.group(1) + " ..."

    m = re.search(r"\bFROM\s+(\S+)", text, re.IGNORECASE)
    if m:
        table = m.group(1).strip("(")
        first_line = text.split("\n")[0][:40]
        return f"{first_line}... FROM {table}"

    short = text[:80].replace("\n", " ")
    if len(text) > 80:
        short += "..."
    return short


def _severity_badge(value: str) -> str:
    """Convert emoji-based severity indicators to CSS badge spans."""
    text = str(value)
    for emoji_label, (css_class, clean_label) in SEVERITY_MAP.items():
        if emoji_label in text:
            return f'<span class="badge badge-{css_class}">{_esc(clean_label)}</span>'
    return _esc(value)


def _has_severity(value: str) -> bool:
    text = str(value)
    return any(k in text for k in SEVERITY_MAP)


def _compute_health_score(results: list) -> tuple[str, str, int]:
    """Derive an overall health label, CSS class, and 0-100 score from results."""
    total_weight = 0
    issue_weight = 0

    severity_weights = {
        "anomalies": 3,
        "anti_patterns": 2,
        "cluster_health": 2,
        "top_n": 1,
        "cost_attribution": 1,
    }

    for qr in results:
        w = severity_weights.get(qr.category, 1)
        total_weight += w
        if not qr.rows:
            continue
        has_critical = any(any(k in str(val) for k in SEVERITY_MAP if SEVERITY_MAP[k][0] == "critical") for row in qr.rows for val in row)
        if has_critical:
            issue_weight += w
        elif qr.category in ("anomalies", "anti_patterns"):
            issue_weight += w * 0.6
        else:
            issue_weight += w * 0.3

    if total_weight == 0:
        return "Unknown", "info", 50

    ratio = issue_weight / total_weight
    score = max(0, min(100, int(100 - ratio * 80)))

    if score >= 75:
        return "Healthy", "ok", score
    if score >= 45:
        return "Needs Attention", "warning", score
    return "Critical", "critical", score


def _generate_executive_summary(results: list, config) -> str:
    """Build an HTML executive summary section from the query results."""
    findings: list[str] = []

    by_key = {(qr.category, qr.name): qr for qr in results}

    # Cost attribution
    cost_qr = by_key.get(("cost_attribution", "by_user"))
    if cost_qr and cost_qr.rows:
        user_col = _col_idx(cost_qr.columns, "user")
        read_col = _col_idx(cost_qr.columns, "total_read")
        if user_col is not None and read_col is not None:
            top = cost_qr.rows[0]
            findings.append(
                f"Top resource consumer is <strong>{_esc(top[user_col])}</strong> reading {_esc(top[read_col])} in the last {config.lookback_days} days."
            )

    # Insert fragmentation
    frag_qr = by_key.get(("cluster_health", "insert_part_fragmentation"))
    if frag_qr and frag_qr.rows:
        critical_tables = sum(1 for row in frag_qr.rows if any("VERY SMALL" in str(v) for v in row))
        if critical_tables > 0:
            findings.append(f"<strong>{critical_tables} table(s)</strong> have critically small insert batch sizes, creating excessive part fragmentation.")

    # Small insert batches
    small_qr = by_key.get(("anti_patterns", "small_insert_batches"))
    if small_qr and small_qr.rows:
        findings.append(f"<strong>{len(small_qr.rows)} query pattern(s)</strong> are inserting with very small batch sizes (&lt;{config.min_batch_size} rows).")

    # Anomalies
    reg_qr = by_key.get(("anomalies", "wow_duration_regressions"))
    new_qr = by_key.get(("anomalies", "new_expensive_patterns"))

    if reg_qr is not None:
        if reg_qr.rows:
            findings.append(
                f"<strong>{len(reg_qr.rows)} query pattern(s)</strong> show week-over-week latency regressions above {config.regression_threshold_pct}%."
            )
        else:
            findings.append("No week-over-week latency regressions detected.")

    if new_qr and new_qr.rows:
        findings.append(f"<strong>{len(new_qr.rows)} new expensive query pattern(s)</strong> appeared this week.")

    # Aggregate anti-pattern count
    anti_count = sum(len(qr.rows) for qr in results if qr.category == "anti_patterns" and qr.rows)
    if anti_count > 0:
        findings.append(f"<strong>{anti_count} anti-pattern instance(s)</strong> detected across all checks.")

    label, css_class, score = _compute_health_score(results)

    if not findings:
        findings.append("No significant issues detected. The cluster looks healthy.")

    bullets = "\n".join(f"    <li>{f}</li>" for f in findings)

    return f"""\
<div class="executive-summary">
  <div class="health-score">
    <div class="score-circle score-{css_class}">{score}</div>
    <div class="score-label">
      <span class="score-title">Health Score</span>
      <span class="badge badge-{css_class}">{_esc(label)}</span>
    </div>
  </div>
  <div class="summary-findings">
    <h3>Key Findings</h3>
    <ul>
{bullets}
    </ul>
  </div>
</div>"""


def _generate_toc(grouped: dict[str, list], category_order: list[str]) -> str:
    items: list[str] = []
    items.append('<li><a href="#executive-summary">Executive Summary</a></li>')
    for category in category_order:
        if category not in grouped:
            continue
        title = CATEGORY_TITLES.get(category, category.replace("_", " ").title())
        cat_id = f"cat-{category}"
        items.append(f'<li><a href="#{_esc(cat_id)}">{_esc(title)}</a><ul>')
        for qr in grouped[category]:
            sec_id = _section_id(category, qr.name)
            human_name = qr.name.replace("_", " ").title()
            badge = ""
            if not qr.rows:
                badge = ' <span class="toc-empty">0</span>'
            else:
                badge = f' <span class="toc-count">{len(qr.rows)}</span>'
            items.append(f'    <li><a href="#{_esc(sec_id)}">{_esc(human_name)}{badge}</a></li>')
        items.append("</ul></li>")
    return "\n".join(items)


def output_html(results: list, config) -> None:
    """Render *results* as a self-contained HTML file."""
    now = datetime.now(tz=timezone.utc)
    start = now - timedelta(days=config.lookback_days)
    date_range = f"{start:%Y-%m-%d} to {now:%Y-%m-%d}"
    output_path = getattr(config, "output_path", None)

    cluster_label = getattr(config, "host", "") or "<unknown host>"
    port = getattr(config, "port", None)
    if port:
        cluster_label = f"{cluster_label}:{port}"
    source_table = getattr(config, "table", "system.query_log")

    only_categories = getattr(config, "only_categories", None)
    only_label = ", ".join(only_categories) if only_categories else "all"

    include_internal = bool(getattr(config, "include_internal", False))
    exclude_users = getattr(config, "exclude_users", None) or []
    exclude_label = ", ".join(exclude_users) if exclude_users else "—"

    grouped: dict[str, list] = {}
    for qr in results:
        grouped.setdefault(qr.category, []).append(qr)

    category_order = [c for c in CATEGORY_ORDER if c in grouped]
    for c in grouped:
        if c not in category_order:
            category_order.append(c)

    summary_html = _generate_executive_summary(results, config)
    toc_html = _generate_toc(grouped, category_order)

    checks_html: list[str] = []
    for category in category_order:
        checks = grouped[category]
        title = CATEGORY_TITLES.get(category, category.replace("_", " ").title())
        cat_id = f"cat-{category}"
        checks_html.append(f'<h2 id="{_esc(cat_id)}">{_esc(title)}</h2>')

        for qr in checks:
            human_name = qr.name.replace("_", " ").title()
            sec_id = _section_id(category, qr.name)
            checks_html.append(f'<h3 id="{_esc(sec_id)}">{_esc(human_name)}</h3>')

            if not qr.rows:
                checks_html.append('<p class="empty">No results — this check found no issues in the analyzed period.</p>')
                rec = RECOMMENDATIONS.get((category, qr.name))
                if rec:
                    checks_html.append(f'<div class="recommendation"><strong>What to look for:</strong> {rec}</div>')
                continue

            checks_html.append(f'<p class="result-count">{len(qr.rows)} result(s)</p>')

            checks_html.append('<div class="table-wrap"><table>')
            query_col_idx = None
            checks_html.append("<thead><tr>")
            for i, col in enumerate(qr.columns):
                if col == "query_text":
                    query_col_idx = i
                checks_html.append(f"<th>{_esc(col)}</th>")
            checks_html.append("</tr></thead>")
            checks_html.append("<tbody>")
            for row in qr.rows:
                checks_html.append("<tr>")
                for i, val in enumerate(row):
                    if i == query_col_idx:
                        text = str(val)
                        short = _extract_query_summary(text)
                        checks_html.append(f'<td class="query-cell"><details><summary>{_esc(short)}</summary><pre>{_esc(text)}</pre></details></td>')
                    elif _has_severity(str(val)):
                        checks_html.append(f"<td>{_severity_badge(str(val))}</td>")
                    else:
                        checks_html.append(f"<td>{_esc(val)}</td>")
                checks_html.append("</tr>")
            checks_html.append("</tbody></table></div>")

            rec = RECOMMENDATIONS.get((category, qr.name))
            if rec:
                checks_html.append(f'<div class="recommendation"><strong>Recommendation:</strong> {rec}</div>')

            xrefs = CROSS_REFERENCES.get((category, qr.name))
            if xrefs:
                links = []
                for ref_cat, ref_name in xrefs:
                    ref_id = _section_id(ref_cat, ref_name)
                    ref_human = ref_name.replace("_", " ").title()
                    links.append(f'<a href="#{_esc(ref_id)}">{_esc(ref_human)}</a>')
                checks_html.append(f'<div class="cross-ref">See also: {" · ".join(links)}</div>')

    body = "\n".join(checks_html)

    filters_html = f"""\
<div class="filters">
  <div class="filters-grid">
    <div><span class="k">Cluster</span><span class="v">{_esc(cluster_label)}</span></div>
    <div><span class="k">Source</span><span class="v"><code>{_esc(source_table)}</code></span></div>
    <div><span class="k">Lookback</span><span class="v">{_esc(config.lookback_days)} days</span></div>
    <div><span class="k">Top N</span><span class="v">{_esc(config.top_n)}</span></div>
    <div><span class="k">Categories</span><span class="v">{_esc(only_label)}</span></div>
    <div><span class="k">Internal users</span><span class="v">{_esc("included" if include_internal else "excluded")}</span></div>
    <div><span class="k">Extra excluded users</span><span class="v">{_esc(exclude_label)}</span></div>
    <div><span class="k">Output</span><span class="v">{_esc(output_path or "stdout")}</span></div>
  </div>
</div>"""
    page = f"""\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>chq report</title>
<style>
{_CSS}
</style>
</head>
<body>
<nav class="toc">
  <div class="toc-title">chq report</div>
  <ul>
{toc_html}
  </ul>
</nav>
<main>
<h1>ClickHouse Query Performance Report</h1>
<p class="meta">Date range: {_esc(date_range)} &middot; \
Generated {_esc(now.strftime("%Y-%m-%d %H:%M UTC"))}</p>
{filters_html}
<section id="executive-summary">
<h2>Executive Summary</h2>
{summary_html}
</section>
{body}
</main>
<script>
{_JS}
</script>
</body>
</html>"""

    if output_path:
        with open(output_path, "w", encoding="utf-8") as fh:
            fh.write(page)
    else:
        sys.stdout.write(page)


# ---------------------------------------------------------------------------
# Inline CSS & JS (kept as module-level constants to keep output_html readable)
# ---------------------------------------------------------------------------

_CSS = """\
* { box-sizing: border-box; }
body {
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  margin: 0; padding: 0; background: #fafafa; color: #1a1a1a; display: flex;
}

/* --- Sidebar TOC --- */
nav.toc {
  position: sticky; top: 0; height: 100vh; width: 260px; min-width: 260px;
  overflow-y: auto; background: #fff; border-right: 1px solid #e5e7eb;
  padding: 16px 12px; font-size: 13px;
}
nav.toc .toc-title {
  font-weight: 700; font-size: 15px; margin-bottom: 12px; color: #2563eb;
}
nav.toc ul { list-style: none; padding-left: 0; margin: 0; }
nav.toc ul ul { padding-left: 16px; }
nav.toc li { margin: 4px 0; }
nav.toc a {
  color: #374151; text-decoration: none; display: flex; align-items: center;
  padding: 3px 8px; border-radius: 4px; gap: 6px;
}
nav.toc a:hover { background: #eff6ff; color: #2563eb; }
.toc-empty {
  font-size: 10px; background: #f3f4f6; color: #9ca3af;
  padding: 1px 6px; border-radius: 8px;
}
.toc-count {
  font-size: 10px; background: #dbeafe; color: #1e40af;
  padding: 1px 6px; border-radius: 8px;
}

/* --- Main content --- */
main { flex: 1; max-width: 1100px; margin: 0 auto; padding: 24px 32px; }
h1 { border-bottom: 2px solid #2563eb; padding-bottom: 8px; }
h2 {
  color: #2563eb; margin-top: 40px; padding-top: 16px;
  border-top: 1px solid #e5e7eb;
}
h3 { color: #374151; margin-top: 24px; }
.meta { color: #6b7280; margin-bottom: 24px; }
.empty { color: #9ca3af; font-style: italic; }
.result-count { color: #6b7280; font-size: 13px; margin: 4px 0 0; }

/* --- Run context (filters) --- */
.filters {
  background: #fff; border: 1px solid #e5e7eb; border-radius: 8px;
  padding: 14px 16px; margin: 12px 0 18px;
}
.filters-grid {
  display: grid; grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 10px 18px;
}
.filters .k {
  display: inline-block; min-width: 140px; color: #6b7280; font-size: 12px;
  text-transform: uppercase; letter-spacing: 0.03em;
}
.filters .v { color: #111827; font-size: 13px; }
.filters code { font-size: 12px; }

/* --- Executive Summary --- */
.executive-summary {
  display: flex; gap: 32px; align-items: flex-start; padding: 20px;
  background: #fff; border: 1px solid #e5e7eb; border-radius: 8px; margin: 16px 0;
}
.health-score {
  display: flex; flex-direction: column; align-items: center; gap: 8px;
  min-width: 100px;
}
.score-circle {
  width: 80px; height: 80px; border-radius: 50%; display: flex;
  align-items: center; justify-content: center; font-size: 28px;
  font-weight: 700; color: #fff;
}
.score-ok { background: #16a34a; }
.score-warning { background: #f59e0b; }
.score-critical { background: #dc2626; }
.score-info { background: #6b7280; }
.score-label { text-align: center; }
.score-title { display: block; font-size: 12px; color: #6b7280; margin-bottom: 4px; }
.summary-findings { flex: 1; }
.summary-findings h3 { margin-top: 0; }
.summary-findings ul { margin: 8px 0; padding-left: 20px; }
.summary-findings li { margin: 6px 0; line-height: 1.5; }

/* --- Severity badges --- */
.badge {
  display: inline-block; padding: 2px 10px; border-radius: 12px;
  font-size: 12px; font-weight: 600; white-space: nowrap;
}
.badge-ok { background: #dcfce7; color: #166534; }
.badge-warning { background: #fef3c7; color: #92400e; }
.badge-critical { background: #fee2e2; color: #991b1b; }
.badge-info { background: #dbeafe; color: #1e40af; }

/* --- Recommendations & cross-refs --- */
.recommendation {
  background: #eff6ff; border-left: 3px solid #2563eb;
  padding: 10px 14px; margin: 12px 0; font-size: 13px;
  border-radius: 0 4px 4px 0; line-height: 1.5;
}
.cross-ref { font-size: 13px; color: #6b7280; margin: 8px 0 20px; }
.cross-ref a { color: #2563eb; text-decoration: none; }
.cross-ref a:hover { text-decoration: underline; }

/* --- Tables --- */
.table-wrap { overflow-x: auto; margin: 12px 0 24px; }
table { border-collapse: collapse; font-size: 14px; width: 100%; }
th, td {
  text-align: left; padding: 8px 12px; border: 1px solid #e5e7eb;
  white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 300px;
}
td.query-cell { white-space: normal; max-width: 500px; min-width: 200px; }
td.query-cell details summary {
  cursor: pointer; white-space: nowrap; overflow: hidden;
  text-overflow: ellipsis; display: block;
}
td.query-cell details[open] { white-space: normal; }
td.query-cell details[open] summary { margin-bottom: 8px; white-space: nowrap; }
td.query-cell pre {
  white-space: pre-wrap; word-break: break-word; font-size: 13px; margin: 0;
  background: #f3f4f6; padding: 8px; border-radius: 4px; max-height: 400px;
  overflow-y: auto;
}
th {
  background: #f3f4f6; position: sticky; top: 0;
  cursor: pointer; user-select: none;
}
th:hover { background: #e5e7eb; }
tr:hover td { background: #f9fafb; }
th .arrow { font-size: 10px; margin-left: 4px; opacity: 0.4; }
th.sorted .arrow { opacity: 1; }

@media (max-width: 900px) {
  body { flex-direction: column; }
  nav.toc {
    position: relative; width: 100%; height: auto; max-height: 300px;
    border-right: none; border-bottom: 1px solid #e5e7eb;
  }
  .filters-grid { grid-template-columns: 1fr; }
}"""

_JS = """\
document.querySelectorAll("table").forEach(table => {
  const headers = table.querySelectorAll("th");
  headers.forEach((th, idx) => {
    th.innerHTML += ' <span class="arrow">&#9650;</span>';
    let asc = true;
    th.addEventListener("click", () => {
      const tbody = table.querySelector("tbody");
      const rows = Array.from(tbody.querySelectorAll("tr"));
      rows.sort((a, b) => {
        const av = a.children[idx].textContent;
        const bv = b.children[idx].textContent;
        const an = parseFloat(av), bn = parseFloat(bv);
        if (!isNaN(an) && !isNaN(bn)) return asc ? an - bn : bn - an;
        return asc ? av.localeCompare(bv) : bv.localeCompare(av);
      });
      rows.forEach(r => tbody.appendChild(r));
      headers.forEach(h => {
        h.classList.remove("sorted");
        h.querySelector(".arrow").textContent = "\\u25B2";
      });
      th.classList.add("sorted");
      th.querySelector(".arrow").textContent = asc ? "\\u25B2" : "\\u25BC";
      asc = !asc;
    });
  });
});"""
