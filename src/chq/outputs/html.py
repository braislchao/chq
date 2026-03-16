"""HTML output formatter for chq reports."""

from __future__ import annotations

import html
import sys
from datetime import datetime, timedelta, timezone

CATEGORY_TITLES = {
    "top_n": "Top N — Most Expensive Queries",
    "anomalies": "Anomalies — Week-over-Week Changes",
    "cost_attribution": "Cost Attribution",
    "anti_patterns": "Anti-Patterns Detected",
}


def _esc(value: object) -> str:
    return html.escape(str(value))


def output_html(results: list, config) -> None:
    """Render *results* as a self-contained HTML file."""
    now = datetime.now(tz=timezone.utc)
    start = now - timedelta(days=config.lookback_days)
    date_range = f"{start:%Y-%m-%d} to {now:%Y-%m-%d}"

    grouped: dict[str, list] = {}
    for qr in results:
        grouped.setdefault(qr.category, []).append(qr)

    checks_html = []
    for category, checks in grouped.items():
        title = CATEGORY_TITLES.get(category, category.replace("_", " ").title())
        checks_html.append(f'<h2>{_esc(title)}</h2>')

        for qr in checks:
            human_name = qr.name.replace("_", " ").title()
            checks_html.append(f'<h3>{_esc(human_name)}</h3>')

            if not qr.rows:
                checks_html.append('<p class="empty">No results</p>')
                continue

            checks_html.append('<div class="table-wrap"><table>')
            query_col_idx = None
            checks_html.append('<thead><tr>')
            for i, col in enumerate(qr.columns):
                if col == "example_query":
                    query_col_idx = i
                checks_html.append(f'<th>{_esc(col)}</th>')
            checks_html.append('</tr></thead>')
            checks_html.append('<tbody>')
            for row in qr.rows:
                checks_html.append('<tr>')
                for i, val in enumerate(row):
                    if i == query_col_idx:
                        text = str(val)
                        short = text[:80] + "..." if len(text) > 80 else text
                        checks_html.append(
                            f'<td class="query-cell"><details><summary>{_esc(short)}</summary>'
                            f'<pre>{_esc(text)}</pre></details></td>'
                        )
                    else:
                        checks_html.append(f'<td>{_esc(val)}</td>')
                checks_html.append('</tr>')
            checks_html.append('</tbody></table></div>')

    body = "\n".join(checks_html)

    page = f"""\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>chq report</title>
<style>
  * {{ box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
         max-width: 1200px; margin: 0 auto; padding: 24px; background: #fafafa; color: #1a1a1a; }}
  h1 {{ border-bottom: 2px solid #2563eb; padding-bottom: 8px; }}
  h2 {{ color: #2563eb; margin-top: 32px; }}
  h3 {{ color: #374151; margin-top: 24px; }}
  .meta {{ color: #6b7280; margin-bottom: 24px; }}
  .empty {{ color: #9ca3af; font-style: italic; }}
  .table-wrap {{ overflow-x: auto; margin: 12px 0 24px; }}
  table {{ border-collapse: collapse; font-size: 14px; width: 100%; }}
  th, td {{ text-align: left; padding: 8px 12px; border: 1px solid #e5e7eb;
            white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 300px; }}
  td.query-cell {{ white-space: normal; max-width: 500px; min-width: 200px; }}
  td.query-cell details summary {{ cursor: pointer; white-space: nowrap; overflow: hidden;
                                   text-overflow: ellipsis; display: block; }}
  td.query-cell details[open] {{ white-space: normal; }}
  td.query-cell details[open] summary {{ margin-bottom: 8px; white-space: nowrap; }}
  td.query-cell pre {{ white-space: pre-wrap; word-break: break-word; font-size: 13px; margin: 0;
                       background: #f3f4f6; padding: 8px; border-radius: 4px; max-height: 400px;
                       overflow-y: auto; }}
  th {{ background: #f3f4f6; position: sticky; top: 0; cursor: pointer; user-select: none; }}
  th:hover {{ background: #e5e7eb; }}
  tr:hover td {{ background: #f9fafb; }}
  th .arrow {{ font-size: 10px; margin-left: 4px; opacity: 0.4; }}
  th.sorted .arrow {{ opacity: 1; }}
</style>
</head>
<body>
<h1>ClickHouse Query Performance Report</h1>
<p class="meta">Date range: {_esc(date_range)} · Generated {_esc(now.strftime("%Y-%m-%d %H:%M UTC"))}</p>
{body}
<script>
document.querySelectorAll("table").forEach(table => {{
  const headers = table.querySelectorAll("th");
  headers.forEach((th, idx) => {{
    th.innerHTML += ' <span class="arrow">▲</span>';
    let asc = true;
    th.addEventListener("click", () => {{
      const tbody = table.querySelector("tbody");
      const rows = Array.from(tbody.querySelectorAll("tr"));
      rows.sort((a, b) => {{
        const av = a.children[idx].textContent;
        const bv = b.children[idx].textContent;
        const an = parseFloat(av), bn = parseFloat(bv);
        if (!isNaN(an) && !isNaN(bn)) return asc ? an - bn : bn - an;
        return asc ? av.localeCompare(bv) : bv.localeCompare(av);
      }});
      rows.forEach(r => tbody.appendChild(r));
      headers.forEach(h => {{ h.classList.remove("sorted"); h.querySelector(".arrow").textContent = "▲"; }});
      th.classList.add("sorted");
      th.querySelector(".arrow").textContent = asc ? "▲" : "▼";
      asc = !asc;
    }});
  }});
}});
</script>
</body>
</html>"""

    output_path = getattr(config, "output_path", None)
    if output_path:
        with open(output_path, "w", encoding="utf-8") as fh:
            fh.write(page)
    else:
        sys.stdout.write(page)
