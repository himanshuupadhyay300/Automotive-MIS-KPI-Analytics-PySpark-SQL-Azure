"""Data quality report (per batch, per entity) and pipeline execution summary writers: JSON + Markdown."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _pct(part: int, whole: int) -> str:
    return f"{100.0 * part / whole:.2f}%" if whole else "n/a"


def render_dq_markdown(report: dict[str, Any]) -> str:
    """Human-readable data quality report for one batch."""
    lines = [f"# Data Quality Report - batch `{report['batch_id']}`", "", f"Generated (UTC): {report['generated_at']}", ""]
    for entity, m in report["entities"].items():
        total = m["bronze_rows"]
        lines += [
            f"## {entity}",
            "",
            f"- Rows evaluated: **{total}** | valid: **{m['valid_rows']}** ({_pct(m['valid_rows'], total)}) | "
            f"rejected: **{m['rejected_rows']}** ({_pct(m['rejected_rows'], total)})",
            f"- Valid rows with warnings (kept, imputed/flagged): {m['rows_with_warnings']}",
            f"- Duplicate rows removed in batch: {m['in_batch_duplicate_rows_removed']} "
            f"(keys with conflicting payloads: {m['conflicting_duplicate_keys']})",
            f"- Keys already in Silver: {m['rows_matching_existing_silver']} "
            f"(of which changed = corrections: {m['rows_changing_existing_silver']})",
            f"- Late-arriving new records: {m['late_arrival_rows']}",
            f"- Silver rows after merge: **{m['silver_rows_after']}**",
        ]
        rej = m["rejected_by_rule"]
        lines.append("- Rejections by rule: " + (", ".join(f"`{k}`={v}" for k, v in sorted(rej.items())) or "none"))
        warn = m["warnings_by_rule"]
        lines.append("- Warnings by rule: " + (", ".join(f"`{k}`={v}" for k, v in sorted(warn.items())) or "none"))
        lines.append("")
    return "\n".join(lines)


def render_summary_markdown(summary: dict[str, Any]) -> str:
    """Human-readable pipeline execution summary."""
    lines = [
        f"# Pipeline Execution Summary - run `{summary['run_id']}`",
        "",
        f"- Status: **{summary['status']}** | storage format: `{summary['storage_format']}`",
        f"- Started: {summary['started_at']} | finished: {summary['finished_at']} | duration: {summary['duration_seconds']} s",
        "",
        "## Stages",
        "",
    ]
    for name, info in summary["stages"].items():
        lines.append(f"- **{name}**: " + ", ".join(f"{k}={v}" for k, v in info.items()))
    lines += ["", "## Table row counts after run", ""]
    lines += [f"- `{k}`: {v}" for k, v in summary.get("table_row_counts", {}).items()] or ["- n/a"]
    if summary.get("error"):
        lines += ["", "## Error", "", f"`{summary['error']}`"]
    return "\n".join([*lines, ""])


def write_report(payload: dict[str, Any], reports_dir: str | Path, stem: str, markdown: str) -> dict[str, str]:
    """Write ``<stem>.json`` and ``<stem>.md``; return both paths."""
    out = Path(reports_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path, md_path = out / f"{stem}.json", out / f"{stem}.md"
    json_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    md_path.write_text(markdown, encoding="utf-8")
    return {"json": str(json_path), "markdown": str(md_path)}
