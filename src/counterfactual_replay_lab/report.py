"""Render a markdown study report from summary.json (and optional recovery.json)."""
from __future__ import annotations

import json
from pathlib import Path


def _r(x, digits=3):
    return "n/a" if x is None else f"{x:.{digits}f}"


def _ratio_line(name, s):
    lo, hi = s["ci95_geomean_ratio"]
    return (f"| {name} | {s['n_units']} | {_r(s['geomean_ratio'])} | [{_r(lo)}, {_r(hi)}] | "
            f"{_r(s['median_ratio'])} | {s['units_where_slower']} |")


def render(study_dir: Path, recovery: Path | None = None) -> str:
    s = json.loads((study_dir / "summary.json").read_text())
    p = s["primary"]
    lines = [f"# Dependency replay study ({s['phase']})", "",
             f"Units: {s['n_units_applicable']} applicable of {s['n_units_total']} generated; "
             f"{s['repetitions']} repetitions per arm per unit. Ratios are arm wall time / full rerun wall time "
             "per unit (median over repetitions), summarized as a geometric mean with a unit-level bootstrap 95% "
             "interval. The interval covers workload-unit sampling within this generated grid only, not "
             "machine noise across sessions or other workloads.", "",
             "## Semantic equivalence (every repetition checked against the full-rerun oracle)", "",
             "| arm | units equivalent | units with duplicate effects |", "|---|---|---|"]
    for mode, e in s["equivalence"].items():
        lines.append(f"| {mode} | {e['equivalent']}/{e['units']} | {e['duplicate_effect_units']} |")
    w = p["incremental_vs_full_wall"]
    x = p["incremental_vs_full_tool_executions"]
    lines += ["", "## Pre-registered primary comparison", "",
              f"Localized regime definition: {json.dumps(p['definition'])}", "",
              f"- Units in regime: {p['localized_units']}",
              f"- Incremental/full wall-time geomean ratio: {_r(w['geomean_ratio'])} "
              f"(95% CI {_r(w['ci95_geomean_ratio'][0])}-{_r(w['ci95_geomean_ratio'][1])}), "
              f"slower in {w['units_where_slower']} units",
              f"- Tool executions: {x['executed']} incremental vs {x['full_executed']} full (ratio {_r(x['ratio'])})",
              f"- Wall criterion met: {p['wall_criterion_met']}; execution criterion met: {p['exec_criterion_met']}; "
              f"semantics preserved: {p['semantics_preserved']}; **win: {p['win']}**", "",
              "## Secondary comparisons", "",
              "| comparison | units | geomean ratio | 95% CI | median | units slower |", "|---|---|---|---|---|---|"]
    sec = s["secondary"]
    lines.append(_ratio_line("localized incremental vs prefix", sec["localized_incremental_vs_prefix"]))
    lines.append(_ratio_line("localized incremental vs memo_value", sec["localized_incremental_vs_memo_value"]))
    lines.append(_ratio_line("localized incremental vs no-cutoff ablation", sec["localized_incremental_vs_nocutoff"]))
    for mode, v in sec["all_units"].items():
        lines.append(_ratio_line(f"all units {mode} vs full", v))
    for group in ("by_regime", "by_change", "by_family"):
        for k, v in sec[group].items():
            lines.append(_ratio_line(f"incremental vs full, {group[3:]}={k}", v))
    lines += ["", "## Break-even by affected fraction (incremental vs full)", "",
              "| affected fraction | regime | units | geomean ratio | 95% CI | median | units slower |",
              "|---|---|---|---|---|---|---|"]
    for b in sec["break_even_by_affected_fraction"]:
        lo, hi = b["ci95_geomean_ratio"]
        lines.append(f"| {b['affected_fraction'][0]}-{b['affected_fraction'][1]} | {b['regime']} | {b['n_units']} | "
                     f"{_r(b['geomean_ratio'])} | [{_r(lo)}, {_r(hi)}] | {_r(b['median_ratio'])} | "
                     f"{b['units_where_slower']} |")
    lines += ["", "## Work and overhead totals (all units, repetition 0)", "",
              "| arm | tool executions | bytes hashed | bytes loaded | storage growth bytes |", "|---|---|---|---|---|"]
    for mode in sec["tool_executions_all_units"]:
        lines.append(f"| {mode} | {sec['tool_executions_all_units'][mode]['executed']} | "
                     f"{sec['hashed_bytes_all_units'][mode]} | {sec['loaded_bytes_all_units'][mode]} | "
                     f"{sec['storage_delta_bytes_all_units'][mode]} |")
    if recovery and recovery.exists():
        r = json.loads(recovery.read_text())
        rec, can = r["recovery"], r["cancellation"]
        lines += ["", "## Crash recovery and cancellation", "",
                  f"- Crash/reopen/resume: {rec['n']} crashes (all crashed: {rec['all_crashed']}), "
                  f"all equivalent to oracle: {rec['all_equivalent']}; median resume {_r(rec['median_resume_ms'], 1)} ms "
                  f"vs median fresh incremental replay {_r(rec['median_fresh_replay_ms'], 1)} ms",
                  f"- Cooperative cancellation: n={can['n']}, statuses {can['statuses']}, median "
                  f"{_r(can['median_ms'], 1)} ms, max {_r(can['max_ms'], 1)} ms, effects after cancel "
                  f"{can['effects_after_cancel']}. {can['note']}"]
    env = s["environment"]
    lines += ["", "## Environment", "", f"`{env['platform']}`, Python {env['python'].split()[0]}, SQLite "
              f"{env['sqlite']}, {env['cpu_count']} CPUs, load average at start {env['load_average_start']}. "
              f"{env['isolation']}. Elapsed {s['elapsed_s']:.0f} s.", ""]
    return "\n".join(lines)
