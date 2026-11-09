"""Local interactive dependency-replay demo backed by the real engine.

A visitor edits the refund policy, a captured tool response, the risk model
version or the graph itself, runs a replay, and sees which nodes were reused,
validated by early cutoff, recomputed, added or removed, the first divergence
with its causes, the isolated effect diff and the full-rerun oracle check.
Saved runs reopen in a clearly labelled recorded mode and can be reproduced.

Bounds: 32 KB request bodies, 64-node graphs, registered operations only (no
code execution), 5 s tool bound, one replay at a time, loopback by default.
"""
from __future__ import annotations

import json
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from .dag import Graph, GraphError
from .engine import Engine, PlanError, capture
from .explain import compare_runs
from .ops import REGISTRY
from .oracle import check_equivalence, full_rerun
from .snapshot import Snapshot, SnapshotError
from .store import RunStore, StoreError
from .workflows import DEFAULT_POLICY, REFUND_ENV, refund_graph

MAX_BODY = 32 * 1024
MAX_NODES = 64
MAX_ROUNDS = 200_000
TOOL_TIMEOUT = 5.0


class DemoError(ValueError):
    pass


class DemoState:
    def __init__(self, data_dir: Path | None = None):
        self.dir = data_dir or Path(tempfile.mkdtemp(prefix="replaylab-demo-"))
        self.dir.mkdir(parents=True, exist_ok=True)
        self.store = RunStore(str(self.dir / "demo.db"))
        self.lock = threading.Lock()
        self.bases: dict[str, str] = {}
        existing = [r for r in self.store.runs() if r["mode"] == "capture" and r["status"].startswith("completed")]
        for r in existing:
            self.bases[r["plan"].get("request_id") or r["run_id"]] = r["run_id"]

    def base_for(self, order: str, customer: str) -> str:
        request_id = f"demo-{order}-{customer}"
        if order not in REFUND_ENV["orders"] and order != "9999":
            raise DemoError("Unknown fixture order")
        if customer not in REFUND_ENV["customers"] and customer != "c-missing":
            raise DemoError("Unknown fixture customer")
        if request_id not in self.bases:
            _snap, result = capture(self.store, refund_graph(order, customer), REFUND_ENV, request_id)
            self.bases[request_id] = result.run_id
        return self.bases[request_id]

    # ------------------------------------------------------------- views
    def run_view(self, run_id: str) -> dict[str, Any]:
        run = self.store.run(run_id)
        graph = Graph.from_json(self.store.graph_body(run["graph_hash"]))
        records = self.store.records(run_id)
        nodes = []
        for name in graph.order:
            spec = graph.nodes[name]
            status, key, result_hash, source, error = records[name]
            value = None
            if result_hash:
                body = self.store.blob(result_hash)
                value = body if len(body) <= 200 else body[:197] + "..."
            nodes.append({"id": name, "op": f"{spec.op}@{spec.op_version}", "effect": spec.effect,
                          "params": spec.params, "inputs": list(spec.inputs),
                          "guards": [g.to_dict() for g in spec.guards], "status": status, "source": source,
                          "error": error, "value": value, "key": (key or "")[:12]})
        return {"run_id": run_id, "mode": run["mode"], "status": run["status"], "base_run": run["base_run"],
                "detail": run["detail"], "nodes": nodes, "graph": json.loads(graph.to_json()),
                "ledger": [{"key": k[:12], "intent": v, "origin": o} for k, v, _n, o in self.store.ledger_rows(run_id)]}

    def saved_runs(self) -> list[dict[str, Any]]:
        return [{"run_id": r["run_id"], "mode": r["mode"], "status": r["status"], "base_run": r["base_run"],
                 "request": (r["plan"].get("request_id") if r["mode"] == "capture" else None)}
                for r in self.store.runs()][-40:]

    # ------------------------------------------------------------ actions
    def replay(self, body: dict[str, Any]) -> dict[str, Any]:
        order = str(body.get("order", "1042"))
        customer = str(body.get("customer", "c-77"))
        base_run = self.base_for(order, customer)
        base = self.store.run(base_run)
        snapshot = Snapshot.from_json(self.store.snapshot_body(base["snapshot_hash"]))
        if body.get("graph") is not None:
            graph = Graph.from_dict(body["graph"])
        else:
            policy = body.get("policy") or {}
            unknown = set(policy) - set(DEFAULT_POLICY)
            if unknown:
                raise DemoError(f"Unknown policy fields {sorted(unknown)}")
            hold = body.get("fraud_hold")
            graph = refund_graph(order, customer, policy, str(body.get("risk_version", "v1")),
                                 None if hold in (None, "") else int(hold))
        validate_demo_graph(graph)
        for node, value in (body.get("observation_edits") or {}).items():
            snapshot = snapshot.with_observation(str(node), value)
        mode = str(body.get("mode", "incremental"))
        engine = Engine(self.store, tool_timeout=TOOL_TIMEOUT)
        t = time.perf_counter_ns()
        plan = engine.plan_replay(base_run, graph, snapshot, mode)
        result = engine.execute(plan)
        wall_ms = (time.perf_counter_ns() - t) / 1e6
        oracle = full_rerun(json.loads(graph.to_json()), json.loads(snapshot.to_json()))
        mismatches = check_equivalence(self.store, result.run_id, oracle)
        explanation = compare_runs(self.store, base_run, result.run_id)
        full_ops = sum(1 for n in graph.nodes.values() if n.effect != "observation"
                       and oracle.status.get(n.id) == "ok")
        return {"execution": "live", "run": self.run_view(result.run_id), "base": self.run_view(base_run),
                "plan": {"seeds": plan.seeds, "scheduled": plan.schedule, "reused": plan.reuse,
                         "commit_only": plan.commit_only},
                "counters": result.counters, "wall_ms": wall_ms, "full_rerun_operations": full_ops,
                "oracle": {"matches_full_rerun": not mismatches, "mismatches": mismatches},
                "explanation": explanation}

    def reopen(self, run_id: str) -> dict[str, Any]:
        run = self.store.run(run_id)
        out = {"execution": "recorded", "run": self.run_view(run_id)}
        if run["base_run"]:
            out["base"] = self.run_view(run["base_run"])
            out["explanation"] = compare_runs(self.store, run["base_run"], run_id)
        return out

    def reproduce(self, run_id: str) -> dict[str, Any]:
        """Re-execute a saved run from its stored graph and snapshot; compare result hashes."""
        run = self.store.run(run_id)
        if not run["base_run"]:
            raise DemoError("Capture runs are reproduced by capturing again")
        graph = Graph.from_json(self.store.graph_body(run["graph_hash"]))
        snapshot = Snapshot.from_json(self.store.snapshot_body(run["snapshot_hash"]))
        engine = Engine(self.store, tool_timeout=TOOL_TIMEOUT)
        again = engine.execute(engine.plan_replay(run["base_run"], graph, snapshot, "full"))
        saved, fresh = self.store.records(run_id), self.store.records(again.run_id)
        differing = sorted(n for n in saved if (saved[n][0], saved[n][2]) != (fresh[n][0], fresh[n][2]))
        same_ledger = self.store.ledger(run_id) == self.store.ledger(again.run_id)
        return {"execution": "live", "saved_run": run_id, "reproduction_run": again.run_id,
                "reproduced": not differing and same_ledger, "differing_nodes": differing,
                "same_ledger": same_ledger}


def validate_demo_graph(graph: Graph) -> None:
    if len(graph.nodes) > MAX_NODES:
        raise DemoError(f"Demo graphs are limited to {MAX_NODES} nodes")
    for node in graph.nodes.values():
        if (node.op, node.op_version) not in REGISTRY:
            raise DemoError(f"Unknown operation {node.op}@{node.op_version}")
        if node.op in ("syn.slow", "syn.stubborn"):
            raise DemoError("Sleep-based test operations are disabled in the demo")
        for k in ("rounds", "risk_rounds"):
            if isinstance(node.params.get(k), int) and node.params[k] > MAX_ROUNDS:
                raise DemoError(f"{k} is limited to {MAX_ROUNDS} in the demo")
        if isinstance(node.params.get("out_bytes"), int) and node.params["out_bytes"] > 4096:
            raise DemoError("out_bytes is limited to 4096 in the demo")


def make_handler(state: DemoState):
    class Handler(BaseHTTPRequestHandler):
        def _send(self, code: int, payload: Any, content_type="application/json"):
            body = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _guard(self, fn):
            try:
                with state.lock:
                    self._send(200, fn())
            except (DemoError, GraphError, SnapshotError, PlanError, StoreError, ValueError, KeyError,
                    TypeError) as error:
                self._send(400, {"error": f"{type(error).__name__}: {error}"})

        def do_GET(self):  # noqa: N802
            path = self.path.split("?", 1)[0]
            if path == "/":
                return self._send(200, PAGE.encode(), "text/html; charset=utf-8")
            if path == "/api/state":
                return self._guard(lambda: {"base_run": state.base_for("1042", "c-77"),
                                            "default_policy": DEFAULT_POLICY, "env": REFUND_ENV,
                                            "runs": state.saved_runs()})
            if path.startswith("/api/run/"):
                return self._guard(lambda: state.reopen(path.rsplit("/", 1)[1]))
            self._send(404, {"error": "not found"})

        def do_POST(self):  # noqa: N802
            length = int(self.headers.get("Content-Length") or 0)
            if length > MAX_BODY:
                return self._send(413, {"error": f"Request body exceeds {MAX_BODY} bytes"})
            try:
                body = json.loads(self.rfile.read(length) or b"{}")
                if not isinstance(body, dict):
                    raise ValueError("Body must be a JSON object")
            except ValueError as error:
                return self._send(400, {"error": f"Invalid JSON: {error}"})
            path = self.path.split("?", 1)[0]
            if path == "/api/replay":
                return self._guard(lambda: state.replay(body))
            if path.startswith("/api/reproduce/"):
                return self._guard(lambda: state.reproduce(path.rsplit("/", 1)[1]))
            self._send(404, {"error": "not found"})

        def log_message(self, *args):
            return

    return Handler


def serve(host: str = "127.0.0.1", port: int = 8766, data_dir: Path | None = None) -> None:
    state = DemoState(data_dir)
    server = ThreadingHTTPServer((host, port), make_handler(state))
    print(f"Dependency replay demo: http://{host}:{server.server_port}  (runs saved in {state.dir})", flush=True)
    server.serve_forever()


PAGE = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Dependency Replay</title>
<style>
:root{--ink:#17202f;--muted:#5d6878;--line:#dfe3ea;--paper:#f6f7f9;--panel:#fff;--accent:#3158d4;--reuse:#9aa3b2;
--cut:#1f8a83;--exec:#c77700;--add:#247454;--rem:#b54747;--inact:#c9ced8;--code:#f0f2f6}
@media (prefers-color-scheme: dark){:root{--ink:#e7ebf3;--muted:#9aa6ba;--line:#2e3747;--paper:#10151f;--panel:#171f2c;
--accent:#91a9ff;--reuse:#7b8496;--cut:#4cc3b8;--exec:#f1a842;--add:#69c99b;--rem:#ff8f8b;--inact:#3a4354;--code:#0f141d}}
*{box-sizing:border-box}body{margin:0;background:var(--paper);color:var(--ink);font:14px/1.45 ui-sans-serif,-apple-system,"Segoe UI",sans-serif}
main{max-width:1280px;margin:0 auto;padding:20px 16px 60px}h1{font-size:24px;margin:0 0 4px;letter-spacing:-.02em}
.sub{color:var(--muted);margin:0 0 16px;max-width:820px}.grid{display:grid;grid-template-columns:320px 1fr;gap:16px}
.panel{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:14px;min-width:0}
.panel h2{font-size:14px;margin:0 0 10px;text-transform:uppercase;letter-spacing:.06em;color:var(--muted)}
label{display:flex;justify-content:space-between;gap:8px;align-items:center;margin:6px 0;font-size:13px}
input,select,textarea{font:inherit;color:var(--ink);background:var(--paper);border:1px solid var(--line);border-radius:6px;padding:5px 7px}
input[type=number]{width:110px}textarea{width:100%;font:12px ui-monospace,Menlo,monospace;min-height:84px}
button{font:inherit;font-weight:650;border:0;border-radius:7px;padding:9px 14px;background:var(--accent);color:#fff;cursor:pointer}
button.secondary{background:transparent;color:var(--accent);border:1px solid var(--line);padding:5px 9px}
.badge{display:inline-block;font-size:11px;font-weight:700;padding:2px 7px;border-radius:5px;margin-right:6px}
.live{background:#e8f5ee;color:#247454}.recorded{background:#fff6e3;color:#8a5300}
.stats{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px;margin:10px 0}.stat{border:1px solid var(--line);border-radius:8px;padding:8px}
.stat b{display:block;font-size:18px}.stat span{color:var(--muted);font-size:12px}
svg{width:100%;height:auto;display:block}.legend{display:flex;flex-wrap:wrap;gap:10px;font-size:12px;color:var(--muted);margin:6px 0}
.sw{display:inline-block;width:11px;height:11px;border-radius:3px;margin-right:4px;vertical-align:-1px}
pre{background:var(--code);border-radius:6px;padding:8px;overflow:auto;font-size:12px;max-height:240px;margin:6px 0}
.div{border-left:3px solid var(--exec);padding:6px 10px;background:var(--code);border-radius:0 6px 6px 0}
.err{color:var(--rem);font-weight:600}.row{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
table{border-collapse:collapse;width:100%;font-size:12px}td,th{border-bottom:1px solid var(--line);padding:4px 6px;text-align:left;vertical-align:top}
details summary{cursor:pointer;color:var(--muted);margin:8px 0}
@media(max-width:860px){.grid{grid-template-columns:1fr}.stats{grid-template-columns:repeat(2,1fr)}}
</style></head><body><main>
<h1>Dependency replay</h1>
<p class="sub">Change a refund policy, a captured tool response or the graph, and replay only what depends on it. Every replay below is executed live by the engine against a frozen snapshot with an isolated local ledger, then checked against an independent full rerun.</p>
<div class="grid">
<section class="panel" id="controls">
<h2>Scenario</h2>
<label>Order <select id="order"></select></label>
<label>Customer <select id="customer"></select></label>
<h2 style="margin-top:14px">Policy</h2>
<label>Refund window (days) <input type="number" id="window_days"></label>
<label>Auto-approve max (cents) <input type="number" id="auto_approve_max_cents"></label>
<label>Max risk score <input type="number" id="max_risk"></label>
<label>Restocking fee % <input type="number" id="restocking_fee_pct"></label>
<label>Approve eligible <input type="checkbox" id="approve_eligible"></label>
<label>Risk model <select id="risk_version"><option>v1</option><option>v2</option></select></label>
<label>Add fraud-hold node at <input type="number" id="fraud_hold" placeholder="off"></label>
<h2 style="margin-top:14px">Captured tool responses</h2>
<p class="sub" style="margin:0 0 4px;font-size:12px">Edit to ask "what if the tool had returned this?"</p>
<label style="display:block">order_lookup<textarea id="obs_order"></textarea></label>
<label style="display:block">customer_status<textarea id="obs_customer"></textarea></label>
<details><summary>Edit raw graph JSON (overrides policy fields)</summary>
<label><span>Use raw graph</span><input type="checkbox" id="use_raw"></label><textarea id="raw" style="min-height:160px"></textarea></details>
<label>Replay mode <select id="mode"><option value="incremental">incremental (proposed)</option><option value="incremental_nocutoff">no early cutoff</option><option value="prefix">fixed prefix</option><option value="memo_value">value memo</option><option value="full">full rerun</option></select></label>
<div class="row" style="margin-top:10px"><button id="go">Replay</button><span id="msg" class="err"></span></div>
<h2 style="margin-top:18px">Saved runs</h2><div id="runs"></div>
</section>
<section class="panel">
<div class="row"><span id="exec" class="badge live">live execution</span><b id="title">Loading</b></div>
<div class="stats"><div class="stat"><b id="s_exec">-</b><span>operations executed</span></div><div class="stat"><b id="s_full">-</b><span>full rerun would execute</span></div>
<div class="stat"><b id="s_reuse">-</b><span>reused / cutoff</span></div><div class="stat"><b id="s_oracle">-</b><span>matches full rerun</span></div></div>
<div class="legend"><span><i class="sw" style="background:var(--reuse)"></i>reused (unaffected)</span><span><i class="sw" style="background:var(--cut)"></i>affected, key unchanged (cutoff)</span>
<span><i class="sw" style="background:var(--exec)"></i>recomputed</span><span><i class="sw" style="border:2px solid var(--add)"></i>added</span><span><i class="sw" style="border:2px dashed var(--rem)"></i>removed</span><span><i class="sw" style="background:var(--inact)"></i>inactive branch</span><span>solid = data edge, dashed = control edge</span></div>
<svg id="dag" viewBox="0 0 900 420" role="img" aria-label="Dependency graph"></svg>
<h2 style="margin-top:12px">First divergence</h2><div id="first" class="div">-</div>
<h2 style="margin-top:12px">Isolated effect diff</h2><div id="effects">-</div>
<h2 style="margin-top:12px">Node detail</h2><div id="detail" class="sub">Click a node.</div>
<details><summary>All nodes</summary><div id="table"></div></details>
</section></div>
<script>
const $=s=>document.querySelector(s);const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
let ENV=null,DEF=null,LAST=null;
async function api(path,body){const r=await fetch(path,body?{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}:{});const d=await r.json();if(!r.ok)throw new Error(d.error||r.status);return d}
function fillObs(){const o=ENV.orders[$('#order').value];const c=ENV.customers[$('#customer').value];
$('#obs_order').value=JSON.stringify(o?{found:true,...o}:{found:false});$('#obs_customer').value=JSON.stringify(c?{found:true,...c}:{found:false});}
async function init(){const s=await api('/api/state');ENV=s.env;DEF=s.default_policy;
$('#order').innerHTML=Object.keys(ENV.orders).map(k=>`<option>${k}</option>`).join('')+'<option>9999</option>';
$('#customer').innerHTML=Object.keys(ENV.customers).map(k=>`<option>${k}</option>`).join('')+'<option>c-missing</option>';
for(const k of ['window_days','auto_approve_max_cents','max_risk','restocking_fee_pct'])$('#'+k).value=DEF[k];$('#approve_eligible').checked=DEF.approve_eligible;
fillObs();$('#order').onchange=fillObs;$('#customer').onchange=fillObs;renderRuns(s.runs);run();}
function body(){const b={order:$('#order').value,customer:$('#customer').value,mode:$('#mode').value,risk_version:$('#risk_version').value,
fraud_hold:$('#fraud_hold').value,policy:{window_days:+$('#window_days').value,auto_approve_max_cents:+$('#auto_approve_max_cents').value,max_risk:+$('#max_risk').value,restocking_fee_pct:+$('#restocking_fee_pct').value,approve_eligible:$('#approve_eligible').checked}};
const o=ENV.orders[b.order],c=ENV.customers[b.customer];const origO=JSON.stringify(o?{found:true,...o}:{found:false}),origC=JSON.stringify(c?{found:true,...c}:{found:false});
const edits={};if($('#obs_order').value.trim()!==origO)edits.order_lookup=JSON.parse($('#obs_order').value);if($('#obs_customer').value.trim()!==origC)edits.customer_status=JSON.parse($('#obs_customer').value);b.observation_edits=edits;
if($('#use_raw').checked)b.graph=JSON.parse($('#raw').value);return b}
async function run(){$('#msg').textContent='';$('#go').disabled=true;try{const d=await api('/api/replay',body());show(d);const s=await api('/api/state');renderRuns(s.runs)}catch(e){$('#msg').textContent=e.message}finally{$('#go').disabled=false}}
$('#go').onclick=run;
function renderRuns(runs){$('#runs').innerHTML='<table>'+runs.slice().reverse().map(r=>`<tr><td>${esc(r.run_id)}</td><td>${esc(r.mode)}</td><td>${r.base_run?`<button class="secondary" data-open="${esc(r.run_id)}">open</button> <button class="secondary" data-rep="${esc(r.run_id)}">reproduce</button>`:'base capture'}</td></tr>`).join('')+'</table>';
document.querySelectorAll('[data-open]').forEach(b=>b.onclick=async()=>{try{show(await api('/api/run/'+b.dataset.open))}catch(e){$('#msg').textContent=e.message}});
document.querySelectorAll('[data-rep]').forEach(b=>b.onclick=async()=>{try{const d=await api('/api/reproduce/'+b.dataset.rep);$('#msg').textContent='';alert(d.reproduced?`Reproduced: ${d.saved_run} re-executed as ${d.reproduction_run} (full rerun) with identical node results and ledger.`:`NOT reproduced: ${d.differing_nodes.join(', ')} ledger same=${d.same_ledger}`);renderRuns((await api('/api/state')).runs)}catch(e){$('#msg').textContent=e.message}});}
function color(n,kind){if(kind==='removed')return'none';if(n.status==='inactive')return'var(--inact)';if(n.status==='failed'||n.status==='blocked')return'var(--rem)';return n.source==='executed'?'var(--exec)':n.source==='cutoff'||n.source==='memo'?'var(--cut)':'var(--reuse)'}
function layout(nodes){const by={};nodes.forEach(n=>by[n.id]=n);const depth={};const d=id=>{if(depth[id]!==undefined)return depth[id];const n=by[id];const ps=[...n.inputs,...n.guards.map(g=>g.node)].filter(p=>by[p]);depth[id]=ps.length?1+Math.max(...ps.map(d)):0;return depth[id]};nodes.forEach(n=>d(n.id));
const layers={};nodes.forEach(n=>(layers[depth[n.id]]=layers[depth[n.id]]||[]).push(n.id));const pos={};const L=Object.keys(layers).length;Object.entries(layers).forEach(([k,ids])=>{ids.sort();ids.forEach((id,i)=>pos[id]={x:60+(+k)*(780/Math.max(1,L-1)),y:40+(i+0.5)*(340/ids.length)})});return pos}
function show(d){LAST=d;const live=d.execution==='live';$('#exec').className='badge '+(live?'live':'recorded');$('#exec').textContent=live?'live execution':'recorded run (reopened from store, not re-executed)';
const run=d.run;$('#title').textContent=`${run.run_id} · ${run.mode} · ${run.status}`;
const c=run.detail&&run.detail.counters||d.counters||{};$('#s_exec').textContent=c.executed??'-';$('#s_full').textContent=d.full_rerun_operations??'-';$('#s_reuse').textContent=c.reused!==undefined?`${c.reused} / ${c.cutoff}`:'-';
$('#s_oracle').textContent=d.oracle?(d.oracle.matches_full_rerun?'yes':'NO'):'(recorded)';$('#raw').value=JSON.stringify(run.graph,null,1);
const kinds={};(d.explanation?d.explanation.nodes:[]).forEach(e=>kinds[e.node]=e.kind);const nodes=run.nodes.slice();
if(d.base){const ids=new Set(nodes.map(n=>n.id));d.base.nodes.filter(n=>!ids.has(n.id)).forEach(n=>nodes.push({...n,_removed:true}))}
const pos=layout(nodes);let svg='<defs><marker id="ar" viewBox="0 0 10 10" refX="10" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M0,0L10,5L0,10z" fill="var(--muted)"/></marker></defs>';
nodes.forEach(n=>{const p=pos[n.id];n.inputs.forEach(i=>{if(pos[i])svg+=`<line x1="${pos[i].x+46}" y1="${pos[i].y}" x2="${p.x-46}" y2="${p.y}" stroke="var(--muted)" stroke-width="1" marker-end="url(#ar)"/>`});n.guards.forEach(g=>{if(pos[g.node])svg+=`<line x1="${pos[g.node].x+46}" y1="${pos[g.node].y}" x2="${p.x-46}" y2="${p.y}" stroke="var(--accent)" stroke-dasharray="4 3" stroke-width="1.2" marker-end="url(#ar)"/>`})});
nodes.forEach(n=>{const p=pos[n.id];const kind=n._removed?'removed':kinds[n.id];const fill=color(n,kind);const stroke=kind==='added'?'var(--add)':kind==='removed'?'var(--rem)':'var(--line)';
const dash=kind==='removed'?' stroke-dasharray="5 3"':'';const w=kind==='added'||kind==='removed'?2.5:1;const first=d.explanation&&d.explanation.first_divergence&&d.explanation.first_divergence.node===n.id;
svg+=`<g data-node="${esc(n.id)}" style="cursor:pointer"><rect x="${p.x-46}" y="${p.y-15}" width="92" height="30" rx="7" fill="${fill}" stroke="${first?'var(--ink)':stroke}" stroke-width="${first?2.5:w}"${dash}/><text x="${p.x}" y="${p.y+4}" text-anchor="middle" font-size="10.5" fill="var(--ink)">${esc(n.id)}</text></g>`});
$('#dag').innerHTML=svg;document.querySelectorAll('[data-node]').forEach(g=>g.onclick=()=>detail(g.dataset.node,nodes,kinds));
const f=d.explanation&&d.explanation.first_divergence;$('#first').innerHTML=f?`<b>${esc(f.node)}</b> ${esc(f.kind)} (${esc(f.original)} → ${esc(f.replay)})<ul>${f.reasons.map(r=>`<li>${esc(r)}</li>`).join('')}</ul>${f.original_value!==undefined?`<div>original: <code>${esc(f.original_value)}</code><br>replay: <code>${esc(f.replay_value)}</code></div>`:''}`:'No semantic divergence: the replay is equivalent to the original.';
const e=d.explanation&&d.explanation.effect_diff;$('#effects').innerHTML=e?['added','removed','changed'].map(k=>`<div><b>${k}</b>: ${e[k].length?e[k].map(x=>`<code>${esc(x.intent||x.replay)}</code>`).join(' '):'none'}</div>`).join(''):'-';
$('#table').innerHTML='<table><tr><th>node</th><th>status</th><th>source</th><th>change</th><th>value</th></tr>'+nodes.map(n=>`<tr><td>${esc(n.id)}</td><td>${esc(n._removed?'removed':n.status)}</td><td>${esc(n.source)}</td><td>${esc(n._removed?'removed':kinds[n.id]||'')}</td><td><code>${esc(n.value)}</code></td></tr>`).join('')+'</table>'}
function detail(id,nodes,kinds){const n=nodes.find(x=>x.id===id);const ex=(LAST.explanation?LAST.explanation.nodes:[]).find(e=>e.node===id);
$('#detail').innerHTML=`<b>${esc(id)}</b> · ${esc(n.op)} · ${esc(n.effect)}<br>status <b>${esc(n._removed?'removed':n.status)}</b>, source <b>${esc(n.source)}</b>, key ${esc(n.key)}${n.error?`<br><span class="err">${esc(n.error)}</span>`:''}<pre>${esc(JSON.stringify({params:n.params,inputs:n.inputs,guards:n.guards},null,1))}</pre>${n.value?`value <code>${esc(n.value)}</code>`:''}${ex&&ex.reasons.length?`<ul>${ex.reasons.map(r=>`<li>${esc(r)}</li>`).join('')}</ul>`:''}`}
init();
</script></main></body></html>"""
