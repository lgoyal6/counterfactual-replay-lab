from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from counterfactual_replay_lab.core import RefundAgent, ReplayEngine, compare


def experiment(policy: str = "v2") -> dict:
    agent = RefundAgent()
    baseline = agent.run("Order 1042 is eligible for a refund", "v1")
    counterfactual = ReplayEngine(agent.run).replay(
        baseline, changed_variable="policy_version", new_value=policy
    )
    result = compare(
        baseline,
        counterfactual,
        changed_variable="policy_version",
        old_value="v1",
        new_value=policy,
    )
    return {"baseline": baseline.to_dict(), "counterfactual": counterfactual.to_dict(), "experiment": result.to_dict()}


HTML = """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Counterfactual Replay Lab</title>
<style>
:root{color-scheme:dark;--bg:#0b1020;--panel:#111a2e;--line:#263555;--muted:#9aa9c4;--good:#55d6a5;--bad:#ff7f8f;--accent:#8ea7ff}
*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 15% 0,#182b57 0,#0b1020 38%);font:15px ui-sans-serif,system-ui,sans-serif;color:#eef3ff}
main{max-width:1180px;margin:0 auto;padding:42px 24px 70px}.eyebrow{color:var(--accent);font-weight:700;letter-spacing:.12em;text-transform:uppercase;font-size:12px}
h1{font-size:42px;line-height:1.05;margin:12px 0}.lede{max-width:680px;color:var(--muted);font-size:18px;line-height:1.5}.toolbar{display:flex;gap:12px;align-items:center;margin:28px 0}.button{border:1px solid #4964a8;background:#253b77;color:#fff;border-radius:9px;padding:11px 15px;font-weight:700;cursor:pointer}.button:hover{background:#304d98}.status{color:var(--muted)}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:16px}.panel{background:rgba(17,26,46,.9);border:1px solid var(--line);border-radius:16px;padding:20px;box-shadow:0 16px 50px #05081455}.panel h2{margin:0 0 16px;font-size:17px}.badge{display:inline-block;padding:5px 9px;border-radius:99px;font-size:12px;font-weight:800}.pass{background:#143e36;color:var(--good)}.fail{background:#48212a;color:var(--bad)}.metric{display:flex;justify-content:space-between;border-top:1px solid var(--line);padding:11px 0;color:var(--muted)}.metric strong{color:#fff}.timeline{margin-top:16px}.span{display:grid;grid-template-columns:12px 1fr auto;gap:10px;align-items:start;border-left:1px solid #45619d;padding:0 0 18px 15px;margin-left:5px}.dot{width:10px;height:10px;border-radius:50%;background:var(--accent);margin-left:-21px;margin-top:4px}.span small{color:var(--muted)}.diff{margin-top:16px;border:1px dashed #6076b4;padding:16px;border-radius:12px}.diff strong{color:var(--good)}.footer{margin-top:18px;color:var(--muted);font-size:13px;line-height:1.5}@media(max-width:800px){.grid{grid-template-columns:1fr}h1{font-size:34px}}
</style></head><body><main>
<div class="eyebrow">Counterfactual Replay Lab</div><h1>What if we changed one thing?</h1>
<p class="lede">Replay the same agent request with one controlled intervention. Find the first divergence, compare the evidence, and decide whether the change is worth keeping.</p>
<div class="toolbar"><button class="button" id="replay">Replay with policy v2</button><span class="status" id="status">Loading deterministic fixture...</span></div>
<section class="grid"><article class="panel" id="before"></article><article class="panel" id="after"></article></section>
<section class="panel diff" id="diff"></section><p class="footer">Local fixture mode. Live neatlogs, Entire, and cfo.ai connections are intentionally disabled until authenticated access is supplied.</p>
</main><script>
const esc=s=>String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
function traceCard(t,label){return `<h2>${label} <span class="badge ${t.verdict==='PASS'?'pass':'fail'}">${t.verdict}</span></h2><div class="metric"><span>Decision</span><strong>${esc(t.decision)}</strong></div><div class="metric"><span>Policy</span><strong>${esc(t.policy_version)}</strong></div><div class="metric"><span>Score</span><strong>${t.score}</strong></div><div class="metric"><span>Cost</span><strong>$${t.cost_usd.toFixed(3)}</strong></div><div class="metric"><span>Latency</span><strong>${t.latency_ms} ms</strong></div><div class="timeline">${t.spans.map(s=>`<div class="span"><i class="dot"></i><div><strong>${esc(s.name)}</strong><br><small>${esc(s.kind)} · ${esc(s.input)} → ${esc(s.output)}</small></div></div>`).join('')}</div>`}
async function load(){const r=await fetch('/api/experiment?policy=v2');const d=await r.json();document.querySelector('#before').innerHTML=traceCard(d.baseline,'Baseline run');document.querySelector('#after').innerHTML=traceCard(d.counterfactual,'Counterfactual run');const x=d.experiment;document.querySelector('#diff').innerHTML=`<strong>${esc(x.recommendation)}</strong> · changed <b>${esc(x.changed_variable)}</b> from <b>${esc(x.old_value)}</b> to <b>${esc(x.new_value)}</b><br><span class="status">Same request: ${x.same_request} · Same prefix: ${x.same_prefix} · First divergence: ${esc(x.first_divergence)} · Score delta: ${x.score_delta>0?'+':''}${x.score_delta}</span>`;document.querySelector('#status').textContent='Replay verified locally';}
document.querySelector('#replay').onclick=load;load();
</script></body></html>"""


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/":
            body = HTML.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
        elif parsed.path == "/api/experiment":
            policy = parse_qs(parsed.query).get("policy", ["v2"])[0]
            body = json.dumps(experiment(policy)).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
        else:
            body = b"Not found"
            self.send_response(404)
            self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):  # noqa: A002
        return


if __name__ == "__main__":
    server = ThreadingHTTPServer(("127.0.0.1", 8765), Handler)
    print("Counterfactual Replay Lab: http://127.0.0.1:8765", flush=True)
    server.serve_forever()
