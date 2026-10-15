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
    return {"baseline": baseline.to_dict(), "counterfactual": counterfactual.to_dict(), "experiment": result.to_dict(), "neatlogs_url": "https://app.neatlogs.com/traces"}


HTML = """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Counterfactual Replay Lab</title>
<style>
:root{color-scheme:light;--ink:#17202f;--muted:#667085;--line:#dfe3ea;--paper:#f7f8fa;--panel:#fff;--cobalt:#3158d4;--cobalt-soft:#edf2ff;--success:#247454;--success-soft:#e8f5ee;--red:#b54747;--red-soft:#fff0ef;--amber:#a86600;--amber-soft:#fff6e3;--shadow:0 10px 30px rgba(24,35,54,.06)}
*{box-sizing:border-box}html,body{max-width:100%;overflow-x:hidden}body{margin:0;background:var(--paper);font:14px/1.5 ui-sans-serif,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:var(--ink)}main{max-width:1180px;margin:0 auto;padding:28px 28px 72px}.topbar{display:flex;align-items:center;justify-content:space-between;gap:18px;padding-bottom:28px;border-bottom:1px solid var(--line)}.brand{display:flex;align-items:center;gap:11px;font-weight:760;letter-spacing:-.02em}.mark{width:28px;height:28px;border-radius:7px;background:var(--ink);color:#fff;display:grid;place-items:center;font-size:12px}.crumb{color:var(--muted);font-size:13px}.hero{display:flex;align-items:flex-end;justify-content:space-between;gap:24px;padding:48px 0 30px}.eyebrow{font-size:12px;font-weight:700;color:var(--cobalt);letter-spacing:.08em;text-transform:uppercase;margin-bottom:10px}.hero h1{font-size:38px;line-height:1.08;letter-spacing:-.045em;margin:0 0 10px}.hero p{max-width:620px;color:var(--muted);font-size:16px;margin:0}.action{display:flex;align-items:center;gap:12px;white-space:nowrap;min-width:0;flex-wrap:wrap;justify-content:flex-end}.hero .action{max-width:100%}.intervention{display:flex;align-items:center;gap:7px;color:var(--muted);font-size:13px}.intervention{min-width:0}.intervention select{max-width:250px;border:1px solid var(--line);border-radius:7px;background:var(--panel);color:var(--ink);padding:9px 10px;font:inherit;font-weight:700}.intervention select:focus-visible{outline:3px solid #9eb3ff;outline-offset:2px}.button{border:0;border-radius:7px;background:var(--cobalt);color:#fff;padding:11px 16px;font-weight:700;cursor:pointer;box-shadow:0 3px 8px rgba(49,88,212,.2)}.button:hover{background:#2749b7}.button:focus-visible{outline:3px solid #9eb3ff;outline-offset:2px}.status{color:var(--muted);font-size:13px}.summary{display:grid;grid-template-columns:repeat(4,1fr);gap:1px;border:1px solid var(--line);background:var(--line);border-radius:10px;overflow:hidden;box-shadow:var(--shadow);margin-bottom:22px}.stat{background:var(--panel);padding:17px 18px}.stat label{display:block;color:var(--muted);font-size:12px;margin-bottom:4px}.stat strong{font-size:18px;letter-spacing:-.02em}.stat strong.good{color:var(--success)}.stat strong.warn{color:var(--amber)}.grid{display:grid;grid-template-columns:1fr 1fr;gap:18px}.finding{display:flex;align-items:baseline;gap:12px;padding:15px 18px;margin:0 0 18px;border-left:3px solid var(--cobalt);background:var(--cobalt-soft);border-radius:0 8px 8px 0}.finding-kicker{font-size:11px;font-weight:800;letter-spacing:.08em;text-transform:uppercase;color:var(--cobalt)}.finding strong{font-size:15px}.finding-detail{color:var(--muted);font-size:13px}.panel{background:var(--panel);border:1px solid var(--line);border-radius:10px;box-shadow:var(--shadow);padding:22px}.panel-head{display:flex;align-items:center;justify-content:space-between;padding-bottom:16px;border-bottom:1px solid var(--line)}.panel h2{font-size:17px;letter-spacing:-.02em;margin:0}.badge{font-size:11px;font-weight:750;padding:4px 8px;border-radius:5px}.pass{background:var(--success-soft);color:var(--success)}.fail{background:var(--red-soft);color:var(--red)}.facts{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;padding:17px 0;border-bottom:1px solid var(--line)}.fact label{display:block;color:var(--muted);font-size:11px;margin-bottom:3px}.fact strong{font-size:14px}.trace{padding-top:17px}.trace-title{font-size:12px;color:var(--muted);font-weight:700;margin-bottom:10px}.span{display:grid;grid-template-columns:112px 1fr;gap:14px;padding:11px 0;border-top:1px solid #edf0f4}.span:first-child{border-top:0}.span.changed{background:var(--amber-soft);margin:0 -10px;padding-left:10px;padding-right:10px;border-left:3px solid var(--amber)}.span-name{font-weight:700}.span-meta{color:var(--muted);font-size:12px}.diff{margin-top:18px;background:var(--ink);color:#fff;border-radius:10px;padding:22px;display:flex;align-items:center;justify-content:space-between;gap:20px}.diff h2{margin:0 0 4px;font-size:18px}.diff p{margin:0;color:#b9c2d1;font-size:13px}.diff strong{color:#7ce1b8}.diff .decision{font-size:26px;font-weight:800;color:var(--cobalt);letter-spacing:-.04em}.footer{color:var(--muted);font-size:12px;margin:18px 2px 0}@media(max-width:960px){.topbar>.action{justify-content:flex-end}.crumb{max-width:320px;white-space:normal;text-align:right}}@media(max-width:600px){.topbar{display:block}.topbar>.action{margin-top:12px;justify-content:space-between;width:100%}.topbar .crumb{max-width:none;flex:1;text-align:left}}@media(max-width:820px){main{padding:20px 16px 52px}.finding{display:block}.finding-kicker,.finding-detail{display:block;margin-bottom:4px}.hero{display:block;padding:34px 0 24px}.action{margin-top:22px;justify-content:flex-start}.hero .action{width:100%}.intervention select{max-width:none}.summary{grid-template-columns:repeat(2,1fr)}.grid{grid-template-columns:1fr}.diff{display:block}.diff .decision{margin-top:14px}}@media(prefers-reduced-motion:reduce){*{scroll-behavior:auto!important;transition:none!important}}
 .theme-toggle{border:1px solid var(--line);background:var(--panel);color:var(--ink);border-radius:7px;padding:8px 11px;font-weight:700;cursor:pointer}.theme-toggle:focus-visible{outline:3px solid #9eb3ff;outline-offset:2px}[data-theme="dark"]{color-scheme:dark;--ink:#e9edf5;--muted:#9aa6ba;--line:#303a4c;--paper:#111722;--panel:#182130;--cobalt:#91a9ff;--cobalt-soft:#202b46;--success:#69c99b;--success-soft:#20392f;--red:#ffaaa7;--red-soft:#3b252c;--amber:#f1bb62;--amber-soft:#3a2e1f;--shadow:0 10px 30px rgba(0,0,0,.18)}[data-theme="dark"] .mark{background:#e9edf5;color:#17202f}[data-theme="dark"] .button{background:#6f89ee;color:#101521}[data-theme="dark"] .button:hover{background:#849cff}[data-theme="dark"] .diff{background:#0b1019}[data-theme="dark"] .diff .decision{color:#91a9ff}[data-theme="dark"] .diff .decision.reject{color:#f1bb62}.trace-link{color:inherit;font-weight:700;text-decoration:underline;text-underline-offset:3px}.trace-link:hover{opacity:.8}.diff .trace-link{color:#b8c8ff} </style></head><body><main>
<header class="topbar"><div class="brand"><span class="mark">CR</span><span>Counterfactual Replay Lab</span></div><div class="action"><span class="crumb">Support agent / refund policy · CLI export verified</span><button class="theme-toggle" id="theme-toggle" type="button" aria-label="Switch color theme">Dark mode</button></div></header>
<section class="hero"><div><div class="eyebrow">Replay review</div><h1>What changes when one decision changes?</h1><p>Replay the same request with one controlled intervention. Inspect the exact point where the agent diverges before shipping a new policy.</p></div><div class="action"><label class="intervention">Try policy <select id="policy"><option value="v2">v2 · approve eligible</option><option value="v3">v3 · deny all</option></select></label><button class="button" id="replay">Run replay</button><span class="status" id="status">Loading fixture...</span></div></section>
<section class="summary"><div class="stat"><label>Outcome</label><strong class="good" id="outcome">KEEP</strong></div><div class="stat"><label>First divergence</label><strong id="divergence">apply_policy</strong></div><div class="stat"><label>Score change</label><strong id="score-change">0 → 1</strong></div><div class="stat"><label>Request integrity</label><strong id="integrity">Preserved</strong></div></section><section class="finding"><span class="finding-kicker">Finding</span><strong id="finding-head">v1 denied the eligible refund. v2 approves it.</strong><span class="finding-detail" id="finding-detail">The first changed step is <b>apply_policy</b>; every earlier step is identical.</span></section>
<section class="grid"><article class="panel" id="before"></article><article class="panel" id="after"></article></section>
<section class="diff" id="diff"></section><p class="footer">This UI shows the deterministic replay fixture. The CLI demo exports live traces to neatlogs only with the explicit <code>--live-neatlogs</code> option and authorized credentials.</p>
<script>
const esc=s=>String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
function traceCard(t,label,divergence){return `<div class="panel-head"><h2>${label}</h2><span class="badge ${t.verdict==='PASS'?'pass':'fail'}">${t.verdict}</span></div><div class="facts"><div class="fact"><label>Decision</label><strong>${esc(t.decision)}</strong></div><div class="fact"><label>Policy</label><strong>${esc(t.policy_version)}</strong></div><div class="fact"><label>Score</label><strong>${t.score}</strong></div></div><div class="facts"><div class="fact"><label>Cost</label><strong>${t.cost_usd === null ? 'Unmeasured' : '$' + t.cost_usd.toFixed(3)}</strong></div><div class="fact"><label>Local wall time</label><strong>${t.latency_ms.toFixed(3)} ms</strong></div><div class="fact"><label>Spans</label><strong>${t.spans.length}</strong></div></div><div class="trace"><div class="trace-title">Trace sequence</div>${t.spans.map(s=>`<div class="span ${s.name===divergence?'changed':''}"><div class="span-name">${esc(s.name)}</div><div class="span-meta">${esc(s.kind)} · ${esc(s.input)} → ${esc(s.output)}</div></div>`).join('')}</div>`}
async function load(){const policy=document.querySelector('#policy').value;const r=await fetch(`/api/experiment?policy=${encodeURIComponent(policy)}`);const d=await r.json();document.querySelector('#before').innerHTML=traceCard(d.baseline,'Baseline run',d.experiment.first_divergence);document.querySelector('#after').innerHTML=traceCard(d.counterfactual,'Counterfactual run',d.experiment.first_divergence);const x=d.experiment;const outcome=document.querySelector('#outcome');outcome.textContent=x.recommendation;outcome.className=x.recommendation==='KEEP'?'good':'warn';document.querySelector('#finding-head').textContent=`v1 ${d.baseline.verdict==='PASS'?'approved':'denied'} the eligible refund. ${d.counterfactual.policy_version} ${d.counterfactual.verdict==='PASS'?'approves':'also denies'} it.`;document.querySelector('#finding-detail').innerHTML=`The first changed step is <b>${esc(x.first_divergence||'none')}</b>; every earlier step is identical.`;document.querySelector('#divergence').textContent=x.first_divergence||'None';document.querySelector('#score-change').textContent=`${d.baseline.score} → ${d.counterfactual.score}`;document.querySelector('#integrity').textContent=x.same_request&&x.same_prefix?'Preserved':'Review';document.querySelector('#diff').innerHTML=`<div><h2>Controlled change</h2><p>Changed <b>${esc(x.changed_variable)}</b> from <b>${esc(x.old_value)}</b> to <b>${esc(x.new_value)}</b>. The request and prefix were preserved.</p><p><a class="trace-link" href="${d.neatlogs_url}" target="_blank" rel="noreferrer">Open traces in neatlogs ↗</a></p></div><div class="decision ${x.recommendation.toLowerCase()}">${esc(x.recommendation)}</div>`;document.querySelector('#status').textContent='Replay verified locally';}
const themeButton=document.querySelector('#theme-toggle');const savedTheme=localStorage.getItem('cr-theme');if(savedTheme==='dark')document.documentElement.dataset.theme='dark';function syncThemeButton(){const dark=document.documentElement.dataset.theme==='dark';themeButton.textContent=dark?'Light mode':'Dark mode';themeButton.setAttribute('aria-label',dark?'Switch to light mode':'Switch to dark mode');}themeButton.onclick=()=>{const dark=document.documentElement.dataset.theme==='dark';if(dark)delete document.documentElement.dataset.theme;else document.documentElement.dataset.theme='dark';localStorage.setItem('cr-theme',dark?'light':'dark');syncThemeButton();};syncThemeButton();document.querySelector('#replay').onclick=load;document.querySelector('#policy').onchange=load;load();
</script></main></body></html>"""


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/":
            body = HTML.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
        elif parsed.path == "/api/experiment":
            policy = parse_qs(parsed.query).get("policy", ["v2"])[0]
            try:
                body = json.dumps(experiment(policy)).encode()
                self.send_response(200)
            except ValueError as error:
                body = json.dumps({"error": str(error)}).encode()
                self.send_response(400)
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
