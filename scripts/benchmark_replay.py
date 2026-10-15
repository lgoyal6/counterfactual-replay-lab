"""Compare full reruns with captured suffix replay on frozen local orders.

core.py supplies the replay path and independent fixture decisions.
Write paired timings and tool counts privately; these are not live costs.
"""
import argparse
from dataclasses import asdict
from hashlib import sha256
import json
from pathlib import Path
import platform
import os
import random
import statistics
import sys
from time import perf_counter_ns

from counterfactual_replay_lab.core import RefundAgent, ReplayEngine, Scenario, compare


def study(cases, repetitions, warmup):
    agent = RefundAgent()
    engine = ReplayEngine(agent.run)
    rows = []
    rng = random.Random(20261015)
    for case in cases:
        scenario = Scenario(**case)
        baseline = agent.run(scenario.request, 'v1', scenario=scenario)
        state = baseline.replay_state
        methods = {
            'full': lambda: agent.run(scenario.request, 'v2', scenario=scenario, observations=state.observations),
            'suffix': lambda: engine.replay(baseline, changed_variable='policy_version', new_value='v2'),
        }
        for _ in range(warmup):
            for f in methods.values():
                f()
        samples = []
        for repeat in range(repetitions):
            order = list(methods)
            rng.shuffle(order)
            traces, elapsed = {}, {}
            for mode in order:
                start = perf_counter_ns()
                traces[mode] = methods[mode]()
                elapsed[mode] = perf_counter_ns() - start
            full, suffix = traces['full'], traces['suffix']
            if (full.decision, full.verdict, full.ledger_after) != (suffix.decision, suffix.verdict, suffix.ledger_after):
                raise AssertionError('Full rerun and suffix disagree')
            expected_effects = 1 if scenario.expected_decision == 'approve_refund' else 0
            if any(len(t.ledger_after) != expected_effects or len(dict(t.ledger_after)) != expected_effects for t in traces.values()):
                raise AssertionError('Unintended or duplicate local effects')
            if suffix.spans[:2] != baseline.spans[:2]:
                raise AssertionError('Replay prefix changed')
            samples.append({'repeat':repeat, 'execution_order':order, 'wall_ns':elapsed,
                            'tool_calls':{m:len(t.executed_tools) for m,t in traces.items()}})
        result = compare(baseline, suffix, changed_variable='policy_version', old_value='v1', new_value='v2')
        rows.append({'scenario':case,'baseline':baseline.to_dict(),'full':full.to_dict(),
                     'suffix':suffix.to_dict(),'experiment':asdict(result),'samples':samples})
    return rows


def summarize(rows):
    deltas = [statistics.median(s['wall_ns']['suffix'] - s['wall_ns']['full'] for s in r['samples']) for r in rows]
    rng = random.Random(20261015)
    boot = sorted(statistics.mean(rng.choices(deltas,k=len(deltas))) for _ in range(2000))
    denials = [r for r in rows if r['scenario']['expected_decision']=='deny_refund']
    return {'n_independent_synthetic_orders':len(rows),
            'baseline_correct':sum(r['baseline']['verdict']=='PASS' for r in rows),
            'suffix_correct':sum(r['suffix']['verdict']=='PASS' for r in rows),
            'unsafe_keep':sum(r['experiment']['recommendation']=='KEEP' and r['suffix']['decision']!=r['scenario']['expected_decision'] for r in rows),
            'correct_denials_preserved':sum(r['suffix']['verdict']=='PASS' for r in denials),
            'denial_controls':len(denials),
            'scenario_tool_calls_saved':sum(r['samples'][0]['tool_calls']['full']-r['samples'][0]['tool_calls']['suffix'] for r in rows),
            'paired_mean_suffix_minus_full_ns':statistics.mean(deltas),
            'cluster_bootstrap_95pct_ns':[boot[49],boot[1949]],
            'unit_of_uncertainty':'synthetic order, not repetition',
            'cost_usd':None,
            'limitations':['Synthetic local CPU fixture only','One read-only dependency and local SQLite effects',
                            'No inference or remote latency/cost measured','Same-observation full rerun is not a live dependency query',
                            'Source truth labels are explicit frozen fixtures, not real customer outcomes']}


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--cases',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--repetitions',type=int,default=20)
    p.add_argument('--warmup',type=int,default=2)
    args=p.parse_args()
    cases=json.loads(args.cases.read_text())
    if len(cases)!=100 or len({c['scenario_id'] for c in cases})!=100 or len({c['request'] for c in cases})!=100:
        p.error('Study requires 100 distinct frozen scenario IDs and requests')
    if args.repetitions<1 or args.warmup<0:
        p.error('Invalid repetition/warmup counts')
    rows=study(cases,args.repetitions,args.warmup)
    out={'cases_sha256':sha256(args.cases.read_bytes()).hexdigest(),
         'environment':{'python':sys.version,'platform':platform.platform(),
                        'machine':platform.machine(),'cpu_count':os.cpu_count(),'load_average':os.getloadavg(),
                        'external_load':'Other portfolio workers active; no process/CPU affinity isolation'},
         'protocol':{'warmup':args.warmup,'repetitions':args.repetitions,'seed':20261015,
                     'concurrency':1,'cache':'warm process, new isolated SQLite ledger per invocation'},
         'summary':summarize(rows),'rows':rows}
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(out,indent=2)+'\n')
    print(json.dumps(out['summary'],indent=2))


if __name__=='__main__':
    main()
