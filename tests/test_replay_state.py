import unittest
from counterfactual_replay_lab.core import RefundAgent, ReplayEngine, compare


class CorrectnessRegressionTests(unittest.TestCase):
    def test_ineligible_order_is_denied_and_scores_pass(self):
        trace = RefundAgent().run("Order 7 is ineligible for a refund", "v2")
        self.assertEqual(trace.decision, "deny_refund")
        self.assertEqual(trace.verdict, "PASS")

    def test_correct_denial_is_preserved_by_replay(self):
        agent = RefundAgent()
        baseline = agent.run("Order 7 is ineligible for a refund", "v1")
        replay = ReplayEngine(agent.run).replay(baseline, changed_variable="policy_version", new_value="v2")
        self.assertEqual(baseline.verdict, "PASS")
        self.assertEqual(replay.verdict, "PASS")
        result = compare(baseline, replay, changed_variable="policy_version", old_value="v1", new_value="v2")
        self.assertNotEqual(result.recommendation, "KEEP")


class StateAndEffectTests(unittest.TestCase):
    def setUp(self):
        self.agent = RefundAgent()
        self.baseline = self.agent.run("Order 7 is eligible for a refund", "v1")
        self.engine = ReplayEngine(self.agent.run)

    def replay(self, baseline=None, **kwargs):
        return self.engine.replay(baseline or self.baseline,
                                 changed_variable="policy_version", new_value="v2", **kwargs)

    def test_suffix_does_not_call_full_runner_and_repeated_replays_are_isolated(self):
        from unittest.mock import patch
        with patch.object(self.agent, 'lookup_order', side_effect=AssertionError('prefix lookup forbidden')):
            first, second = self.replay(), self.replay()
        self.assertEqual(first.trace_id, second.trace_id)
        self.assertEqual(first.ledger_after, second.ledger_after)
        self.assertEqual(len(first.ledger_after), 1)
        self.assertNotIn('lookup_order', first.executed_tools)
        self.assertEqual(self.baseline.ledger_after, ())

    def test_mutated_request_prefix_or_missing_state_is_refused(self):
        from dataclasses import replace
        from counterfactual_replay_lab.core import Span
        for bad in [replace(self.baseline, request='another request'),
                    replace(self.baseline, spans=(Span('different', 'agent', '', ''),) + self.baseline.spans[1:]),
                    replace(self.baseline, replay_state=None),
                    replace(self.baseline, verdict='PASS', score=1)]:
            with self.subTest(trace=bad):
                with self.assertRaises(ValueError):
                    self.replay(bad)

    def test_changed_dependency_inputs_versions_order_errors_and_effects_refused(self):
        from dataclasses import replace
        from counterfactual_replay_lab.replay_state import canonical
        state = self.baseline.replay_state
        obs = state.observations[0]
        for changes in [dict(inputs=canonical({'request':'changed'})), dict(version='v9'),
                        dict(sequence=1), dict(error='timeout'), dict(effect='external_write'),
                        dict(output=canonical('invalid'))]:
            changed = replace(state, observations=(replace(obs, **changes),))
            # Even a fresh checksum cannot make incompatible recorded input valid.
            bad = replace(self.baseline, replay_state=changed, state_fingerprint=changed.fingerprint)
            with self.subTest(changes=changes):
                with self.assertRaises(ValueError):
                    self.replay(bad)

    def test_changed_state_without_fresh_fingerprint_refused(self):
        from dataclasses import replace
        bad = replace(self.baseline, replay_state=replace(self.baseline.replay_state, scenario_id='changed'))
        with self.assertRaisesRegex(ValueError, 'Captured state changed'):
            self.replay(bad)

    def test_missing_observations_refused(self):
        from dataclasses import replace
        state = replace(self.baseline.replay_state, observations=())
        with self.assertRaisesRegex(ValueError, 'dependency state missing'):
            self.replay(replace(self.baseline, replay_state=state, state_fingerprint=state.fingerprint))

    def test_unknown_policy_and_noop_refused(self):
        for value in ['v99', '', 'v1']:
            with self.assertRaises(ValueError):
                self.engine.replay(self.baseline, changed_variable='policy_version', new_value=value)

    def test_multi_variable_intervention_refused(self):
        with self.assertRaises(TypeError):
            self.replay(request='Order 99 is eligible for a refund')

    def test_cancellation_before_lookup_and_before_effect(self):
        from counterfactual_replay_lab.replay_state import EffectLedger
        ledger = EffectLedger()
        try:
            with self.assertRaisesRegex(RuntimeError, 'Cancelled before lookup'):
                self.agent.run(self.baseline.request, 'v2', ledger=ledger, cancelled=lambda:True)
            calls = iter([False, True])
            with self.assertRaisesRegex(RuntimeError, 'Cancelled before effect'):
                self.agent.run(self.baseline.request, 'v2', ledger=ledger, cancelled=lambda:next(calls))
            with self.assertRaisesRegex(RuntimeError, 'Cancelled before effect'):
                self.replay(cancelled=lambda:True)
            self.assertEqual(ledger.snapshot(), ())
        finally:
            ledger.close()

    def test_crash_after_effect_reopen_and_retry_avoids_duplicate(self):
        import tempfile
        from pathlib import Path
        from counterfactual_replay_lab.replay_state import EffectLedger
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / 'ledger.sqlite')
            ledger = EffectLedger(path)
            with self.assertRaisesRegex(RuntimeError, 'Injected crash'):
                self.agent.run(self.baseline.request, 'v2', ledger=ledger, crash_after_effect=True)
            ledger.close()
            ledger = EffectLedger(path)
            try:
                trace = self.agent.run(self.baseline.request, 'v2', ledger=ledger)
                self.assertEqual(len(ledger.snapshot()), 1)
                self.assertEqual(trace.verdict, 'PASS')
                replay = self.engine.replay(trace, changed_variable='policy_version', new_value='v3')
                self.assertEqual(len(replay.ledger_after), 1)
                self.assertEqual(len(ledger.snapshot()), 1)
            finally:
                ledger.close()

    def test_effect_conflict_is_refused_without_overwrite(self):
        from counterfactual_replay_lab.replay_state import EffectLedger
        ledger = EffectLedger()
        try:
            ledger.apply('one', 'a')
            with self.assertRaisesRegex(ValueError, 'different effect'):
                ledger.apply('one', 'b')
            self.assertEqual(ledger.snapshot(), (('one','a'),))
        finally:
            ledger.close()

    def test_independent_oracle_detects_wrong_recorded_observation(self):
        from counterfactual_replay_lab.core import Scenario
        from counterfactual_replay_lab.replay_state import Observation
        scenario = Scenario('wrong-tool', self.baseline.request, 'ineligible', 'deny_refund')
        trace = self.agent.run(scenario.request, 'v2', scenario=scenario,
                              observations=(Observation.lookup(scenario.request, 'eligible'),))
        self.assertEqual(trace.verdict, 'FAIL')

    def test_unknown_truth_never_receives_keep(self):
        trace = self.agent.run('Ambiguous order request', 'v1')
        replay = self.engine.replay(trace, changed_variable='policy_version', new_value='v2')
        result = compare(trace, replay, changed_variable='policy_version', old_value='v1', new_value='v2')
        self.assertEqual(result.recommendation, 'INVESTIGATE')
        self.assertEqual(replay.decision, 'review_refund')
        self.assertIsNone(trace.cost_usd)

    def test_comparison_rejects_intervention_metadata_lie(self):
        replay = self.replay()
        with self.assertRaises(ValueError):
            compare(self.baseline, replay, changed_variable='policy_version', old_value='v3', new_value='v2')


class ExportTests(unittest.TestCase):
    def test_export_roundtrip_replays_identically(self):
        import json
        from counterfactual_replay_lab.core import Trace
        agent = RefundAgent()
        original = agent.run("Order 7 is eligible for a refund", "v1")
        restored = Trace.from_dict(json.loads(json.dumps(original.to_dict())))
        self.assertEqual(original, restored)
        replay = ReplayEngine(agent.run).replay(restored, changed_variable="policy_version", new_value="v2")
        self.assertEqual(replay.verdict, "PASS")
