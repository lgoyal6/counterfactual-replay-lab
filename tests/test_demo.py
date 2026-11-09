"""The local demo drives the real engine: edits change execution, saved runs reproduce."""
import json
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from counterfactual_replay_lab.demo import DemoState, make_handler


class DemoHTTP(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.state = DemoState(Path(self.tmp.name))
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(self.state))
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.state.store.close()
        self.tmp.cleanup()

    def call(self, path, body=None):
        data = None if body is None else json.dumps(body).encode()
        req = Request(self.url + path, data=data, headers={"Content-Type": "application/json"})
        with urlopen(req) as response:
            return json.load(response)

    def test_page_and_state(self):
        with urlopen(self.url + "/") as response:
            self.assertIn("Dependency replay", response.read().decode())
        state = self.call("/api/state")
        self.assertIn("1042", state["env"]["orders"])

    def test_policy_edit_changes_real_execution_and_divergence(self):
        same = self.call("/api/replay", {"order": "1042", "customer": "c-77"})
        self.assertEqual(same["execution"], "live")
        self.assertTrue(same["oracle"]["matches_full_rerun"])
        self.assertIsNone(same["explanation"]["first_divergence"])
        self.assertEqual(same["counters"]["executed"], 0)
        changed = self.call("/api/replay", {"order": "1042", "customer": "c-77",
                                            "policy": {"auto_approve_max_cents": 10000}})
        self.assertTrue(changed["oracle"]["matches_full_rerun"])
        self.assertEqual(changed["explanation"]["first_divergence"]["node"], "route")
        self.assertGreater(changed["counters"]["executed"], 0)
        self.assertLess(changed["counters"]["executed"], changed["full_rerun_operations"])
        sources = {n["id"]: n["source"] for n in changed["run"]["nodes"]}
        self.assertEqual(sources["risk_check"], "reused")
        self.assertEqual(sources["route"], "executed")
        self.assertEqual(len(changed["explanation"]["effect_diff"]["added"]), 1)

    def test_observation_edit_and_added_node(self):
        obs = {"found": True, "amount_cents": 12900, "days_since_delivery": 60, "category": "apparel",
               "delivered": True}
        out = self.call("/api/replay", {"observation_edits": {"order_lookup": obs}, "fraud_hold": 10})
        kinds = {n["node"]: n["kind"] for n in out["explanation"]["nodes"]}
        self.assertEqual(kinds["fraud_hold"], "added")
        self.assertEqual(kinds["send_denial"], "activated")
        self.assertTrue(out["oracle"]["matches_full_rerun"])

    def test_saved_run_reopens_as_recorded_and_reproduces(self):
        out = self.call("/api/replay", {"risk_version": "v2", "policy": {"max_risk": 10}})
        run_id = out["run"]["run_id"]
        recorded = self.call(f"/api/run/{run_id}")
        self.assertEqual(recorded["execution"], "recorded")
        self.assertEqual(recorded["run"]["nodes"], out["run"]["nodes"])
        rep = self.call(f"/api/reproduce/{run_id}", {})
        self.assertTrue(rep["reproduced"], rep)
        self.assertNotEqual(rep["reproduction_run"], run_id)

    def test_bounds_and_invalid_input(self):
        for body in ({"policy": {"evil": 1}},
                     {"graph": {"format": 1, "env_version": "x", "nodes": [{"id": "a", "op": "os.system",
                                                                            "op_version": "v1"}]}},
                     {"graph": {"format": 1, "env_version": "x", "nodes": [
                         {"id": f"n{i}", "op": "syn.source", "op_version": "v1", "params": {"salt": i}}
                         for i in range(65)]}},
                     {"policy": {"risk_rounds": 10_000_000}},
                     {"order": "nope"}):
            with self.subTest(body=str(body)[:60]), self.assertRaises(HTTPError) as err:
                self.call("/api/replay", body)
            self.assertEqual(err.exception.code, 400)
            err.exception.close()
        with self.assertRaises(HTTPError) as err:
            urlopen(Request(self.url + "/api/replay", data=b"x" * 40000))
        self.assertEqual(err.exception.code, 413)
        err.exception.close()


if __name__ == "__main__":
    unittest.main()
