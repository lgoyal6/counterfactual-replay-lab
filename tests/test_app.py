import importlib.util
import json
from pathlib import Path
import threading
import unittest
from urllib.error import HTTPError
from urllib.request import urlopen
from http.server import ThreadingHTTPServer

spec = importlib.util.spec_from_file_location('replay_app', Path(__file__).parents[1] / 'scripts/app.py')
app = importlib.util.module_from_spec(spec)
spec.loader.exec_module(app)


class AppTests(unittest.TestCase):
    def test_policy_choices_and_invalid_policy_http_response(self):
        server = ThreadingHTTPServer(('127.0.0.1', 0), app.Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        url = f'http://127.0.0.1:{server.server_port}'
        try:
            for policy, verdict in [('v2','PASS'), ('v3','FAIL')]:
                with urlopen(f'{url}/api/experiment?policy={policy}') as response:
                    data = json.load(response)
                self.assertEqual(data['counterfactual']['verdict'], verdict)
                self.assertIsNone(data['counterfactual']['cost_usd'])
            with self.assertRaises(HTTPError) as error:
                urlopen(f'{url}/api/experiment?policy=v999')
            self.assertEqual(error.exception.code, 400)
            self.assertIn('Unknown policy', json.load(error.exception)['error'])
            error.exception.close()
        finally:
            server.shutdown()
            server.server_close()
            thread.join()
