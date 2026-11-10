"""The fleet CLI never contacts hosts and reads isolated BATON_STATE only."""
import argparse
import atexit
import contextlib
import io
import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

_STATE = tempfile.mkdtemp(prefix='baton-fleet-tests-')
os.environ["BATON_STATE"] = _STATE
atexit.register(shutil.rmtree, _STATE, True)
from baton import compute_status, config


class FleetCliTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.state = Path(self.tmp.name)
        self.patch = patch.object(config, 'STATE_DIR', self.state)
        self.patch.start()
        self.addCleanup(self.patch.stop)
        parser = argparse.ArgumentParser()
        compute_status.register(parser.add_subparsers(dest='command', required=True))
        self.parser = parser

    def test_json_cache_snapshot(self):
        path = self.state / 'compute/fleet'
        path.mkdir(parents=True)
        (path / 'capacity.json').write_text(json.dumps({'at': 1700000000, 'devices': {}}), encoding='utf-8')
        out = io.StringIO()
        args = self.parser.parse_args(['fleet', '--json'])
        with contextlib.redirect_stdout(out):
            self.assertEqual(args.func(args), 0)
        self.assertIn('line', json.loads(out.getvalue()))
        self.assertFalse((path / 'events.jsonl').exists())

    def test_watch_is_explicit_and_interruptible(self):
        args = self.parser.parse_args(['fleet', '--watch'])
        out = io.StringIO()
        with contextlib.redirect_stdout(out), patch.object(compute_status.time, 'sleep', side_effect=[None, KeyboardInterrupt]) as sleep:
            self.assertEqual(args.func(args), 0)
        self.assertEqual(len(out.getvalue().splitlines()), 1)  # unchanged cache does not flood scrollback
        self.assertEqual(sleep.call_count, 2)
        sleep.assert_called_with(1)

    def test_details_distinguish_reserve_from_configured_capacity(self):
        path = self.state / 'compute/fleet'
        path.mkdir(parents=True)
        (path / 'capacity.json').write_text(json.dumps({'at': 1700000000, 'devices': {
            'hearth': {'slots_used': 1, 'slots_total': 6, 'slots_reserved': 2, 'slots_configured': 8}
        }}), encoding='utf-8')
        out = io.StringIO()
        args = self.parser.parse_args(['fleet', '--details'])
        with contextlib.redirect_stdout(out):
            self.assertEqual(args.func(args), 0)
        self.assertIn('hearth: 1/6 usable CPU slots; 2 reserved; 8 configured', out.getvalue())
        self.assertIn('cached sample is stale', out.getvalue())
        self.assertIn('laptop: ?/? usable CPU slots; ? reserved; ? configured', out.getvalue())


if __name__ == '__main__':
    unittest.main()
