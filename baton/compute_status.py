"""Display the local fleet cache; no host probes or background processes."""
from __future__ import annotations

import importlib.util
import json
import time

from baton import config


def _telemetry():
    path = config.BATON_HOME / 'mods' / 'compute-queue' / 'bin' / 'fleet_telemetry.py'
    spec = importlib.util.spec_from_file_location('baton_fleet_telemetry', path)
    if spec is None or spec.loader is None:
        raise RuntimeError('fleet telemetry module is unavailable')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def run(args):
    telemetry = _telemetry()
    state = config.STATE_DIR / 'compute'
    previous = None
    try:
        while True:
            snapshot = telemetry.summarize_generations(state)
            text = json.dumps(snapshot, separators=(',', ':'), allow_nan=False) if args.json else snapshot['line']
            if getattr(args, 'details', False) and not args.json:
                for host, device in snapshot['devices'].items():
                    number = lambda key: '?' if device.get(key) is None else str(device[key])
                    text += (f"\n{host}: {number('slots_used')}/{number('slots_total')} usable CPU slots; "
                             f"{number('slots_reserved')} reserved; {number('slots_configured')} configured")
                    if device.get('reason'):
                        text += '; ' + device['reason']
                    if device.get('stale'):
                        text += ' (cached sample is stale)'
            if text != previous:
                print(text, flush=True)
                previous = text
            if not args.watch:
                break
            time.sleep(1)
    except KeyboardInterrupt:
        return 0
    return 0


def register(sub):
    parser = sub.add_parser('fleet', help='show the local cached fleet status')
    parser.add_argument('--json', action='store_true', help='print a machine-readable snapshot')
    parser.add_argument('--details', action='store_true', help='explain usable, reserved and configured slots and cached blockers')
    parser.add_argument('--watch', action='store_true', help='refresh once per second until Ctrl-C')
    parser.set_defaults(func=run)
