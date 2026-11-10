# Compute queue

An optional local controller with Windows workers and user-scoped Linux remote
workers. It executes commands as the configured operating-system user. It is
not a sandbox for untrusted code or a multi-user authorization service.

## State and clients

The default state directory is `<Baton checkout>/.state/compute`. `CQ_STATE`
overrides it; `BATON_STATE` supplies the parent when `CQ_STATE` is absent. Both
clients must use the same state for a shared queue. Keep state, credentials,
receipts and output private and out of version control.

For Codex, add a local stdio MCP server pointing to
`<checkout>/mods/compute-queue/bin/codex_bridge.py` with Python. For example:

```powershell
codex mcp add compute-queue -- python -X utf8 C:\Work\baton\mods\compute-queue\bin\codex_bridge.py
```

On Windows, native completion delivery also needs `BATON_CODEX_EXE` pointing to
the installed native `codex.exe`, rather than an npm `.cmd` shim. The bridge
uses the client's supported `queue` command; use normal reconnect/new-session
loading after updates. If the installed client lacks that command, inspect
results explicitly with `bt compute --refresh` rather than assuming delivery.
Start sessions through Baton so root, owner and account identity are available.

Claude's native integration uses `hooks/register.tsx` and the `claude-code`
plugin API. On a compatible CLI, configure `CLAUDE_CODE_PLUGIN_DIRS` to this
module, and optionally `CQ_PYTHON` to your Python executable. Validate with
`claude plugin test <absolute module path>`. This API is version-dependent;
ordinary MCP support alone does not establish support for this native plugin.

## Local fleet jobs

No host address is built in. A Windows-only job can use `where: "fleet"`,
`platform: "windows"`, and `candidates: ["laptop"]`. The worker defaults to four
CPU slots and a 6 GiB physical/commit-memory reserve. It uses Job Objects for
CPU, private/committed-memory limits, process-tree lifetime and cancellation.
The memory cap is not a hard resident-RAM limit.

Legacy `where: "laptop"` jobs need an explicitly configured compatible
`heavyrun` helper; that historical external helper is not bundled. Use fleet for
the self-contained Windows executor. Existing installations can retain helper
paths in `<state>/runtime.local.json` (`remoterun`, `heavyrun`, `heavy_lock`) or
the corresponding `CQ_*` environment variables. All executors sharing a machine
must share the same heavy-lock convention.

On upgrade, keep `CQ_BATON_DIR` or configure the actual existing helper/lock in
`runtime.local.json` before enabling laptop fleet work. An inherited queue with
persisted work and no explicit convention is held for migration; it does not
silently select a different lock. A fresh queue records its default convention.

## Linux host setup

Use a trusted SSH alias with verified host keys and non-interactive key access.
The remote user needs Python 3.12+, Git, Bash, `flock`, systemd user services and
cgroup accounting. Configure only machines and accounts you control. For jobs
that must outlive an SSH connection, an administrator enables lingering for
that user with `loginctl enable-linger USER`.

Copy the supplied `host/heavy`, `host/lanerun`, `host/deferred.py` and
`host/install_deferred.py` to the host. Install the first two in `~/bin` as
executable files and create a private `~/q` directory. Run the installer from
the directory containing both Python files, selecting budgets for that machine:

```bash
python3 install_deferred.py --pool-slots 8 --load-cap 8 --ram-floor-gb 4
```

This installs a user-scoped helper and timer. It requires at least three pool
slots and refuses to replace a different installed policy; settle existing jobs
before changing a host policy. Fleet currently reserves two of the configured
slots for foreground work. Do not copy an example's budget without checking the
machine. `lanerun` checks for at least 10 GiB of free disk space.

Create `<state>/host.conf.json` locally. The primary fleet ID is `ember` for
compatibility; this is an internal identifier, not a preconfigured computer.
Replace the SSH alias and machine ID with your host's values:

```json
{
  "host_id": "ember",
  "name": "Build server",
  "address": "build-worker",
  "machine_id": "REPLACE_WITH_ETC_MACHINE_ID",
  "portable_runner": true,
  "slots": 8,
  "load_cap": 8,
  "ram_floor_gb": 4
}
```

The source transport and host identity must agree. The current automatic fleet
supports the laptop and two remote identities (`ember` and `hearth`, with
`hestia` as an alias). Expanding to arbitrary host counts is future work. The
extra host uses the existing `hosts.json` registry and its own `host.conf.json`;
do not silently repoint an identity that has unresolved jobs.

## Submit, inspect and review

For compatible work, submit the actual script with explicit resource needs:

```json
{
  "where": "fleet",
  "platform": "linux",
  "requires": ["python"],
  "command": "python3 checks/extended.py",
  "slots": 2,
  "memory_gb": 1,
  "memory_max_gb": 2,
  "gets": ["evidence/results.json"]
}
```

Codex additionally supplies the absolute owned worktree `root`; Claude derives
it. Fleet chooses a compatible destination and pins it before staging. CPU and
RAM are independent; admission reserves the full enforced memory cap. Do not
wrap fleet commands in `heavy`/`heavyrun` again. GPU work stays on an explicitly
qualified backend. Source is frozen, and the command runs in a separate clone.

`defer: "spare"` or `"overnight"` selects remote execution. Short spare jobs can
use spare capacity anytime; long spare/overnight work uses the host's configured
window (00:00–08:00 Europe/London by default). Confirm `deferred_registered: true`
before relying on laptop sleep. Foreground reserves and IANA timezone/HH:MM windows
must match the installed host policy. Upgrade the helper unchanged first; intentional
policy edits use its guarded `--update-policy` operation while drained.

Continue independent work after submission. Use `bt compute --refresh`, verify
the source SHA, exit, logs and actual returned files, then use
`bt compute --reviewed ID --resolution checked|follow-up|superseded`. Open images
and inspect video frames before approving creative results. Missing artifacts
remain visible. New artifact jobs default to 256 MiB per file and 1 GiB per job.
`artifact_max_mb` allows up to 1024 MiB and `artifact_total_mb` up to 4096 MiB;
the per-file limit cannot exceed the total. Capture uses bounded binary streams,
SHA-256 verification and a recovery journal, without overwriting completed files.
Existing legacy jobs retain their original limits. Split artifact jobs are refused;
new local artifact work requires `where: "fleet"`, `platform: "windows"` and
honest CPU/RAM requirements. `gets` accepts at most 64 portable relative filenames.
Artifact-bearing `where: "host"` requests use that host's contained fleet backend,
with at least one CPU slot, disk scratch and no `self_heavy` wrapper. The shared
admission floor covers managed reservations and current available memory; manual
processes and legacy zero-slot work can still change pressure after a sample.

New fleet/artifact jobs use `cq-v2--` IDs and isolated `queue-v2` stores, so old
loaded clients cannot bypass the new checks. Both current clients and Baton pickup
read old and new generations. Windows uses one shared pool controller with bounded
concurrent Job Objects, fencing older workers through the existing physical lock.

`bt fleet --details` explains cached slot budgets and blockers;
`status({"view":"fleet"})` returns the compact client view. The controller exits
when idle, so an old sample is marked stale. Late or uncertain results retain
their job ID; inspect and reconcile rather than submitting the command again.

Ordinary host cancellation verifies the session and inherited job identity after
opening a Linux pidfd, then signals only through that stable handle. A reused
numeric session ID cannot authorize termination. Missing identity or pidfd support
leaves the stop pending; reservations remain held until shutdown is confirmed.

If Codex reports `notice_delivery.state: "uncertain"`, the external client may
already have accepted the completion message. The bridge retains that owner's
attempt and holds automatic resend; inspect the job through status or morning
pickup. This avoids claiming exactly-once message delivery from an API without
an idempotency key. Receipt acknowledgement is separate from human/agent review.
