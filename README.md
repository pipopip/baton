# Baton

Baton keeps Claude Code and Codex work moving across sessions, worktrees and machines. A small worktree capsule restores the task after a handoff; owned job receipts bring compute results back to the workstream that submitted them.

This is an experimental, Windows-first public snapshot. It contains the Python CLI and optional compute queue. Python 3.12+, Git and your own installed/signed-in coding clients are required. PowerShell 7 and Windows Terminal are used by the Windows launcher. See the [support matrix](SUPPORT.md) for the limits.

## What it does

- Claims one worktree per live session and carries a bounded task capsule through supported lifecycle hooks.
- Starts either client in an isolated worktree, with explicit resume/fresh behavior.
- Records job source, owner and worktree identity so late results cannot silently attach to a recreated directory.
- Schedules compatible jobs using separate CPU and RAM budgets, Windows Job Objects or Linux systemd limits, and declared tool requirements.
- Supports remotely registered spare-time/overnight jobs, returned artifacts and explicit result review.
- Exposes cached fleet status without a model polling continuously.

Codex uses its native compaction. Claude's optional process cycling is disabled in this distribution. The supplied configuration has one account per client and no automatic account switching. No model subscription or API access is included.

## Start with the CLI

Clone into a path without spaces on Windows:

```powershell
git clone https://github.com/pipopip/baton.git C:\Work\baton
cd C:\Work\baton
python bt.py --help
python bt.py install --claude --codex --profile --dry-run
```

Inspect the proposed hooks and profile change. To install those components, rerun the last command without `--dry-run`, then approve Codex hooks through the client's normal `/hooks` flow. Keep this checkout in place: installed hooks point to it. Sign in to the clients through their own normal flows; Baton does not need your credentials copied into its repository.

In a new PowerShell session, use an existing repository:

```powershell
bt wt new feature --repo C:\Work\my-project --exit pr --purpose "Describe the task"
bt my-project/feature -Vendor codex
# Or: bt my-project/feature -Vendor claude
```

Start with `bt` so ownership and the capsule are established. Update `.baton/now.md` with the objective, decisions, verification and next action. A worktree's declared exit governs retirement; a PR worktree remains until its work is merged. See `bt wt --help`.

## Optional compute queue

Read [compute setup and operation](mods/compute-queue/README.md). No remote destination is preconfigured. `bt fleet --details` distinguishes usable slots from foreground reserves; idle output is a cached observation, not a live probe. GPU jobs require a separately qualified backend.

Images, video and other artifacts are files, not merely console text. Consumers must open and inspect creative results and iterate; a completed job is not automatically an approved result. New artifact jobs default to 256 MiB per file and 1 GiB per job, with bounded streaming, SHA-256 verification and explicit missing-file notices. Existing legacy jobs retain their original limits.

Session-workflow notices, usage widgets and LANE-specific integrations are optional external integrations. Their private deployment assets, creative skill bundles and operational history are absent from this distribution.

## Development

```powershell
python -m unittest discover -s tests -p test_procs.py
python -m unittest discover -s tests -p test_compute_status.py
python -m unittest discover -s tests -p test_compute_pickup.py
python -m unittest discover -s mods/compute-queue/bin -p "test_*.py"
```

These tests use temporary state and mocks where appropriate; they do not certify every client version or remote deployment. Client hooks and the native Claude plugin API can change. Compatibility problems should include the client version and a minimal reproduction, with private session data removed.

[Design references and roadmap](ROADMAP.md) · [MIT license](LICENSE) · [Contributing](CONTRIBUTING.md) · [Security](SECURITY.md)
