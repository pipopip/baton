# Support matrix

| Component | Status |
| --- | --- |
| Python 3.12+ and Git | Required for the CLI |
| Windows | Primary development environment; individual CLI integrations and terminal behavior still need local verification |
| Linux | Queue unit checks and Linux workers; the complete desktop coordinator is not supported |
| Claude Code and Codex | Optional integrations; hook and plugin interfaces depend on the installed CLI version |
| Remote compute | Optional; a user-configured host and separate setup are required |
| session-workflow, usage monitoring and LANE | External optional integrations; not included in this snapshot |

CI runs the queue unit suite and selected core process/status/pickup checks on
Windows and Linux. It does not install a coding client or a remote host. The
maintainer's native checks used Claude Code 2.1.322 and Codex CLI 0.167.0;
compatibility with other versions requires checking their hook/plugin interfaces.

Remote fleet identities, foreground reserves and overnight windows are configurable.
New artifact jobs default to 256 MiB per file and 1 GiB per job; split artifact jobs
are unsupported. New local artifact jobs require the contained fleet backend;
legacy laptop jobs still use an external helper. Fresh Windows fleet jobs use the bundled
pool. Core tests for Windows junctions, terminals and account integration are
not an assurance of equivalent Linux desktop behavior.

Stopping ordinary Linux host processes requires pidfd support and matching
inherited job metadata. Unverifiable processes are not signalled. Cancellation
and timeout remain pending until shutdown is confirmed; a stop request does not
release reservations or authorize replay.
