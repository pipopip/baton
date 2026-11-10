# Design references and next improvements

Baton combines session continuity with resource-limited jobs. It does not need
an LLM to make each scheduling decision. These established projects are useful
references, rather than dependencies installed by this distribution:

- [HTCondor](https://htcondor.readthedocs.io/en/latest/admin-manual/ep-policy-configuration.html): dynamic allocations from separate CPU, RAM and disk pools. Baton has independent CPU/RAM requests; admitted disk budgets and per-host reserve policy would improve it.
- [HTCondor fair share](https://htcondor.readthedocs.io/en/latest/users-manual/job-scheduling.html): user fairness is separate from job priority. Baton currently prioritizes jobs and protects per-device reservations; historical fairness between workstreams remains future work.
- [Ray resources](https://docs.ray.io/en/latest/ray-core/scheduling/resources.html): logical requests do not themselves enforce physical limits. Baton retains cgroups/Job Objects, and should qualify recipe-specific thread controls rather than silently applying them to every command.
- [Agent Deck](https://github.com/asheshgoplani/agent-deck): clear multi-client session states and lifecycle operations. Baton's client compatibility tests and status explanations should grow alongside its supported versions.
- [Worktrunk](https://worktrunk.dev/): simple worktree lifecycle and shared build caches on supported filesystems. Cache sharing could reduce disk and repeated builds, with separate Windows qualification and ownership-safe cleanup.

Priority follow-ups are configurable host identities/reserves/time windows,
larger artifact transport with integrity and storage quotas, and a broader
client-version test matrix. These are not implemented promises in the initial
snapshot. Resource-policy changes should be measured on the target hosts and
reconciled with existing jobs before increasing concurrency.
