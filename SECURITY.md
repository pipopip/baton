# Security

Please use GitHub's private vulnerability reporting for security issues rather than posting sensitive details publicly. Do not include credentials or private session data in a report.

Jobs and hooks execute with your account's privileges. Worktree ownership,
resource limits and the command classifier prevent coordination mistakes; they
do not isolate malicious code from your files or network. Use trusted source,
least-privilege SSH accounts and verified host keys. Never expose the local
queue state as a public network service. Review hooks before installation and
approve them through the client's normal controls.
