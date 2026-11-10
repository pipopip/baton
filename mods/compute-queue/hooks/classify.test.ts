import { describe, expect, test } from 'claude-code/testing'

import { classifyBash, RUN_HINT } from './classify'

const REMOTERUN = 'C:/Work/app/.claude/worktrees/lane_plan/.baton/remote/remoterun.py'
const HEAVYRUN = 'C:/Work/app/.claude/worktrees/lane_plan/.baton/heavyrun.py'
const SSH = 'C:/Windows/System32/OpenSSH/ssh.exe -o BatchMode=yes builder@198.51.100.16'

const denies = (cmd: string) => {
  const v = classifyBash(cmd)
  expect(v.deny).toBe(true)
  if (v.deny) expect(v.reason).toContain(RUN_HINT)
}
const allows = (cmd: string) => expect(classifyBash(cmd)).toEqual({ deny: false })

describe('S12 the Bash guard', () => {
  test('remoterun.py is denied with the run() example', () => {
    denies(`python ${REMOTERUN} -- go test ./...`)
    denies(`python3 ${REMOTERUN} --rev abc123 -- bash -c 'echo hi'`)
    denies(`LANE_HOST=198.51.100.16 C:/Work/app/.venv/Scripts/python.exe ${REMOTERUN} --subdir go -- go vet ./...`)
    denies(`cd /d/Code && python ${REMOTERUN} -- ls`)
    denies(`pyfrozen ${REMOTERUN} -- ls`)
    const v = classifyBash(`python ${REMOTERUN} -- go test ./...`)
    expect(v.deny && v.reason).toContain("run({command: '...', slots: 1, est_minutes: 10})")
  })

  test('heavyrun.py is denied', () => {
    denies(`python ${HEAVYRUN} -- go test ./...`)
    denies(`py ${HEAVYRUN} -- lake build`)
    denies(`bash -c "python ${HEAVYRUN} -- mutate"`)
  })

  test('ssh to the host that only reads is allowed', () => {
    allows(`${SSH} 'tail -5 ~/q/x.log'`)
    allows(`ssh builder@198.51.100.16 "cat ~/q/cq-1/run.log | tail -40"`)
    allows(`ssh builder@198.51.100.16 'grep -c PASS ~/q/integk-1.log; ls ~/q | head'`)
    allows(`ssh builder@198.51.100.16 'for i in 0 1 2 3; do flock -n ~/q/.heavy.$i true || echo held $i; done'`)
    allows(`ssh builder@198.51.100.16 'uptime; df -h ~; free -m; ps aux | grep -c go; stat ~/q/a; wc -l ~/q/x.log'`)
    allows(`ssh builder@198.51.100.16 'sha256sum ~/q/out/a.bin; test -f ~/q/exit && echo yes'`)
    allows(`ssh builder@198.51.100.16 'find ~/q -maxdepth 1 -name "cq-*" -mtime +3'`)
    allows(`ssh builder@198.51.100.16 'tail -f ~/q/heavy.log'`)
    allows(`scp builder@198.51.100.16:~/q/cq-1/run.log .`)
    allows(`ssh other@example.com 'go test ./...'`)
  })

  test('ssh to the new build host (198.51.100.16) that only reads is allowed', () => {
    allows(`ssh builder@198.51.100.16 "cat ~/q/cq-1/run.log | tail -40"`)
    allows(`ssh builder@198.51.100.16 'for i in 0 1 2 3; do flock -n ~/q/.heavy.$i true || echo held $i; done'`)
    allows(`scp builder@198.51.100.16:~/q/cq-1/run.log .`)
    allows(`ssh builder@ember 'tail -f ~/q/heavy.log'`)
  })

  test('ssh to the new build host (198.51.100.16, ember) that starts work is denied', () => {
    denies(`ssh builder@198.51.100.16 'go test ./...'`)
    denies(`ssh builder@198.51.100.16 'cd ~/work/x && setsid nohup bash run.sh > log 2>&1 &'`)
    denies(`ssh builder@198.51.100.16 bash -s < launcher.sh`)
    denies(`cat x.sh | ssh -o BatchMode=yes 198.51.100.16`)
    denies(`ssh builder@ember 'heavy -n 3 bash run.sh'`)
    denies(`cat x.sh | ssh -o BatchMode=yes ember`)
  })

  test('ssh to the ember IPv6 address (2001:db8:2:1::/64) that only reads is allowed', () => {
    allows(`ssh builder@2001:db8:2:1::2 'cat ~/q/cq-1/run.log | tail -40'`)
    allows(`ssh builder@[2001:db8:2:1::2] 'uptime; df -h ~/q'`)
  })

  test('ssh to the ember IPv6 address that starts work is denied, bracketed or not', () => {
    denies(`ssh builder@2001:db8:2:1::2 'go test ./...'`)
    denies(`ssh 2001:db8:2:1::2 'go test ./...'`)
    denies(`ssh builder@[2001:db8:2:1::2] 'heavy -n 3 bash run.sh'`)
    denies(`cat x.sh | ssh -o BatchMode=yes 2001:db8:1:1::9`)
  })

  test('an address in 2001:db8:0:1::/64 is not the build host (a non-builder user is not checked)', () => {
    allows(`ssh other@2001:db8:0:1::2 'go test ./...'`)
    allows(`ssh 2001:db8:0:1::2 'go test ./...'`)
  })

  test('ssh to the host that starts work is denied', () => {
    denies(`${SSH} 'nohup bash run.sh &'`)
    denies(`${SSH} 'cd ~/work/x && setsid nohup bash run.sh > log 2>&1 &'`)
    denies(`${SSH} 'cd ~/work/app/lane_k0/go && go test ./...'`)
    denies(`${SSH} 'cd x && go test -p 1 -count=1 ./... && echo done'`)
    denies(`${SSH} 'go vet ./a ./b'`)
    denies(`${SSH} 'go build ./...'`)
    denies(`${SSH} 'heavy -n 3 bash run.sh'`)
    denies(`${SSH} '~/bin/heavy go test ./...'`)
    denies(`${SSH} '~/bin/lanerun . -- go test'`)
    denies(`${SSH} 'python3.13 -u go/product/write/testdata/mutate.py --only x'`)
    denies(`${SSH} 'lake build'`)
    denies(`${SSH} 'cd ~/q/x && lake build Foo'`)
    denies(`${SSH} 'pyfrozen go/conformance/oracles/gen_recall.py --check'`)
    denies(`${SSH} 'python3 go/kernel/internal/genesis/testdata/oracle/gen_dates.py'`)
    denies(`${SSH} "bash -c 'cd x && go test ./...'"`)
    denies(`ssh builder@198.51.100.16 bash -s < launcher.sh`)
    denies(`cat launcher.sh | ssh builder@198.51.100.16 bash`)
    denies(`echo ${'abc'} | base64 -d | ssh builder@198.51.100.16 'base64 -d | bash'`)
    denies(`${SSH} 'echo aGk= | base64 -d | bash'`)
    denies(`${SSH} 'bash ~/q/run.sh'`)
    denies(`cat x.sh | ssh -o BatchMode=yes 198.51.100.16`)
    denies(`cat x.sh | ssh -o BatchMode=yes lane-ex44`) // the EX44's alias is a build host too
    denies(`cat x.sh | ssh -o BatchMode=yes builder@203.0.113.7`) // any builder@ address is, whatever the IP
    allows(`cat x.sh | ssh -o BatchMode=yes root@203.0.113.7`) // a bootstrap as root on a host not yet in the queue is not
  })

  test('single-package go checks and other local commands are allowed', () => {
    for (const host of ['hestia', 'hestia-ts', 'hestia-wifi', 'hearth', 'hearth-ts', 'k12', 'k12-wifi', 'k12-ts', 'worker@198.51.100.19', 'worker@198.51.100.18', 'worker@198.51.100.15']) {
      denies(`ssh ${host} 'go test ./...'`)
      allows(`ssh ${host} 'uptime; df -h ~'`)
    }
    allows(`${SSH} 'cd ~/work/x/go && go vet ./product/write'`)
    allows(`${SSH} 'cd ~/work/x/go && go test -run TestX -count=1 ./product/write'`)
    allows(`${SSH} 'go version; go env GOCACHE'`)
    allows('go vet ./...')
    allows('go build ./cmd/x')
    allows('C:/Developers/example/sdk/go1.27.2/go/bin/go.exe test -run TestFoo ./kernel/x')
    allows('gofmt -l go/')
    allows('git status && git diff --stat')
    allows('python tools/analyze.py --report out.json')
    allows('bash .baton/remote/integ-laptop-checks.sh')
    allows('cat .baton/remote/remoterun.py | head -20')
    allows('grep -n remoterun.py README.md')
    allows("git commit -m 'docs: remoterun.py is replaced by the queue; ssh builder@x go test ./...'")
    allows('echo "python remoterun.py -- go test ./..." > note.txt')
    allows('ls C:/Work/app/.claude/worktrees')
  })

  test('here-document text is not a command', () => {
    allows(`cat > notes.md <<'EOF'\nrun python ${REMOTERUN} -- go test ./...\nssh builder@198.51.100.16 'go test ./...'\nEOF\nls`)
    denies(`cat > notes.md <<'EOF'\nharmless\nEOF\npython ${REMOTERUN} -- ls`)
  })

  test('nested shells and wrappers are looked through', () => {
    denies(`bash -c 'python ${REMOTERUN} -- go test ./...'`)
    denies(`sh -lc "ssh builder@198.51.100.16 'nohup x &'"`)
    denies(`time python ${REMOTERUN} -- ls`)
    denies(`env FOO=1 python ${REMOTERUN} -- ls`)
    denies(`nohup python ${REMOTERUN} -- ls &`)
    denies(`timeout 600 ssh builder@198.51.100.16 'go test ./...'`)
    denies(`ls; (python ${REMOTERUN} -- ls)`)
    denies(`true && python ${REMOTERUN} -- ls || echo failed`)
    denies(`python -c "import subprocess; subprocess.run(['python', '${REMOTERUN}'])"`)
  })

  // cq-review-1 P2-1: every bypass it confirmed, each now denied
  test('review bypass: a here-document fed to a remote shell is classified as the remote script', () => {
    denies(`ssh builder@198.51.100.16 <<'EOF'\ncd ~/work/x && nohup go test ./... &\nEOF`)
    denies(`ssh builder@198.51.100.16 bash <<EOF\ngo test ./...\nEOF`)
    denies(`ssh builder@198.51.100.16 bash -s <<'EOF'\nsetsid heavy -n 3 bash run.sh\nEOF`)
    denies(`ssh builder@198.51.100.16 'bash' <<'EOF'\nlake build\nEOF`)
    denies(`ssh builder@198.51.100.16 bash <<< "go test ./..."`)
    // reads through a here-document stay allowed
    allows(`ssh builder@198.51.100.16 bash <<'EOF'\ntail -5 ~/q/x.log\nls ~/q | head\nEOF`)
    allows(`ssh builder@198.51.100.16 <<'EOF'\ndf -h ~\nuptime\nEOF`)
    allows(`ssh builder@198.51.100.16 'cat > ~/q/note.txt' <<'EOF'\nhello\nEOF`)
  })

  test('review bypass: a shell fed from a file, an empty remote command fed by <', () => {
    denies(`ssh builder@198.51.100.16 bash < launch.sh`)
    denies(`ssh builder@198.51.100.16 'bash < launch.sh'`)
    denies(`ssh builder@198.51.100.16 sh < ~/run.sh`)
    denies(`ssh builder@198.51.100.16 < launch.sh`)
    denies(`ssh builder@198.51.100.16 python3 - < gen.py`)
    allows(`ssh builder@198.51.100.16 'tail -5 ~/q/x.log' < /dev/null`)
  })

  test('review bypass: a remote command built by substitution (the program is dynamic), arguments may substitute', () => {
    denies(`ssh builder@198.51.100.16 "$(cat run.sh)"`)
    denies(`ssh builder@198.51.100.16 $(cat run.sh)`)
    denies('ssh builder@198.51.100.16 `cat run.sh`')
    denies(`ssh builder@198.51.100.16 'cd ~; $(cat run.sh)'`)
    denies(`ssh builder@198.51.100.16 '$PY go/x/gen.py'`)
    denies(`ssh builder@198.51.100.16 'bash -c "$(cat run.sh)"'`)
    // false positive of the first design: a read whose argument is substituted
    allows(`ssh builder@198.51.100.16 tail -n 20 $(ls -t ~/q/*/run.log | head -1)`)
    allows(`ssh builder@198.51.100.16 'tail -n 20 $(ls -t ~/q/*/run.log | head -1)'`)
    allows(`ssh builder@198.51.100.16 'for f in $(ls ~/q); do tail -1 ~/q/$f/run.log; done'`)
    allows(`ssh builder@198.51.100.16 'x=$(cat y); echo $x; $HOME/bin/hello'`)
    allows('echo $(date) `uname`')
  })

  test('review bypass: -- after the host', () => {
    denies(`ssh -o BatchMode=yes builder@198.51.100.16 -- go test ./...`)
    denies(`ssh builder@198.51.100.16 -- nohup bash run.sh`)
    allows(`ssh -o BatchMode=yes builder@198.51.100.16 -- tail -5 ~/q/x.log`)
  })

  test('review bypass: a $VAR program that is given a queue script', () => {
    denies(`PY=python; $PY .baton/remote/remoterun.py -- ls`)
    denies(`PY=python; $PY ${REMOTERUN} -- ls`)
    denies('"$PY" .baton/heavyrun.py -- ls')
    denies('${PYTHON} .baton/remote/remoterun.py')
    allows('$EDITOR notes.md')
    allows('$PY tools/analyze.py')
  })

  test('review bypass: tmux, screen, at, batch, systemd-run on the host', () => {
    denies(`${SSH} 'tmux new -d "go test ./..."'`)
    denies(`${SSH} 'screen -dm bash run.sh'`)
    denies(`${SSH} 'echo "bash run.sh" | at now'`)
    denies(`${SSH} 'batch < run.sh'`)
    denies(`${SSH} 'systemd-run --user bash run.sh'`)
    allows(`${SSH} 'echo tmux is not run here; cat ~/q/x'`)
  })

  test('review false positive: only an executed queue script is denied, `-m MOD` is the program', () => {
    allows(`python -m py_compile ${REMOTERUN}`)
    allows(`python -m py_compile ${REMOTERUN} ${HEAVYRUN}`)
    allows(`python3 -m pyflakes ${REMOTERUN}`)
    allows(`python -W ignore -m py_compile ${REMOTERUN}`)
    allows(`python tools/check.py ${REMOTERUN}`)
    denies(`python -u ${REMOTERUN} -- ls`)
    denies(`python -W ignore ${REMOTERUN} -- ls`)
    denies(`python -m runpy ${REMOTERUN}`)
    denies(`uv run ${REMOTERUN} -- ls`)
  })

  test('review false positive: | python -c on the host parses output, | python alone runs a script', () => {
    allows(`${SSH} 'cat ~/q/cq-1/status.json | python3 -c "import json,sys; print(json.load(sys.stdin)[\\"ok\\"])"'`)
    allows(`${SSH} 'ps aux | python3 -c "import sys; print(len(sys.stdin.read()))"'`)
    denies(`${SSH} 'cat run.py | python3'`)
    denies(`${SSH} 'cat run.py | python3 -'`)
    denies(`${SSH} 'cat run.sh | bash'`)
  })

  test('review false positive: lake is denied only for build, exe, test, update and run', () => {
    allows(`${SSH} 'lake env printenv LEAN_PATH'`)
    allows(`${SSH} 'lake --version'`)
    allows(`${SSH} 'cd ~/q/x && lake env which lean'`)
    for (const sub of ['build', 'exe', 'test', 'update', 'run']) denies(`${SSH} 'lake ${sub} Foo'`)
    denies(`${SSH} 'cd ~/q/x && lake build'`)
    denies(`${SSH} 'lake env bash run.sh'`)
  })

  test('here-document text is looked into only for a shell that runs it', () => {
    denies(`bash <<'EOF'\npython ${REMOTERUN} -- ls\nEOF`)
    denies(`bash <<< "ssh builder@198.51.100.16 'go test ./...'"`)
    allows(`bash <<'EOF'\nls -la\nEOF`)
    allows(`cat <<'EOF'\npython ${REMOTERUN}\nEOF`)
  })

  test('quoted <, > and <tag> are arguments, not redirections', () => {
    allows(`${SSH} 'grep "<tag>" ~/q/x.log | head'`)
    allows(`grep '<tag>' f | head`)
    allows(`echo "a > b" && git status`)
    denies(`grep '<tag>' f; python ${REMOTERUN}`)
  })

  test('PowerShell spellings of a queue script are denied; the same guard serves PowerShell and Monitor', () => {
    denies(`& python ${REMOTERUN} -- ls`)
    denies(`& 'C:\\Tools\\.venv\\Scripts\\python.exe' '${REMOTERUN}' -- ls`)
    denies(`Start-Process python -ArgumentList '${REMOTERUN}'`)
    denies(`Invoke-Expression "python ${REMOTERUN}"`)
    denies(`$PY = 'python'; & $PY ${REMOTERUN}`)
    denies(`ssh builder@198.51.100.16 'nohup bash run.sh &'`)
    allows(`Get-Content ${REMOTERUN} | Select-Object -First 5`)
    allows('Get-ChildItem C:/Work | Select-Object -First 5')
  })

  // cq-fix-check-1 and the Fable examination: each command with the verdict it must get
  const H = 'builder@198.51.100.16'
  const TABLE: [string, string, 'DENY' | 'ALLOW'][] = [
    // P2-B: a PowerShell assignment in front of the command
    ['assignment', `$r = ssh ${H} 'cd ~/repo && nohup go test ./... &'`, 'DENY'],
    ['$null assignment', `$null = ssh ${H} 'heavy -n 4 go test ./...'`, 'DENY'],
    ['typed assignment', `[string]$r = ssh ${H} 'go test ./...'`, 'DENY'],
    ['assignment without spaces', `$r=ssh ${H} 'go test ./...'`, 'DENY'],
    ['+= assignment', `$r += ssh ${H} 'go test ./...'`, 'DENY'],
    ['$env: assignment then remoterun', `$env:LANE_HOST = '198.51.100.16'; python .baton/remote/remoterun.py --rev HEAD -- go test ./...`, 'DENY'],
    ['value assignment then ssh', `$x = 'a'; ssh ${H} 'go test ./...'`, 'DENY'],
    ['call operator', `& ssh ${H} 'go test ./...'`, 'DENY'],
    ['assignment of a path then a read of remoterun.py', `$p = "C:/Work/app/.claude/worktrees/lane_plan/.baton/remote/remoterun.py"; Select-String -Path $p -Pattern lanerun`, 'ALLOW'],
    ['assignment of a number', '$n = 5; Get-ChildItem', 'ALLOW'],
    ['assignment of a variable', '$a = $b; Get-ChildItem', 'ALLOW'],
    // P2-B: PowerShell wrappers
    ['remote eval', `ssh ${H} 'eval "go test ./..."'`, 'DENY'],
    ['pwsh -Command', `pwsh -Command "ssh ${H} 'go test ./...'"`, 'DENY'],
    ['powershell -NoProfile -Command', `powershell -NoProfile -Command "ssh ${H} 'go test ./...'"`, 'DENY'],
    ['cmd /c', `cmd /c "ssh ${H} go test ./..."`, 'DENY'],
    ['Invoke-Expression', `Invoke-Expression "ssh ${H} 'go test ./...'"`, 'DENY'],
    ['Invoke-Command -ScriptBlock', `Invoke-Command -ScriptBlock { ssh ${H} 'go test ./...' }`, 'DENY'],
    ['Start-Job body', `Start-Job { ssh ${H} 'go test ./...' }`, 'DENY'],
    ['Start-ThreadJob body', `Start-ThreadJob -ScriptBlock { ssh ${H} 'go test ./...' }`, 'DENY'],
    ['sajb body', `sajb { ssh ${H} 'go test ./...' }`, 'DENY'],
    ['Start-Job with remoterun', 'Start-Job { python .baton/remote/remoterun.py -- ls }', 'DENY'],
    ['Invoke-Command of a read', 'Invoke-Command -ScriptBlock { Get-ChildItem }', 'ALLOW'],
    // heredocs and line continuation
    ['hyphen delimiter', `ssh ${H} <<'RUN-EOF'\ncd repo && go test ./...\nRUN-EOF`, 'DENY'],
    ['two heredocs on one line', `cat <<A; ssh ${H} <<B\nhello\nA\ngo test ./...\nB`, 'DENY'],
    ['two heredocs, text not a command', `cat <<A; cat <<B\nssh ${H} 'go test ./...'\nA\nsecond\nB`, 'ALLOW'],
    ['PowerShell backtick continuation', 'ssh ' + H + " `\n  'cat ~/q/cq-1/run.log'", 'ALLOW'],
    ['PowerShell backtick continuation, CRLF', 'ssh ' + H + " `\r\n  'cat ~/q/cq-1/run.log'", 'ALLOW'],
    ['PowerShell backtick continuation hiding a start', 'ssh ' + H + " `\n  'go test ./...'", 'DENY'],
    // remote reads and starts
    ['json.tool', `ssh ${H} 'cat x.json | python3 -m json.tool'`, 'ALLOW'],
    ['| python3 -', `ssh ${H} 'cat x.py | python3 -'`, 'DENY'],
    ['| python3', `ssh ${H} 'cat x.py | python3'`, 'DENY'],
    ['tmux ls', `ssh ${H} 'tmux ls'`, 'ALLOW'],
    ['tmux has-session', `ssh ${H} 'tmux has-session -t cq'`, 'ALLOW'],
    ['tmux capture-pane', `ssh ${H} 'tmux capture-pane -p -t cq'`, 'ALLOW'],
    ['tmux list-sessions', `ssh ${H} 'tmux list-sessions'`, 'ALLOW'],
    ['tmux display-message', `ssh ${H} 'tmux display-message -p "#S"'`, 'ALLOW'],
    ['tmux kill-session', `ssh ${H} 'tmux kill-session -t cq'`, 'ALLOW'],
    ['tmux list-clients', `ssh ${H} 'tmux list-clients'`, 'ALLOW'],
    ['tmux kill-server', `ssh ${H} 'tmux kill-server'`, 'DENY'],
    ['tmux new', `ssh ${H} 'tmux new -d "go test ./..."'`, 'DENY'],
    ['tmux send-keys', `ssh ${H} 'tmux send-keys -t cq "go test ./..." Enter'`, 'DENY'],
    ['screen -ls', `ssh ${H} 'screen -ls'`, 'ALLOW'],
    ['screen -dm', `ssh ${H} 'screen -dm bash run.sh'`, 'DENY'],
    ['remote source of a script', `ssh ${H} '. ./run-all.sh'`, 'DENY'],
    ['remote source keyword', `ssh ${H} 'source ~/q/launch.sh'`, 'DENY'],
    ['remote source of a profile', `ssh ${H} '. ~/.profile; df -h ~'`, 'ALLOW'],
    ['remote source of the cargo env', `ssh ${H} 'source ~/.cargo/env && uptime'`, 'ALLOW'],
    ['heavy alias', `ssh ${H} 'cd repo; ~/bin/heavy -n 2 go test ./pkg'`, 'DENY'],
    ['limit: a script on disk (README), only run.sh/launch*.sh/cq*.sh are named', `ssh ${H} 'bash ~/q/x.sh'`, 'ALLOW'],
    ['sh -c on the host', `ssh ${H} "sh -c 'go test ./...'"`, 'DENY'],
    ['timeout wrapper on the host', `ssh ${H} 'timeout 600 go test ./...'`, 'DENY'],
    ['pipe into ssh', `cat run.sh | ssh ${H}`, 'DENY'],
    ['ssh -T < file', `ssh -T ${H} < run.sh`, 'DENY'],
    // go run and make on the host
    ['go run on the host', `ssh ${H} 'cd repo && go run ./cmd/lanetest -n 1000'`, 'DENY'],
    ['go run via heavy on the host', `ssh ${H} 'heavy -n 4 go run ./cmd/lanetest'`, 'DENY'],
    ['go version on the host', `ssh ${H} 'go version'`, 'ALLOW'],
    ['make test on the host', `ssh ${H} 'cd repo && make test'`, 'DENY'],
    ['make with no target on the host', `ssh ${H} 'cd repo && make'`, 'DENY'],
    ['make -j4 on the host', `ssh ${H} 'make -j4'`, 'DENY'],
    ['make -C dir check on the host', `ssh ${H} 'make -C go check'`, 'DENY'],
    ['make build on the host', `ssh ${H} 'make build'`, 'DENY'],
    ['make all on the host', `ssh ${H} 'make all'`, 'DENY'],
    ['make test-race on the host', `ssh ${H} 'make test-race'`, 'DENY'],
    ['make clean on the host', `ssh ${H} 'make clean'`, 'ALLOW'],
    ['local make test', 'make test', 'ALLOW'],
    // the six reads that must stay allowed
    ['must stay: git', 'git status && git diff --stat', 'ALLOW'],
    ['must stay: scp', `scp ${H}:~/q/cq-1/run.log .`, 'ALLOW'],
    ['must stay: ssh tail', `ssh ${H} 'tail -n 20 ~/q/cq-1/run.log'`, 'ALLOW'],
    ['must stay: ssh grep', `ssh ${H} 'grep -c PASS ~/q/integk-1.log'`, 'ALLOW'],
    ['must stay: ssh ps | grep', `ssh ${H} 'ps aux | grep -c go'`, 'ALLOW'],
    ['must stay: single-package go test -race', `ssh ${H} 'cd ~/work/x/go && go test -race ./product/write'`, 'ALLOW'],
  ]
  test('the review tables: every command gets its verdict', () => {
    for (const [name, cmd, want] of TABLE) {
      const got = classifyBash(cmd).deny ? 'DENY' : 'ALLOW'
      expect(`${name}: ${got}`).toBe(`${name}: ${want}`)
    }
  })

  test('the classifier never throws on odd input', () => {
    for (const odd of ['', '   ', "'unterminated", '"also', '((((', '| | |', 'ssh', 'ssh -o', 'ssh builder@198.51.100.16', '\n\n', '$(', '`']) {
      expect(() => classifyBash(odd)).not.toThrow()
    }
  })
})
