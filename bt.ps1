# Baton launcher for PowerShell. Dot-source it from the profile:
#     . C:\Work\baton\bt.ps1
#
#   bt [worktree] [-Vendor claude|codex] [-Account x] [-Fresh|-Resume] [-NewTab] [-DryRun] [-Force] [-Prompt text] [-Model id] [-Role name]
#   bt switch [worktree] [-Account x]     fresh session on the other account
#   bt <subcommand> ...                   passed through to bt.py (status, accounts, list, ...)
#   bt help [command]                     a guide to the commands, or one command's options
#   bt                                    outside a worktree: pick one with fzf and start an agent (bt <worktree>)
#
# Keep this file ASCII: Windows PowerShell 5.1 reads BOM-less files as ANSI.

$script:BatonHome = $PSScriptRoot
$script:BatonSubcommands = @('status', 'accounts', 'account', 'plan', 'routing', 'follow', 'merge', 'inbox', 'install', 'now', 'close', 'doctor', 'attach', 'keepalive', 'list', 'wt', 'backup', 'startup', 'cycle', 'compute', 'fleet', 'night', 'migrate', 'shell', 'help')

function Invoke-BatonPython {
    # Runs bt.py with UTF-8 console decoding. Output goes to the pipeline; exit code is in $LASTEXITCODE.
    $savedEncoding = [Console]::OutputEncoding
    try {
        [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
        & python (Join-Path $script:BatonHome 'bt.py') @args
    }
    finally {
        [Console]::OutputEncoding = $savedEncoding
    }
}

function Get-BatonLaunchOptions {
    param([object[]]$Tokens)
    $opt = @{ Query = $null; Vendor = $null; Account = $null; Fresh = $false; Resume = $false; NewTab = $false; DryRun = $false; Switch = $false; Force = $false; Prompt = $null; Model = $null; Role = $null }
    $n = if ($null -eq $Tokens) { 0 } else { @($Tokens).Count }
    for ($i = 0; $i -lt $n; $i++) {
        $t = [string]$Tokens[$i]
        switch -Regex ($t) {
            '^--?vendor$' {
                if (++$i -ge $n) { throw "$t needs a value" }
                $opt.Vendor = [string]$Tokens[$i]
            }
            '^--?account$' {
                if (++$i -ge $n) { throw "$t needs a value" }
                $opt.Account = [string]$Tokens[$i]
            }
            '^--?prompt$' {
                if (++$i -ge $n) { throw "$t needs a value" }
                $opt.Prompt = [string]$Tokens[$i]
            }
            '^--?role$' {
                if (++$i -ge $n) { throw "$t needs a value" }
                $opt.Role = [string]$Tokens[$i]
            }
            '^--?model$' {
                if (++$i -ge $n) { throw "$t needs a value" }
                $opt.Model = [string]$Tokens[$i]
            }
            '^--?fresh$' { $opt.Fresh = $true }
            '^--?resume$' { $opt.Resume = $true }
            '^--?new-?tab$' { $opt.NewTab = $true }
            '^--?dry-?run$' { $opt.DryRun = $true }
            '^--?force$' { $opt.Force = $true }
            default {
                if ($t.StartsWith('-')) { throw "unknown option $t" }
                if ($opt.Query) { throw "unexpected argument $t" }
                $opt.Query = $t
            }
        }
    }
    if ($opt.Fresh -and $opt.Resume) { throw '-Fresh and -Resume are mutually exclusive' }
    return $opt
}

function Format-BatonCommand {
    param([string[]]$Argv)
    ($Argv | ForEach-Object { if ($_ -match '[\s"]') { '"' + ($_ -replace '"', '\"') + '"' } else { $_ } }) -join ' '
}

function ConvertTo-BatonNativeArgs {
    # Windows PowerShell 5.1 and pwsh < 7.3 drop embedded double quotes when calling native programs, and
    # pwsh 7.3+ falls back to the same legacy quoting for .cmd/.bat files (-Batch).
    param([string[]]$Argv, [switch]$Batch)
    $ver = $PSVersionTable.PSVersion
    $mode = Get-Variable -Name PSNativeCommandArgumentPassing -ValueOnly -ErrorAction SilentlyContinue
    if ($Batch -or $ver.Major -lt 7 -or ($ver.Major -eq 7 -and $ver.Minor -lt 3) -or $mode -eq 'Legacy') {
        return @($Argv | ForEach-Object { $_ -replace '"', '\"' })
    }
    return @($Argv)
}

function Clear-BatonEnv {
    # Delete a process environment variable. [Environment]::SetEnvironmentVariable($n, $null) does not:
    # PowerShell passes $null to a [string] parameter as "", which leaves an EMPTY variable that the
    # child inherits (an empty CLAUDE_CONFIG_DIR sent Claude Code to the wrong credentials location).
    param([string]$Name)
    Remove-Item -LiteralPath ("Env:" + $Name) -ErrorAction SilentlyContinue
}

function Format-BatonLiteral {
    param([string]$Text)
    "'" + ($Text -replace "'", "''") + "'"
}

function Test-BatonTerminalRetired {
    # Only the matching invocation's confirmed cycle may retire this shell.
    param([string]$Root, [string]$Token)
    if ($Token -cnotmatch '^[0-9a-f]{32}$') { return $false }
    $path = Join-Path (Join-Path (Join-Path $Root '.baton') 'terminal-retire') ($Token + '.json')
    $deadline = [DateTime]::UtcNow.AddSeconds(6)
    while ($true) {
        try {
            if (-not (Test-Path -LiteralPath $path -PathType Leaf)) { return $false }
            $marker = Get-Content -LiteralPath $path -Raw -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
            if ($marker.token -cne $Token) { return $false }
            if ($marker.phase -ceq 'retired') {
                Remove-Item -LiteralPath $path -ErrorAction SilentlyContinue
                return $true
            }
            if ($marker.phase -cne 'waiting' -or [DateTime]::UtcNow -ge $deadline) { return $false }
        } catch { return $false }
        Start-Sleep -Milliseconds 100
    }
}

function Open-BatonTab {
    param($Plan, [hashtable]$Opt, [string]$Title)
    $inner = '. ' + (Format-BatonLiteral (Join-Path $script:BatonHome 'bt.ps1')) + '; bt'
    if ($Opt.Switch) { $inner += ' switch' }
    # The resolved worktree, not the raw query: a relative or ambiguous query could resolve differently in the new tab.
    $inner += ' ' + (Format-BatonLiteral ([string]$Plan.cwd))
    if ($Opt.Vendor) { $inner += ' -Vendor ' + (Format-BatonLiteral $Opt.Vendor) }
    if ($Opt.Account) { $inner += ' -Account ' + (Format-BatonLiteral $Opt.Account) }
    if ($Opt.Fresh) { $inner += ' -Fresh' }
    if ($Opt.Resume) { $inner += ' -Resume' }
    if ($Opt.Force) { $inner += ' -Force' }
    if ($Opt.Prompt) { $inner += ' -Prompt ' + (Format-BatonLiteral $Opt.Prompt) }
    if ($Opt.Role) { $inner += ' -Role ' + (Format-BatonLiteral $Opt.Role) }
    if ($Opt.Model) { $inner += ' -Model ' + (Format-BatonLiteral $Opt.Model) }
    # This shell belongs solely to the new tab. Explicitly terminate it after bt returns,
    # including when the CLI exits nonzero; never do this in the caller's shell.
    $managed = '$global:LASTEXITCODE = 0; $ErrorActionPreference = ''Stop''; $PSNativeCommandUseErrorActionPreference = $false; try { ' + $inner + "`n" + '; exit $global:LASTEXITCODE } catch { Write-Error -ErrorAction Continue $_; exit 1 }'
    $encoded = [Convert]::ToBase64String([System.Text.Encoding]::Unicode.GetBytes($managed))
    $shell = if (Get-Command pwsh -ErrorAction SilentlyContinue) { 'pwsh' } else { 'powershell' }
    $wtArgs = "-w 0 nt -d `"$($Plan.cwd)`" --title `"$Title`" --suppressApplicationTitle $shell -EncodedCommand $encoded"
    if ($Opt.DryRun) {
        Write-Host "wt.exe $wtArgs"
        Write-Host "  (tab command: $inner)"
        return
    }
    # wt.exe hands its environment to the new tab: drop the calling agent session's variables.
    $sessionVars = @('CLAUDECODE','CLAUDE_CODE_CHILD_SESSION','CLAUDE_CODE_SESSION_ID','CLAUDE_CODE_SESSION_ATTENDED',
        'CLAUDE_CODE_MESSAGING_SOCKET','CLAUDE_CODE_MESSAGING_TOKEN','CLAUDE_CODE_ENTRYPOINT','CLAUDE_CODE_EXECPATH',
        'CLAUDE_PID','CLAUDE_EFFORT','CODEX_THREAD_ID','CODEX_SESSION_ID','CODEX_CI')
    if ((Get-Command Start-Process).Parameters.ContainsKey('Environment')) {
        $clear = @{}; foreach ($n in $sessionVars) { $clear[$n] = $null }
        Start-Process -FilePath 'wt.exe' -ArgumentList $wtArgs -Environment $clear
    } else {
        $saved = @{}
        try {
            foreach ($n in $sessionVars) { $saved[$n] = [Environment]::GetEnvironmentVariable($n, 'Process'); Clear-BatonEnv $n }
            Start-Process -FilePath 'wt.exe' -ArgumentList $wtArgs
        } finally {
            foreach ($n in $saved.Keys) { if ($null -ne $saved[$n]) { Set-Item -LiteralPath ("Env:" + $n) -Value $saved[$n] } }
        }
    }
}

function Invoke-BatonLaunch {
    param([hashtable]$Opt)
    $pyArgs = @('plan')
    if ($Opt.Query) { $pyArgs += $Opt.Query }
    if ($Opt.Vendor) { $pyArgs += @('--vendor', $Opt.Vendor) }
    if ($Opt.Account) { $pyArgs += @('--account', $Opt.Account) }
    if ($Opt.Fresh) { $pyArgs += '--fresh' }
    if ($Opt.Resume) { $pyArgs += '--resume' }
    if ($Opt.Switch) { $pyArgs += '--switch' }
    if ($Opt.Force) { $pyArgs += '--force' }
    if ($Opt.Prompt) { $pyArgs += @('--prompt', $Opt.Prompt) }
    if ($Opt.Role) { $pyArgs += @('--role', $Opt.Role) }
    if ($Opt.Model) { $pyArgs += @('--model', $Opt.Model) }
    # Dry runs and new tabs must not sync the second account here (the tab's own launch does).
    if ($Opt.DryRun -or $Opt.NewTab) { $pyArgs += '--no-sync' }
    $pyArgs += '--json'

    $raw = Invoke-BatonPython @pyArgs
    if ($LASTEXITCODE -ne 0) { return }   # bt.py already printed the reason on stderr
    $plan = ($raw -join "`n") | ConvertFrom-Json
    $title = Split-Path -Leaf $plan.cwd

    foreach ($line in ([string]$plan.reason -split "`n")) {
        if ($line) { Write-Host "[baton] $line" -ForegroundColor DarkGray }
    }

    if ($Opt.NewTab) {
        if (Get-Command wt.exe -ErrorAction SilentlyContinue) {
            Open-BatonTab -Plan $plan -Opt $Opt -Title $title
            return
        }
        Write-Host '[baton] wt.exe not found; running inline' -ForegroundColor Yellow
    }

    $argv = @($plan.argv)
    if ($Opt.DryRun) {
        Write-Host "cwd:   $($plan.cwd)"
        foreach ($p in $plan.env_set.PSObject.Properties) { Write-Host "set:   $($p.Name)=$($p.Value)" }
        foreach ($name in @($plan.env_unset)) { Write-Host "unset: $name" }
        Write-Host "run:   $(Format-BatonCommand $argv)"
        return
    }

    $saved = @{}
    $pushed = $false
    $token = [Guid]::NewGuid().ToString('N')
    $savedToken = [Environment]::GetEnvironmentVariable('BATON_TERMINAL_TOKEN', 'Process')
    $cliInvoked = $false
    try {
        foreach ($p in $plan.env_set.PSObject.Properties) {
            $saved[$p.Name] = [Environment]::GetEnvironmentVariable($p.Name, 'Process')
            [Environment]::SetEnvironmentVariable($p.Name, [string]$p.Value, 'Process')
        }
        foreach ($name in @($plan.env_unset)) {
            if (-not $saved.ContainsKey($name)) { $saved[$name] = [Environment]::GetEnvironmentVariable($name, 'Process') }
            Clear-BatonEnv $name
        }
        Push-Location -LiteralPath $plan.cwd
        $pushed = $true
        $tail = if ($argv.Count -gt 1) { @($argv[1..($argv.Count - 1)]) } else { @() }
        $rest = ConvertTo-BatonNativeArgs $tail -Batch:([string]$argv[0] -match '\.(cmd|bat)$')
        [Environment]::SetEnvironmentVariable('BATON_TERMINAL_TOKEN', $token, 'Process')
        $cliInvoked = $true
        & $argv[0] @rest
    }
    finally {
        $cliExitCode = $global:LASTEXITCODE
        if ($pushed) { Pop-Location }
        foreach ($key in @($saved.Keys)) {
            if ($null -eq $saved[$key]) { Clear-BatonEnv $key } else { Set-Item -LiteralPath ("Env:" + $key) -Value $saved[$key] }
        }
        if ($null -eq $savedToken) { Clear-BatonEnv 'BATON_TERMINAL_TOKEN' } else { Set-Item -LiteralPath 'Env:BATON_TERMINAL_TOKEN' -Value $savedToken }
        # A confirmed context retirement is successful even when TerminateProcess
        # gave the old CLI a nonzero code; default Terminal profiles close on success.
        if ($cliInvoked -and (Test-BatonTerminalRetired -Root ([string]$plan.cwd) -Token $token)) { exit 0 }
        if ($cliInvoked) { $global:LASTEXITCODE = $cliExitCode }
    }
}

function Test-BatonInWorktree {
    # True when the current directory is inside a git worktree (a .git file or directory here or in a parent).
    $location = Get-Location
    if ($location.Provider.Name -ne 'FileSystem') { return $false }
    $dir = $location.ProviderPath
    while ($dir) {
        if (Test-Path -LiteralPath (Join-Path $dir '.git')) { return $true }
        $parent = Split-Path -Parent $dir
        if (-not $parent -or $parent -eq $dir) { break }
        $dir = $parent
    }
    return $false
}

function Invoke-BatonPicker {
    # bt with no arguments outside a worktree: choose from `bt list` (most recent first) and start `bt <key>`.
    $rows = @(Invoke-BatonPython 'list' '--tsv')
    if ($LASTEXITCODE -ne 0) { return }   # bt.py already printed the reason on stderr
    $rows = @($rows | Where-Object { $_ })
    if (-not $rows.Count) { Write-Host '[baton] no worktrees found under the code root' -ForegroundColor Yellow; return }
    if (-not (Get-Command fzf -ErrorAction SilentlyContinue)) {
        Invoke-BatonPython 'list' | Out-Host
        return
    }
    $choice = $rows | & fzf --delimiter "`t" --with-nth 1,2,3 --prompt 'Start agent > ' --header 'Most recent first. Enter starts an agent with baton (bt <worktree>).'
    if (-not $choice) { return }
    $key = ([string]$choice -split "`t", 2)[0]
    bt $key
}

function bt {
    if ($args.Count -eq 0 -and -not (Test-BatonInWorktree)) {
        Invoke-BatonPicker
        return
    }
    $first = if ($args.Count -gt 0) { [string]$args[0] } else { '' }
    if ($first -and ($script:BatonSubcommands -contains $first.ToLowerInvariant() -or $first -in @('-h', '--help', '-?', '/?'))) {
        Invoke-BatonPython @args
        return
    }
    $isSwitch = $first -ieq 'switch'
    $tokens = @()
    if ($isSwitch) {
        if ($args.Count -gt 1) { $tokens = @($args[1..($args.Count - 1)]) }
    }
    else {
        $tokens = @($args)
    }
    try {
        $opt = Get-BatonLaunchOptions $tokens
    }
    catch {
        Write-Host "bt: $($_.Exception.Message)" -ForegroundColor Red
        return
    }
    $opt.Switch = $isSwitch
    Invoke-BatonLaunch $opt
}
