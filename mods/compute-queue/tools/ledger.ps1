# Read-only RAM ledger for this laptop: where the physical memory goes at one instant. No admin needed.
# Run: pwsh -NoProfile -File ledger.ps1
$k = Add-Type -MemberDefinition '[DllImport("kernel32.dll")] public static extern bool GetPhysicallyInstalledSystemMemory(out ulong kb);' -Name K -Namespace W2 -PassThru
[ulong]$inst = 0; [void]$k::GetPhysicallyInstalledSystemMemory([ref]$inst)
$cn = '\Memory\Free & Zero Page List Bytes', '\Memory\Standby Cache Normal Priority Bytes', '\Memory\Standby Cache Reserve Bytes', '\Memory\Standby Cache Core Bytes', '\Memory\Modified Page List Bytes', '\Memory\Pool Nonpaged Bytes', '\Memory\Pool Paged Resident Bytes', '\Memory\System Code Resident Bytes', '\Memory\System Driver Resident Bytes'
$m = @{}; (Get-Counter $cn -ErrorAction SilentlyContinue).CounterSamples | ForEach-Object { $m[$_.Path.Split('\')[-1]] = $_.CookedValue / 1MB }
$pw = 0; $ws = 0
foreach ($s in (Get-Counter '\Process(*)\Working Set - Private', '\Process(*)\Working Set' -ErrorAction SilentlyContinue).CounterSamples) {
    if ($s.InstanceName -in '_total', 'idle') { continue }
    if ($s.Path -like '*working set - private') { $pw += $s.CookedValue } else { $ws += $s.CookedValue }
}
$vis = (Get-CimInstance Win32_OperatingSystem).TotalVisibleMemorySize / 1KB
$free = $m['free & zero page list bytes']; $stb = $m['standby cache normal priority bytes'] + $m['standby cache reserve bytes'] + $m['standby cache core bytes']; $mod = $m['modified page list bytes']
$inuse = $vis - $free - $stb - $mod
$kern = $m['pool nonpaged bytes'] + $m['pool paged resident bytes'] + $m['system code resident bytes'] + $m['system driver resident bytes']
$gpu = ((Get-Counter '\GPU Adapter Memory(*)\Shared Usage' -ErrorAction SilentlyContinue).CounterSamples | Measure-Object CookedValue -Sum).Sum / 1MB
$thr = (Get-Process | ForEach-Object { $_.Threads.Count } | Measure-Object -Sum).Sum
$row = { param($n, $v) '{0,-58} {1,9:N0} MB' -f $n, $v }
& $row 'Installed (SMBIOS)' ($inst / 1KB)
& $row '  reserved by firmware / iGPU' ($inst / 1KB - $vis)
& $row 'Visible to Windows' $vis
& $row '  free + zero' $free
& $row '  standby (reclaimable cache)' $stb
& $row '  modified' $mod
& $row '  IN USE' $inuse
'--- in use, by owner ---'
& $row 'process private working sets' ($pw / 1MB)
& $row 'kernel: nonpaged pool' $m['pool nonpaged bytes']
& $row 'kernel: paged pool (resident)' $m['pool paged resident bytes']
& $row 'kernel: code + drivers' ($m['system code resident bytes'] + $m['system driver resident bytes'])
& $row 'GPU shared memory (system RAM held for the GPU)' $gpu
& $row 'kernel thread stacks (threads x 24 KB, estimate)' ($thr * 24 / 1KB)
& $row 'residual: shared/mapped file pages, compression store, WSL VM, page tables' ($inuse - $pw / 1MB - $kern - $gpu - $thr * 24 / 1KB)
& $row '(memo) all working sets incl. shared, double counted' ($ws / 1MB)
