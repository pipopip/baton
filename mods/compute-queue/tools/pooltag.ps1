# Read-only: top nonpaged pool tags (NtQuerySystemInformation class 22, works without admin). Usage: pwsh -NoProfile -File pooltag.ps1 [-Top 12] [-Interval 0]
param([int]$Top = 12, [int]$Interval = 0, [string]$Watch = 'File')
Add-Type -MemberDefinition @'
[DllImport("ntdll.dll")] public static extern int NtQuerySystemInformation(int cls, IntPtr buf, int len, out int ret);
'@ -Name N -Namespace W
function Get-Tags {
    $len = 1MB; $buf = [Runtime.InteropServices.Marshal]::AllocHGlobal($len)
    try {
        while ($true) {
            $ret = 0; $st = [W.N]::NtQuerySystemInformation(22, $buf, $len, [ref]$ret)
            if ($st -eq 0) { break }
            if ($st -ne -1073741820) { throw ("NTSTATUS 0x{0:X8}" -f $st) }   # STATUS_INFO_LENGTH_MISMATCH: grow
            [Runtime.InteropServices.Marshal]::FreeHGlobal($buf); $len = $ret + 64KB; $buf = [Runtime.InteropServices.Marshal]::AllocHGlobal($len)
        }
        $n = [Runtime.InteropServices.Marshal]::ReadInt32($buf, 0)
        for ($i = 0; $i -lt $n; $i++) {
            $o = 8 + 40 * $i
            $tag = -join (0..3 | ForEach-Object { [char][Runtime.InteropServices.Marshal]::ReadByte($buf, $o + $_) })
            $na = [int64][Runtime.InteropServices.Marshal]::ReadInt32($buf, $o + 24) -band 0xFFFFFFFFL   # unsigned 32-bit counters
            $nf = [int64][Runtime.InteropServices.Marshal]::ReadInt32($buf, $o + 28) -band 0xFFFFFFFFL
            $nu = [Runtime.InteropServices.Marshal]::ReadInt64($buf, $o + 32)
            [pscustomobject]@{ Tag = $tag; NpAllocs = $na; NpFrees = $nf; NpOutstanding = [int64]$na - [int64]$nf; NpMB = [math]::Round($nu / 1MB, 1) }
        }
    } finally { [Runtime.InteropServices.Marshal]::FreeHGlobal($buf) }
}
$a = @(Get-Tags)
$a | Sort-Object NpMB -Descending | Select-Object -First $Top | Format-Table -Auto | Out-String
if ($Interval -gt 0) {
    Start-Sleep -Seconds $Interval
    $b = @(Get-Tags)
    $x = $a | Where-Object Tag -eq $Watch; $y = $b | Where-Object Tag -eq $Watch
    "tag {0}: {1} MB -> {2} MB in {3}s; outstanding {4:N0} -> {5:N0} (delta {6:N0}); alloc rate {7:N0}/s, free rate {8:N0}/s" -f $Watch, $x.NpMB, $y.NpMB, $Interval, $x.NpOutstanding, $y.NpOutstanding, ($y.NpOutstanding - $x.NpOutstanding), ((([int64]$y.NpAllocs - [int64]$x.NpAllocs + 4GB) % 4GB) / $Interval), ((([int64]$y.NpFrees - [int64]$x.NpFrees + 4GB) % 4GB) / $Interval)
}
