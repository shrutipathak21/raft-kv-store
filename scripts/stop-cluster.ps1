# stop-cluster.ps1 - Stops all node processes recorded in pids.txt.
# Each line is "id:pid1,pid2,..." -- more than one pid per node happens when `java`
# on PATH resolves through a launcher/redirector (e.g. Oracle's javapath) that stays
# alive as a parent alongside the real JVM child; see start-cluster.ps1 for details.
$ErrorActionPreference = "SilentlyContinue"
Set-Location -Path (Join-Path $PSScriptRoot "..")

if (-not (Test-Path "pids.txt")) {
    Write-Host "No pids.txt found, nothing to stop."
    exit 0
}

Get-Content "pids.txt" | ForEach-Object {
    $parts = $_.Split(":")
    $id = $parts[0]
    $nodePids = $parts[1].Split(",") | ForEach-Object { [int]$_ }
    foreach ($procId in $nodePids) {
        if (Get-Process -Id $procId -ErrorAction SilentlyContinue) {
            Stop-Process -Id $procId -Force
            Write-Host ("Killed node {0} (pid {1})" -f $id, $procId)
        }
    }
}

Remove-Item "pids.txt"
