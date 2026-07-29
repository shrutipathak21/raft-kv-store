# start-cluster.ps1 - Starts every node listed in config/cluster.conf as a background process.
$ErrorActionPreference = "Stop"
Set-Location -Path (Join-Path $PSScriptRoot "..")

New-Item -ItemType Directory -Force -Path "logs" | Out-Null
if (Test-Path "pids.txt") { Remove-Item "pids.txt" }

$conf = "config/cluster.conf"
$lines = Get-Content $conf | Where-Object { $_ -and -not $_.StartsWith("#") }

foreach ($line in $lines) {
    $parts = $line.Split(",")
    $id = $parts[0].Trim()

    Write-Host ("Starting node {0}..." -f $id)
    $proc = Start-Process -FilePath "java" `
        -ArgumentList @("-cp", "out", "com.raftkv.Server", $id, $conf) `
        -RedirectStandardOutput ("logs/node{0}.log" -f $id) `
        -RedirectStandardError ("logs/node{0}.err.log" -f $id) `
        -NoNewWindow -PassThru

    # `java` on PATH can resolve through a launcher/redirector rather than the real
    # binary directly -- e.g. Oracle's "javapath" redirector, or shims from Chocolatey/
    # Scoop/winget/JDK version managers. Unlike a typical shim that execs-and-exits,
    # some of these (Oracle's javapath in particular) stay alive as a PARENT process
    # while spawning the real JVM as a CHILD -- meaning a single `java` launch can
    # produce TWO live processes with matching command lines, and there's no reliable
    # way to know in advance which one Start-Process's own .Id refers to. Rather than
    # guessing which single process is "the real one", find and record EVERY java.exe
    # process whose command line matches this node, and stop-cluster.ps1 kills all of
    # them together -- that way it doesn't matter which is the wrapper and which is
    # the worker.
    $foundPids = @()
    $cimFailureReason = $null
    for ($i = 0; $i -lt 25; $i++) {
        Start-Sleep -Milliseconds 200
        try {
            $matches = Get-CimInstance Win32_Process -Filter "Name = 'java.exe'" -ErrorAction Stop |
                       Where-Object { $_.CommandLine -match "com\.raftkv\.Server\s+$id\s" }
        } catch {
            $cimFailureReason = $_.Exception.Message
            $matches = $null
            break
        }
        if ($matches) { $foundPids = @($matches | ForEach-Object { $_.ProcessId }); break }
    }
    $finalPids = if ($foundPids.Count -gt 0) { $foundPids } else { @($proc.Id) }

    if ($foundPids.Count -gt 1) {
        Write-Host ("   (found {0} java.exe processes for node {1}: {2} -- tracking all of them, likely a launcher/redirector like Oracle's javapath)" -f $foundPids.Count, $id, ($foundPids -join ", "))
    } elseif ($foundPids.Count -eq 1 -and $foundPids[0] -ne $proc.Id) {
        Write-Host ("   (resolved real java.exe pid {0} -- Start-Process itself reported a different pid {1}, confirming a launcher/shim is in play)" -f $foundPids[0], $proc.Id)
    } elseif ($foundPids.Count -eq 1) {
        Write-Host ("   (confirmed pid {0} via command line match)" -f $foundPids[0])
    } elseif ($cimFailureReason) {
        Write-Host ("   (WARNING: could not verify the real pid -- Get-CimInstance failed: {0})" -f $cimFailureReason)
        Write-Host ("   (falling back to Start-Process's reported pid {0}, which may be wrong if java is a shim)" -f $proc.Id)
    } else {
        Write-Host ("   (WARNING: could not find a java.exe process matching node {0} by command line after 5s)" -f $id)
        Write-Host ("   (falling back to Start-Process's reported pid {0}, which may be wrong if java is a shim)" -f $proc.Id)
    }

    ("{0}:{1}" -f $id, ($finalPids -join ",")) | Add-Content "pids.txt"
}

Write-Host "Cluster started. PIDs recorded in pids.txt"
Get-Content "pids.txt"
