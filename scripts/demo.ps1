# demo.ps1 - Full failure-injection demo, native PowerShell (no WSL/bash required).
# Starts a 5-node cluster, performs writes, kills the leader mid-run, confirms
# automatic re-election + continued writes, then rejoins the crashed node and
# confirms it catches up.
$ErrorActionPreference = "Stop"
Set-Location -Path (Join-Path $PSScriptRoot "..")

$conf = "config/cluster.conf"
New-Item -ItemType Directory -Force -Path "logs" | Out-Null
Get-ChildItem "logs" -Filter "*.log" -ErrorAction SilentlyContinue | Remove-Item -Force

$procIds = @{}   # nodeId (int) -> process id (int)

$script:startGeneration = @{}

function Start-Node {
    param([int]$id)
    Write-Host (">> starting node {0}" -f $id)
    if (-not $script:startGeneration.ContainsKey($id)) { $script:startGeneration[$id] = 0 }
    $script:startGeneration[$id]++
    $gen = $script:startGeneration[$id]
    # Suffix with a generation number rather than truncating on restart -- Start-Process's
    # redirection doesn't support append mode, and losing a killed node's pre-crash log
    # history makes exactly the kind of election/livelock debugging we needed it for impossible.
    $logFile = "logs/node{0}-gen{1}.log" -f $id, $gen
    $proc = Start-Process -FilePath "java" `
        -ArgumentList @("-cp", "out", "com.raftkv.Server", $id, $conf) `
        -RedirectStandardOutput $logFile `
        -RedirectStandardError ("logs/node{0}-gen{1}.err.log" -f $id, $gen) `
        -NoNewWindow -PassThru

    # `java` on PATH can resolve through a launcher/redirector rather than the real
    # binary directly -- e.g. Oracle's "javapath" redirector, or shims from Chocolatey/
    # Scoop/winget/JDK version managers. Unlike a typical shim that execs-and-exits,
    # some of these (Oracle's javapath in particular) stay alive as a PARENT process
    # while spawning the real JVM as a CHILD -- meaning a single `java` launch can
    # produce TWO live processes with matching command lines, and there's no reliable
    # way to know in advance which one Start-Process's own .Id refers to. Rather than
    # guessing which single process is "the real one", find and track EVERY java.exe
    # process whose command line matches this node, and kill all of them together --
    # that way it doesn't matter which is the wrapper and which is the worker.
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
            break # CIM/WMI not available on this system -- fall back below rather than looping uselessly
        }
        if ($matches) { $foundPids = @($matches | ForEach-Object { $_.ProcessId }); break }
    }

    # Report exactly what happened rather than silently falling back -- this is the
    # difference between diagnosing a PID mismatch in seconds versus guessing blind.
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

    if ($foundPids.Count -gt 0) {
        $procIds[$id] = $foundPids
    } else {
        $procIds[$id] = @($proc.Id)
    }
}

# Kills every tracked process for a node (there may be more than one -- see Start-Node).
function Stop-NodeProcesses {
    param([int]$id)
    foreach ($p in $procIds[$id]) {
        Stop-Process -Id $p -Force -ErrorAction SilentlyContinue
    }
}


function Invoke-Client {
    param([int]$preferredNode, [string[]]$cmdParts)
    $cliArgs = @($conf, $preferredNode) + $cmdParts
    return (& java -cp out com.raftkv.client.Client @cliArgs)
}

function Get-StatusOf {
    param([int]$id)
    return (Invoke-Client -preferredNode $id -cmdParts @("STATUS"))
}

function Find-Leader {
    # Reuses the exact same server-side redirect-following that every PUT/GET
    # already relies on successfully, instead of guessing which node to poll
    # with STATUS. One JVM launch per call (not up to five), and it won't
    # report a leader that isn't yet ready to serve (see RaftNode#isReadyToServeReads).
    $result = Invoke-Client -preferredNode 0 -cmdParts @("WHOISLEADER")
    if ($result -match "^LEADER (\d+)") {
        return [int]$matches[1]
    }
    return 0
}

# Cross-checks a WHOISLEADER answer with a direct STATUS call to that specific
# node before trusting it. WHOISLEADER can occasionally report a stale/dead
# node during the brief window right after a forced kill (a race in the OS
# tearing down the killed process's listening socket, not a Raft bug); this
# catches that instead of silently reporting a wrong node as "elected".
function Confirm-Leader {
    param([int]$candidateId, [int]$excludeId)
    if ($candidateId -eq 0 -or $candidateId -eq $excludeId) { return $false }
    $status = Get-StatusOf -id $candidateId
    return ($status -match "state=LEADER")
}

Write-Host "=============================================="
Write-Host "PHASE 1: Starting 5-node cluster"
Write-Host "=============================================="
1..5 | ForEach-Object { Start-Node -id $_ }
Start-Sleep -Seconds 6

Write-Host ""
Write-Host "=============================================="
Write-Host "PHASE 2: Waiting for leader election"
Write-Host "=============================================="
$leader = 0
$confirmed = $false
for ($i = 0; $i -lt 40; $i++) {
    $candidate = Find-Leader
    if (Confirm-Leader -candidateId $candidate -excludeId 0) {
        $leader = $candidate
        $confirmed = $true
        break
    }
    Start-Sleep -Milliseconds 1000
}
if ($confirmed) {
    Write-Host ("Leader elected: node-{0}" -f $leader)
} else {
    Write-Host "WARNING: could not confirm a leader within the retry window (cluster may still be settling)."
}
foreach ($id in 1..5) {
    Write-Host ("  node-{0}: {1}" -f $id, (Get-StatusOf -id $id))
}

Write-Host ""
Write-Host "=============================================="
Write-Host "PHASE 3: Writing keys through the cluster"
Write-Host "=============================================="
Write-Host ("PUT alpha=1  -> {0}" -f (Invoke-Client -preferredNode 0 -cmdParts @("PUT","alpha","1")))
Write-Host ("PUT beta=2   -> {0}" -f (Invoke-Client -preferredNode 0 -cmdParts @("PUT","beta","2")))
Write-Host ("PUT gamma=3  -> {0}" -f (Invoke-Client -preferredNode 0 -cmdParts @("PUT","gamma","3")))
Write-Host ("GET alpha    -> {0}" -f (Invoke-Client -preferredNode 0 -cmdParts @("GET","alpha")))
Write-Host ("GET beta     -> {0}" -f (Invoke-Client -preferredNode 0 -cmdParts @("GET","beta")))

Write-Host ""
Write-Host "=============================================="
Write-Host ("PHASE 4: Killing the leader (node-{0}) mid-run" -f $leader)
Write-Host "=============================================="
$killedPids = $procIds[$leader]
Stop-NodeProcesses -id $leader
Write-Host ("Killed node-{0} (pid(s): {1})" -f $leader, ($killedPids -join ", "))
$oldLeader = $leader
$procIds.Remove($leader)
Start-Sleep -Seconds 2

# Verify the kill actually took effect. A process that's genuinely dead should not
# answer STATUS at all; if it still does, Stop-Process silently failed to kill the
# real process (see the PID-resolution comment in Start-Node above for why that can
# happen) -- better to say so loudly here than let it show up as a confusing symptom
# several phases later.
$stillAlive = Get-StatusOf -id $leader
if ($stillAlive -match "state=LEADER") {
    Write-Host ("WARNING: node-{0} still responds as LEADER after being 'killed' (pid(s) {1})." -f $leader, ($killedPids -join ", "))
    Write-Host "  This means Stop-Process did not actually terminate every real java.exe process --"
    Write-Host "  likely because 'java' on PATH resolves to a launcher/shim rather than the real"
    Write-Host "  binary directly. Everything below will look wrong as a result. Check manually with:"
    Write-Host "    Get-Process java | Select-Object Id,Path,StartTime"
    Write-Host "  and kill the correct PID yourself, or close this terminal to force-stop everything."
}
Start-Sleep -Seconds 4

Write-Host ""
Write-Host "=============================================="
Write-Host "PHASE 5: Confirming automatic re-election"
Write-Host "=============================================="
$newLeader = 0
$confirmed = $false
for ($i = 0; $i -lt 40; $i++) {
    $candidate = Find-Leader
    if (Confirm-Leader -candidateId $candidate -excludeId $oldLeader) {
        $newLeader = $candidate
        $confirmed = $true
        break
    }
    Start-Sleep -Milliseconds 1000
}
if ($confirmed) {
    Write-Host ("New leader elected: node-{0}" -f $newLeader)
} else {
    Write-Host "WARNING: could not confirm a new leader distinct from the old one within the retry window."
}
foreach ($id in 1..5) {
    if ($id -ne $oldLeader) {
        Write-Host ("  node-{0}: {1}" -f $id, (Get-StatusOf -id $id))
    }
}

Write-Host ""
Write-Host "=============================================="
Write-Host "PHASE 6: Confirming writes continue after failover"
Write-Host "=============================================="
Write-Host ("PUT delta=4   -> {0}" -f (Invoke-Client -preferredNode 0 -cmdParts @("PUT","delta","4")))
Write-Host ("PUT epsilon=5 -> {0}" -f (Invoke-Client -preferredNode 0 -cmdParts @("PUT","epsilon","5")))
Write-Host ("GET alpha (pre-failure key)  -> {0}" -f (Invoke-Client -preferredNode 0 -cmdParts @("GET","alpha")))
Write-Host ("GET delta (post-failure key) -> {0}" -f (Invoke-Client -preferredNode 0 -cmdParts @("GET","delta")))

Write-Host ""
Write-Host "=============================================="
Write-Host "PHASE 7: Rejoining the crashed node and verifying catch-up"
Write-Host "=============================================="
Start-Node -id $oldLeader
Start-Sleep -Seconds 6
Write-Host ("Rejoined node-{0} status: {1}" -f $oldLeader, (Get-StatusOf -id $oldLeader))
Write-Host ("Current leader status:    {0}" -f (Get-StatusOf -id $newLeader))
Write-Host ("GET delta from rejoined node's cluster view -> {0}" -f (Invoke-Client -preferredNode 0 -cmdParts @("GET","delta")))

Write-Host ""
Write-Host "=============================================="
Write-Host "PHASE 8: Shutting down cluster"
Write-Host "=============================================="
foreach ($id in $procIds.Keys) {
    Stop-NodeProcesses -id $id
}
Write-Host "Demo complete."
