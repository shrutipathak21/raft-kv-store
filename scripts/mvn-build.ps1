# mvn-build.ps1 - Builds the project using Maven instead of the plain-javac
# scripts\build.ps1. Requires Maven (`mvn`) installed and normal internet
# access (Maven Central), neither of which is available in the sandbox this
# project was built in -- this script has NOT been run/verified by Claude.
# If it fails, scripts\build.ps1 + scripts\demo.ps1 are the verified fallback.
$ErrorActionPreference = "Stop"
Set-Location -Path (Join-Path $PSScriptRoot "..")

mvn -q package
if ($LASTEXITCODE -ne 0) {
    Write-Error "Maven build failed."
    exit 1
}
Write-Host "Build OK -> target\raft-kv-store.jar"
Write-Host "Run a node with:   java -jar target\raft-kv-store.jar <nodeId> config\cluster.conf"
Write-Host "Run other tools with -cp, e.g.:"
Write-Host "  java -cp target\raft-kv-store.jar com.raftkv.client.Client config\cluster.conf 0 STATUS"
