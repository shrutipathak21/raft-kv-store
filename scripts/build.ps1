# build.ps1 - Compiles the project. Run from anywhere; it locates the project root itself.
$ErrorActionPreference = "Stop"
Set-Location -Path (Join-Path $PSScriptRoot "..")

if (Test-Path "out") {
    Remove-Item -Recurse -Force "out"
}
New-Item -ItemType Directory -Path "out" | Out-Null

$sourceFiles = Get-ChildItem -Recurse -Filter "*.java" -Path "src" | ForEach-Object { '"' + ($_.FullName -replace '\\','/') + '"' }
$sourceFiles | Out-File -FilePath "sources.txt" -Encoding ascii

javac -d out -encoding UTF-8 "@sources.txt"
if ($LASTEXITCODE -ne 0) {
    Write-Error "Build failed."
    exit 1
}
Write-Host "Build OK -> out/"
