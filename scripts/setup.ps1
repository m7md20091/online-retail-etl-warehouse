$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

if (-not (Test-Path ".venv\Scripts\python.exe")) {
    py -3.13 -m venv .venv
}

& ".\.venv\Scripts\python.exe" -m pip install -r requirements.txt
Write-Host "Setup complete. Use .\.venv\Scripts\python.exe to run the project."

