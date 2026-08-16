$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot
$ProjectPython = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $ProjectPython)) {
    throw "Virtual environment not found. Run scripts\setup.ps1 first."
}
& $ProjectPython run_pipeline.py run
