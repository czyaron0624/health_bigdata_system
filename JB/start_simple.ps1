# Simplified startup script - handles Docker and app startup

# Set UTF-8 encoding for stable console output
[Console]::InputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$OutputEncoding = [System.Text.UTF8Encoding]::new($false)

# Force PowerShell to use UTF-8 when supported
if ($PSVersionTable.PSVersion.Major -ge 6) {
    $PSDefaultParameterValues['*:Encoding'] = 'UTF8'
}

$ErrorActionPreference = 'Continue'

Write-Host "================================" -ForegroundColor Cyan
Write-Host "Health Big Data System Launcher" -ForegroundColor Cyan
Write-Host "================================" -ForegroundColor Cyan
Write-Host ""

$scriptRoot = $PSScriptRoot
if (-not $scriptRoot) {
    $scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
}

$projectRoot = Split-Path -Parent $scriptRoot
if (-not (Test-Path (Join-Path $projectRoot 'run.py'))) {
    $projectRoot = $scriptRoot
}

if (-not (Test-Path (Join-Path $projectRoot 'run.py'))) {
    Write-Host "ERROR: cannot locate the project root. Make sure this script is in the JB folder." -ForegroundColor Red
    Read-Host "Press Enter to exit"
    exit 1
}

Set-Location $projectRoot

$env:OCR_BACKEND = 'rapidocr'

# Check virtual environment
Write-Host "[1/3] Checking virtual environment..." -ForegroundColor Yellow
if (-not (Test-Path "venv\Scripts\python.exe")) {
    Write-Host "ERROR: Python virtual environment not found" -ForegroundColor Red
    Write-Host "Run: py -3.12 -m venv venv" -ForegroundColor Yellow
    Write-Host "Then run: .\venv\Scripts\python.exe -m pip install -r requirements.txt"
    Read-Host "Press Enter to exit"
    exit 1
}
if (-not (Test-Path "venv\pyvenv.cfg")) {
    Write-Host "ERROR: Python virtual environment is broken; pyvenv.cfg is missing" -ForegroundColor Red
    Write-Host "Delete venv and recreate it with: py -3.12 -m venv venv" -ForegroundColor Yellow
    Write-Host "Then run: .\venv\Scripts\python.exe -m pip install -r requirements.txt"
    Read-Host "Press Enter to exit"
    exit 1
}
Write-Host "OK: virtual environment looks healthy" -ForegroundColor Green

# Check Docker (optional)
Write-Host "[2/3] Checking Docker and database..." -ForegroundColor Yellow
$dockerAvailable = $null -ne (Get-Command docker -ErrorAction SilentlyContinue)

if ($dockerAvailable) {
    Write-Host "  Docker is available. Trying to start containers..." -ForegroundColor Cyan
    
    # Try to start containers, but continue on failure
    try {
        @('health_mysql', 'health_redis') | ForEach-Object {
            $exists = docker ps -a --format "{{.Names}}" | Select-String -Pattern "^$_`$"
            if ($exists) {
                docker start $_ 2>&1 | Out-Null
                Write-Host "  Started container: $_"
            }
        }
    }
    catch {
        Write-Host "  WARNING: Docker startup failed; continuing with the Flask app" -ForegroundColor Yellow
    }
}
else {
    Write-Host "  Docker not found. Make sure MySQL and Redis are running locally." -ForegroundColor Cyan
    Write-Host "     MySQL: localhost:3306" -ForegroundColor Gray
    Write-Host "     Redis: localhost:6379" -ForegroundColor Gray
}

# Start app
Write-Host "[3/3] Starting Flask app..." -ForegroundColor Yellow
Write-Host ""

$pythonExe = Join-Path $projectRoot 'venv\Scripts\python.exe'
$appFile = Join-Path $projectRoot 'run.py'

if (-not (Test-Path $pythonExe)) {
    Write-Host "ERROR: cannot find virtual environment Python: $pythonExe" -ForegroundColor Red
    Read-Host "Press Enter to exit"
    exit 1
}

if (-not (Test-Path $appFile)) {
    Write-Host "ERROR: cannot find app entry point: $appFile" -ForegroundColor Red
    Read-Host "Press Enter to exit"
    exit 1
}

Write-Host "=================================================" -ForegroundColor Green
Write-Host "OK: Flask app is starting..." -ForegroundColor Green
Write-Host "=================================================" -ForegroundColor Green
Write-Host ""
Write-Host "Access URLs:" -ForegroundColor Cyan
Write-Host "  - Login page: http://127.0.0.1:5000/login" -ForegroundColor White
Write-Host "  - Admin account:" -ForegroundColor White
Write-Host "    Username: admin" -ForegroundColor Gray
Write-Host "    Password: admin123" -ForegroundColor Gray
Write-Host ""
Write-Host "Press Ctrl+C to stop the service" -ForegroundColor Yellow
Write-Host ""

& $pythonExe $appFile
