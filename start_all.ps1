$ErrorActionPreference = 'Stop'

Set-Location -Path $PSScriptRoot

Write-Host "[1/3] Checking Docker command..." -ForegroundColor Cyan
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Docker command not found. Please install and start Docker Desktop first."
}

Write-Host "[2/3] Starting MySQL / Redis containers..." -ForegroundColor Cyan
$containerNames = @('health_mysql', 'health_redis')
$allNames = @(docker ps -a --format "{{.Names}}")
$missing = @()

foreach ($name in $containerNames) {
    if ($allNames -contains $name) {
        docker start $name | Out-Null
        Write-Host "  - Container started: $name" -ForegroundColor Green
    }
    else {
        $missing += $name
    }
}

if ($missing.Count -gt 0) {
    Write-Host "  - Missing container(s), creating via compose: $($missing -join ', ')" -ForegroundColor Yellow
    docker compose up -d db redis | Out-Null
}

$running = @(docker ps --format "{{.Names}}")
foreach ($name in $containerNames) {
    if (-not ($running -contains $name)) {
        throw "Container is not running: $name"
    }
}

    Write-Host "[3/3] Starting Flask app..." -ForegroundColor Cyan
$pythonExe = Join-Path $PSScriptRoot "venv\Scripts\python.exe"
$appFile = Join-Path $PSScriptRoot "run.py"

if (-not (Test-Path $pythonExe)) {
    throw "Python virtual environment not found: $pythonExe"
}
if (-not (Test-Path $appFile)) {
    throw "App entry file not found: $appFile"
}

# If port 5000 is occupied, verify app health first.
$portInUse = Get-NetTCPConnection -LocalPort 5000 -State Listen -ErrorAction SilentlyContinue
if ($portInUse) {
    try {
        $resp = Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:5000/login" -TimeoutSec 5
        if ($resp.StatusCode -eq 200) {
            Write-Host "Port 5000 is in use and /login is healthy." -ForegroundColor Yellow
            Write-Host "URL: http://127.0.0.1:5000/login" -ForegroundColor Green
            exit 0
        }
    }
    catch {
        # Ignore and restart process on port 5000 below.
    }

    Write-Host "Port 5000 is in use but /login is unhealthy. Restarting app..." -ForegroundColor Yellow
    $procIds = $portInUse | Select-Object -ExpandProperty OwningProcess -Unique
    foreach ($procId in $procIds) {
        try {
            Stop-Process -Id $procId -Force
            Write-Host "  - Stopped PID: $procId" -ForegroundColor Green
        }
        catch {
            Write-Host "  - Failed to stop PID ${procId}: $($_.Exception.Message)" -ForegroundColor Yellow
        }
    }
}

$proc = Start-Process -FilePath $pythonExe -ArgumentList "`"$appFile`"" -WorkingDirectory $PSScriptRoot -PassThru
Write-Host "app started. PID: $($proc.Id)" -ForegroundColor Green
Write-Host "URL: http://127.0.0.1:5000/login" -ForegroundColor Green
