$ErrorActionPreference = 'Stop'

Set-Location -Path $PSScriptRoot

Write-Host "[1/2] Stopping web_app on port 5000..." -ForegroundColor Cyan
$listeners = Get-NetTCPConnection -LocalPort 5000 -State Listen -ErrorAction SilentlyContinue
if ($listeners) {
    $procIds = $listeners | Select-Object -ExpandProperty OwningProcess -Unique
    foreach ($procId in $procIds) {
        try {
            $proc = Get-Process -Id $procId -ErrorAction Stop
            Stop-Process -Id $procId -Force
            Write-Host "  - Stopped process $($proc.ProcessName) (PID: $procId)" -ForegroundColor Green
        }
        catch {
            Write-Host "  - Failed to stop PID ${procId}: $($_.Exception.Message)" -ForegroundColor Yellow
        }
    }
}
else {
    Write-Host "  - No listening process found on port 5000" -ForegroundColor Yellow
}

Write-Host "[2/2] Stopping Docker containers..." -ForegroundColor Cyan
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Host "  - Docker command not found, skip container stop" -ForegroundColor Yellow
    exit 0
}

$containerNames = @('health_mysql', 'health_redis')
$allNames = @(docker ps -a --format "{{.Names}}")

foreach ($name in $containerNames) {
    if ($allNames -contains $name) {
        $running = @(docker ps --format "{{.Names}}")
        if ($running -contains $name) {
            docker stop $name | Out-Null
            Write-Host "  - Stopped container: $name" -ForegroundColor Green
        }
        else {
            Write-Host "  - Container already stopped: $name" -ForegroundColor Yellow
        }
    }
    else {
        Write-Host "  - Container not found: $name" -ForegroundColor Yellow
    }
}

Write-Host "Done." -ForegroundColor Green
