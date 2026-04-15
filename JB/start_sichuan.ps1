param(
    [int]$MinYear = 2015,
    [switch]$DisableOCR,
    [string]$Sections = 'ylfw',
    [ValidateSet('title', 'publish')]
    [string]$YearFilterSource = 'title'
)

# Sichuan crawler launcher - runs only Sichuan collection on demand

[Console]::InputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$OutputEncoding = [System.Text.UTF8Encoding]::new($false)

if ($IsWindows -or $env:OS -eq 'Windows_NT') {
    # Keep external process I/O code page aligned with UTF-8.
    chcp.com 65001 | Out-Null
}

$env:PYTHONUTF8 = '1'
$env:PYTHONIOENCODING = 'utf-8'

if ($PSVersionTable.PSVersion.Major -ge 6) {
    $PSDefaultParameterValues['*:Encoding'] = 'UTF8'
}

$ErrorActionPreference = 'Stop'

Write-Host "================================" -ForegroundColor Cyan
Write-Host "Health Big Data System - Sichuan Crawler" -ForegroundColor Cyan
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

Write-Host "[1/2] Checking virtual environment..." -ForegroundColor Yellow
if (-not (Test-Path "venv\Scripts\python.exe")) {
    Write-Host "ERROR: Python virtual environment not found" -ForegroundColor Red
    Write-Host "Run: py -3.12 -m venv venv" -ForegroundColor Yellow
    Write-Host "Then run: .\venv\Scripts\python.exe -m pip install -r requirements.txt" -ForegroundColor Yellow
    Read-Host "Press Enter to exit"
    exit 1
}

if (-not (Test-Path "venv\pyvenv.cfg")) {
    Write-Host "ERROR: Python virtual environment is broken; pyvenv.cfg is missing" -ForegroundColor Red
    Write-Host "Delete venv and recreate it with: py -3.12 -m venv venv" -ForegroundColor Yellow
    Write-Host "Then run: .\venv\Scripts\python.exe -m pip install -r requirements.txt" -ForegroundColor Yellow
    Read-Host "Press Enter to exit"
    exit 1
}
Write-Host "OK: virtual environment looks healthy" -ForegroundColor Green

$pythonExe = Join-Path $projectRoot 'venv\Scripts\python.exe'
$crawlerFile = Join-Path $projectRoot 'crawlers\sichuan_health_crawler.py'

if (-not (Test-Path $pythonExe)) {
    Write-Host "ERROR: cannot find virtual environment Python: $pythonExe" -ForegroundColor Red
    Read-Host "Press Enter to exit"
    exit 1
}

if (-not (Test-Path $crawlerFile)) {
    Write-Host "ERROR: cannot find crawler entry point: $crawlerFile" -ForegroundColor Red
    Read-Host "Press Enter to exit"
    exit 1
}

$argsList = @(
    $crawlerFile,
    '--min-year', $MinYear.ToString(),
    '--sections', $Sections,
    '--year-filter-source', $YearFilterSource
)

if ($DisableOCR) {
    $argsList += '--disable-ocr'
}

Write-Host "[2/2] Running Sichuan crawler..." -ForegroundColor Yellow
Write-Host "Command: $pythonExe -X utf8 $($argsList -join ' ')" -ForegroundColor Gray
Write-Host ""

& $pythonExe -X utf8 @argsList
