# 简化启动脚本 - 智能处理 Docker 和应用启动

# 设置 UTF-8 编码以正确显示中文
[Console]::InputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$OutputEncoding = [System.Text.UTF8Encoding]::new($false)

# 强制 PowerShell 使用 UTF-8
if ($PSVersionTable.PSVersion.Major -ge 6) {
    $PSDefaultParameterValues['*:Encoding'] = 'UTF8'
}

$ErrorActionPreference = 'Continue'

Write-Host "================================" -ForegroundColor Cyan
Write-Host "健康大数据系统启动器" -ForegroundColor Cyan
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
    Write-Host "❌ 无法定位项目根目录，请确认脚本位于仓库的 JB 目录下。" -ForegroundColor Red
    Read-Host "按Enter键退出"
    exit 1
}

Set-Location $projectRoot

# 检查虚拟环境
Write-Host "[1/3] 检查虚拟环境..." -ForegroundColor Yellow
if (-not (Test-Path "venv\Scripts\python.exe")) {
    Write-Host "❌ Python虚拟环境不存在" -ForegroundColor Red
    Write-Host "请先运行: python -m venv venv" -ForegroundColor Yellow
    Write-Host "然后运行: .\venv\Scripts\pip install -r requirements.txt"
    Read-Host "按Enter键退出"
    exit 1
}
Write-Host "✅ 虚拟环境正常" -ForegroundColor Green

# 检查Docker（可选）
Write-Host "[2/3] 检查Docker和数据库..." -ForegroundColor Yellow
$dockerAvailable = $null -ne (Get-Command docker -ErrorAction SilentlyContinue)

if ($dockerAvailable) {
    Write-Host "  🐳 Docker已安装，尝试启动容器..." -ForegroundColor Cyan
    
    # 尝试启动容器，但失败时继续
    try {
        @('health_mysql', 'health_redis') | ForEach-Object {
            $exists = docker ps -a --format "{{.Names}}" | Select-String -Pattern "^$_`$"
            if ($exists) {
                docker start $_ 2>&1 | Out-Null
                Write-Host "  ✅ 容器已启动: $_"
            }
        }
    }
    catch {
        Write-Host "  ⚠️  Docker启动失败，将继续启动Flask应用" -ForegroundColor Yellow
    }
}
else {
    Write-Host "  ℹ️  未检测到Docker，请确保MySQL和Redis已在本地运行" -ForegroundColor Cyan
    Write-Host "     MySQL: localhost:3307" -ForegroundColor Gray
    Write-Host "     Redis: localhost:6379" -ForegroundColor Gray
}

# 启动应用
Write-Host "[3/3] 启动Flask应用..." -ForegroundColor Yellow
Write-Host ""

$pythonExe = Join-Path $projectRoot 'venv\Scripts\python.exe'
$appFile = Join-Path $projectRoot 'run.py'

if (-not (Test-Path $pythonExe)) {
    Write-Host "❌ 找不到虚拟环境 Python: $pythonExe" -ForegroundColor Red
    Read-Host "按Enter键退出"
    exit 1
}

if (-not (Test-Path $appFile)) {
    Write-Host "❌ 找不到应用入口: $appFile" -ForegroundColor Red
    Read-Host "按Enter键退出"
    exit 1
}

Write-Host "=================================================" -ForegroundColor Green
Write-Host "✅ Flask应用正在启动..." -ForegroundColor Green
Write-Host "=================================================" -ForegroundColor Green
Write-Host ""
Write-Host "📱 访问地址:" -ForegroundColor Cyan
Write-Host "  • 登录页面: http://127.0.0.1:5000/login" -ForegroundColor White
Write-Host "  • 管理员账号:" -ForegroundColor White
Write-Host "    用户名: admin" -ForegroundColor Gray
Write-Host "    密码: admin123" -ForegroundColor Gray
Write-Host ""
Write-Host "按 Ctrl+C 停止服务" -ForegroundColor Yellow
Write-Host ""

& $pythonExe $appFile
