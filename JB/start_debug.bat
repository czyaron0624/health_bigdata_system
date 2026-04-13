@echo off
REM Debug启动脚本 - 显示详细错误信息而不是直接关闭窗口
setlocal enabledelayedexpansion

cd /d "%~dp0.."

echo.
echo ================================
echo 健康大数据系统 - 调试启动
echo ================================
echo.

REM 检查虚拟环境
if not exist "venv\Scripts\python.exe" (
    echo [ERROR] 虚拟环境不存在，请先运行: python -m venv venv
    pause
    exit /b 1
)

REM 激活虚拟环境并运行应用
echo [1/2] 激活Python虚拟环境...
call venv\Scripts\activate.bat

if %ERRORLEVEL% neq 0 (
    echo [ERROR] 虚拟环境激活失败
    pause
    exit /b 1
)

echo [2/2] 启动Flask应用...
echo.

REM 运行应用，保持窗口打开以显示错误
python run.py

if %ERRORLEVEL% neq 0 (
    echo.
    echo [ERROR] 应用启动失败，错误代码: %ERRORLEVEL%
    echo.
    pause
    exit /b 1
)

pause
