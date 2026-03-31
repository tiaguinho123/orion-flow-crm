@echo off
title Orion Flow - CRM Dashboard Setup
color 0A

echo ============================================================
echo    Orion Flow - CRM Lead Dashboard
echo    Instalacao Automatica
echo ============================================================
echo.

:: Check Python
python --version >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [ERRO] Python nao encontrado. Instale em python.org
    pause
    exit /b 1
)

:: Create virtual environment
if not exist "venv\" (
    echo [1/3] Criando ambiente virtual...
    python -m venv venv
) else (
    echo [1/3] Ambiente virtual ja existe.
)

:: Activate and install
echo [2/3] Instalando dependencias...
call venv\Scripts\activate.bat
pip install -r requirements.txt --quiet

:: Run
echo [3/3] Iniciando o dashboard...
echo.
echo ============================================================
echo    Dashboard disponivel em: http://localhost:5000
echo    Pressione Ctrl+C para parar
echo ============================================================
echo.
python app.py

pause
