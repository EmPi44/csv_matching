@echo off
REM CSV Processing Backend Installation Script
REM This script sets up the virtual environment and installs dependencies

echo 🚀 CSV Processing Backend - Installation Script
echo ================================================

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Python not found. Please install Python 3.7 or higher.
    pause
    exit /b 1
)

echo ✅ Python detected

REM Check if virtual environment already exists
if exist "venv" (
    echo 📁 Virtual environment already exists at 'venv'
    echo 🔄 Removing existing virtual environment...
    rmdir /s /q venv
)

REM Create virtual environment
echo 🔧 Creating virtual environment...
python -m venv venv

REM Activate virtual environment
echo 🔄 Activating virtual environment...
call venv\Scripts\activate.bat

REM Upgrade pip
echo 🔄 Upgrading pip...
python -m pip install --upgrade pip

REM Install requirements
echo 🔄 Installing dependencies...
pip install -r requirements.txt

REM Create activation script
echo 📝 Creating activation script...
(
echo @echo off
echo echo Activating virtual environment...
echo call venv\Scripts\activate.bat
echo echo Virtual environment activated!
echo echo You can now run: python start_server.py
echo cmd /k
) > activate_env.bat

echo.
echo 🎉 Installation completed successfully!
echo.
echo 📋 Next steps:
echo 1. Activate the virtual environment:
echo    activate_env.bat
echo.
echo 2. Start the server:
echo    python start_server.py
echo.
echo 3. Open your browser to:
echo    http://localhost:8000/docs
echo.
echo 📚 For more information, see README.md
pause 