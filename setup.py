#!/usr/bin/env python3
"""
Setup script for CSV Processing Backend.
Creates a virtual environment and installs all dependencies.
"""

import os
import sys
import subprocess
import venv
from pathlib import Path

def run_command(command, description):
    """Run a command and handle errors."""
    print(f"🔄 {description}...")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"✅ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed:")
        print(f"   Error: {e.stderr}")
        return False

def create_virtual_environment():
    """Create a virtual environment."""
    venv_path = Path("venv")
    
    if venv_path.exists():
        print("📁 Virtual environment already exists at 'venv'")
        return True
    
    print("🔧 Creating virtual environment...")
    try:
        venv.create("venv", with_pip=True)
        print("✅ Virtual environment created successfully")
        return True
    except Exception as e:
        print(f"❌ Failed to create virtual environment: {e}")
        return False

def get_python_executable():
    """Get the Python executable path for the virtual environment."""
    if sys.platform == "win32":
        return "venv\\Scripts\\python.exe"
    else:
        return "venv/bin/python"

def get_pip_executable():
    """Get the pip executable path for the virtual environment."""
    if sys.platform == "win32":
        return "venv\\Scripts\\pip.exe"
    else:
        return "venv/bin/pip"

def upgrade_pip():
    """Upgrade pip in the virtual environment."""
    pip_cmd = get_pip_executable()
    return run_command(f"{pip_cmd} install --upgrade pip", "Upgrading pip")

def install_requirements():
    """Install requirements from requirements.txt."""
    pip_cmd = get_pip_executable()
    return run_command(f"{pip_cmd} install -r requirements.txt", "Installing dependencies")

def create_activation_script():
    """Create activation script for easy virtual environment activation."""
    if sys.platform == "win32":
        script_content = """@echo off
echo Activating virtual environment...
call venv\\Scripts\\activate.bat
echo Virtual environment activated!
echo You can now run: python start_server.py
cmd /k
"""
        script_path = "activate_env.bat"
    else:
        script_content = """#!/bin/bash
echo "Activating virtual environment..."
source venv/bin/activate
echo "Virtual environment activated!"
echo "You can now run: python start_server.py"
exec $SHELL
"""
        script_path = "activate_env.sh"
    
    with open(script_path, 'w') as f:
        f.write(script_content)
    
    # Make the script executable on Unix systems
    if sys.platform != "win32":
        os.chmod(script_path, 0o755)
    
    print(f"📝 Created activation script: {script_path}")

def main():
    """Main setup function."""
    print("🚀 Setting up CSV Processing Backend...")
    print("=" * 50)
    
    # Check if Python 3.7+ is available
    if sys.version_info < (3, 7):
        print("❌ Python 3.7 or higher is required")
        sys.exit(1)
    
    print(f"✅ Python {sys.version_info.major}.{sys.version_info.minor} detected")
    
    # Step 1: Create virtual environment
    if not create_virtual_environment():
        sys.exit(1)
    
    # Step 2: Upgrade pip
    if not upgrade_pip():
        print("⚠️  Pip upgrade failed, but continuing...")
    
    # Step 3: Install requirements
    if not install_requirements():
        print("❌ Failed to install dependencies")
        sys.exit(1)
    
    # Step 4: Create activation script
    create_activation_script()
    
    print("\n" + "=" * 50)
    print("🎉 Setup completed successfully!")
    print("\n📋 Next steps:")
    print("1. Activate the virtual environment:")
    if sys.platform == "win32":
        print("   activate_env.bat")
    else:
        print("   source activate_env.sh")
    print("2. Start the server:")
    print("   python start_server.py")
    print("3. Open your browser to:")
    print("   http://localhost:8000/docs")
    print("\n📚 For more information, see README.md")

if __name__ == "__main__":
    main() 