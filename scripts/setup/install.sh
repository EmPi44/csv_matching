#!/bin/bash

# CSV Processing Backend Installation Script
# This script sets up the virtual environment and installs dependencies

set -e  # Exit on any error

echo "🚀 CSV Processing Backend - Installation Script"
echo "================================================"

# Check if Python 3.7+ is available
python_version=$(python3 --version 2>&1 | grep -oP '\d+\.\d+' | head -1)
if [[ -z "$python_version" ]]; then
    echo "❌ Python 3 not found. Please install Python 3.7 or higher."
    exit 1
fi

echo "✅ Python $python_version detected"

# Check if virtual environment already exists
if [[ -d "venv" ]]; then
    echo "📁 Virtual environment already exists at 'venv'"
    echo "🔄 Removing existing virtual environment..."
    rm -rf venv
fi

# Create virtual environment
echo "🔧 Creating virtual environment..."
python3 -m venv venv

# Activate virtual environment
echo "🔄 Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo "🔄 Upgrading pip..."
pip install --upgrade pip

# Install requirements
echo "🔄 Installing dependencies..."
pip install -r requirements.txt

# Create activation script
echo "📝 Creating activation script..."
cat > activate_env.sh << 'EOF'
#!/bin/bash
echo "Activating virtual environment..."
source venv/bin/activate
echo "Virtual environment activated!"
echo "You can now run: python start_server.py"
exec $SHELL
EOF

chmod +x activate_env.sh

echo ""
echo "🎉 Installation completed successfully!"
echo ""
echo "📋 Next steps:"
echo "1. Activate the virtual environment:"
echo "   source activate_env.sh"
echo ""
echo "2. Start the server:"
echo "   python start_server.py"
echo ""
echo "3. Open your browser to:"
echo "   http://localhost:8000/docs"
echo ""
echo "📚 For more information, see README.md" 