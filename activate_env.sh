#!/bin/bash
echo "Activating virtual environment..."
source venv/bin/activate
echo "Virtual environment activated!"
echo "You can now run: python start_server.py"
exec $SHELL
