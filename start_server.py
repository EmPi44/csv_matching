#!/usr/bin/env python3
"""
Simple script to start the CSV processing server.
Configured for handling large files efficiently.
"""

import uvicorn
import os
import sys

def main():
    """Start the FastAPI server with optimized settings for large file processing."""
    
    # Check if virtual environment is activated
    if not hasattr(sys, 'real_prefix') and not (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
        print("⚠️  Virtual environment not detected!")
        print("Please activate the virtual environment first:")
        print("  - macOS/Linux: source activate_env.sh")
        print("  - Windows: activate_env.bat")
        print("  - Or run: python setup.py to set up automatically")
        sys.exit(1)
    
    # Check if dependencies are installed
    try:
        import fastapi
        import pandas
        import pydantic
    except ImportError as e:
        print(f"❌ Missing dependency: {e}")
        print("Please install dependencies with: pip install -r requirements.txt")
        print("Or run: python setup.py to set up automatically")
        sys.exit(1)
    
    print("🚀 Starting CSV Processing Server...")
    print("📁 Server will be available at: http://localhost:8000")
    print("📚 API Documentation: http://localhost:8000/docs")
    print("🔧 Press Ctrl+C to stop the server")
    print()
    
    # Configure server for large file processing
    config = {
        "host": "0.0.0.0",
        "port": 8000,
        "reload": False,  # Disable reload for production
        "workers": 1,     # Single worker for large file processing
        "log_level": "info",
        "access_log": True
    }
    
    try:
        uvicorn.run("main:app", **config)
    except KeyboardInterrupt:
        print("\n👋 Server stopped by user")
    except Exception as e:
        print(f"❌ Error starting server: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 