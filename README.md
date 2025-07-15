# CSV Processing Backend

A Python FastAPI backend for processing large CSV files (900MB+) with capabilities for column removal and file splitting based on size.

## Features

- **Memory-efficient processing**: Handles large CSV files without loading them entirely into memory
- **Column removal**: Remove irrelevant columns from CSV data
- **File splitting**: Split large files into smaller chunks based on size limits
- **Combined operations**: Remove columns and split files in one operation
- **RESTful API**: FastAPI-based web service with automatic documentation
- **Progress tracking**: Real-time logging of processing progress

## Installation

### Option 1: One-Click Installation (Recommended)

**On macOS/Linux:**
```bash
chmod +x install.sh
./install.sh
```

**On Windows:**
```bash
install.bat
```

### Option 2: Python Setup Script

1. Clone or download the project files
2. Run the automated setup script:

```bash
python setup.py
```

This will:
- Create a virtual environment
- Install all dependencies
- Create activation scripts for easy use

### Option 3: Manual Setup

1. Clone or download the project files
2. Create a virtual environment:

```bash
python -m venv venv
```

3. Activate the virtual environment:

**On macOS/Linux:**
```bash
source venv/bin/activate
```

**On Windows:**
```bash
venv\Scripts\activate
```

4. Install dependencies:

```bash
pip install -r requirements.txt
```

### Activating the Virtual Environment

After installation, activate the virtual environment:

**On macOS/Linux:**
```bash
source activate_env.sh
```

**On Windows:**
```bash
activate_env.bat
```

### Option 2: Manual Setup

1. Clone or download the project files
2. Create a virtual environment:

```bash
python -m venv venv
```

3. Activate the virtual environment:

**On macOS/Linux:**
```bash
source venv/bin/activate
```

**On Windows:**
```bash
venv\Scripts\activate
```

4. Install dependencies:

```bash
pip install -r requirements.txt
```

## Quick Start

### Running the API Server

```bash
python main.py
```

The server will start on `http://localhost:8000`

### API Documentation

Once the server is running, visit:
- **Interactive API docs**: `http://localhost:8000/docs`
- **Alternative docs**: `http://localhost:8000/redoc`

## Usage Examples

### 1. Using the API

#### Get CSV File Information
```bash
curl "http://localhost:8000/csv/info/path%2Fto%2Fyour%2Ffile.csv"
```

#### Remove Columns
```bash
curl -X POST "http://localhost:8000/csv/remove-columns" \
  -H "Content-Type: application/json" \
  -d '{
    "input_file": "/path/to/input.csv",
    "output_file": "/path/to/output.csv",
    "columns_to_remove": ["unused_column", "metadata_field"]
  }'
```

#### Split File by Size
```bash
curl -X POST "http://localhost:8000/csv/split" \
  -H "Content-Type: application/json" \
  -d '{
    "input_file": "/path/to/input.csv",
    "output_dir": "/path/to/output/directory",
    "max_size_mb": 50
  }'
```

#### Combined Operation (Remove Columns + Split)
```bash
curl -X POST "http://localhost:8000/csv/process-and-split" \
  -H "Content-Type: application/json" \
  -d '{
    "input_file": "/path/to/input.csv",
    "output_dir": "/path/to/output/directory",
    "columns_to_remove": ["unused_column", "metadata_field"],
    "max_size_mb": 50
  }'
```

### 2. Using the Python Module Directly

```python
from csv_processor import CSVProcessor

# Initialize processor
processor = CSVProcessor(chunk_size=10000)

# Get file information
info = processor.get_csv_info("your_large_file.csv")
print(f"File size: {info['file_size_mb']} MB")
print(f"Columns: {info['columns']}")

# Remove irrelevant columns
result = processor.remove_columns(
    "your_large_file.csv",
    "cleaned_data.csv",
    ["unused_column", "metadata_field"]
)

# Split into smaller files
split_result = processor.split_file_by_size(
    "cleaned_data.csv",
    "split_files",
    max_size_mb=50
)
```

### 3. Running the Example Script

```bash
python example_usage.py
```

## API Endpoints

### GET `/`
Root endpoint with API information and available endpoints.

### GET `/health`
Health check endpoint.

### GET `/csv/info/{file_path}`
Get information about a CSV file without loading it entirely into memory.

**Parameters:**
- `file_path`: URL-encoded path to the CSV file

**Response:**
```json
{
  "file_size_mb": 900.5,
  "total_rows": 1000000,
  "columns": ["col1", "col2", "col3"],
  "column_count": 3
}
```

### POST `/csv/remove-columns`
Remove specified columns from a CSV file.

**Request Body:**
```json
{
  "input_file": "/path/to/input.csv",
  "output_file": "/path/to/output.csv",
  "columns_to_remove": ["col1", "col2"]
}
```

### POST `/csv/split`
Split a CSV file into smaller files based on size limit.

**Request Body:**
```json
{
  "input_file": "/path/to/input.csv",
  "output_dir": "/path/to/output/directory",
  "max_size_mb": 50
}
```

### POST `/csv/process-and-split`
Combined operation: remove columns and then split the file.

**Request Body:**
```json
{
  "input_file": "/path/to/input.csv",
  "output_dir": "/path/to/output/directory",
  "columns_to_remove": ["col1", "col2"],
  "max_size_mb": 50
}
```

## Configuration

### Chunk Size
The `chunk_size` parameter in `CSVProcessor` controls how many rows are processed at once. For very large files, you might want to increase this value:

```python
processor = CSVProcessor(chunk_size=50000)  # Process 50k rows at a time
```

### Memory Considerations
- The processor uses pandas' `chunksize` parameter to avoid loading the entire file into memory
- Each chunk is processed independently and written to disk
- Temporary files are cleaned up automatically

## File Structure

```
csv_pipeline/
├── main.py              # FastAPI application
├── csv_processor.py     # Core CSV processing logic
├── api_models.py        # Pydantic models for API
├── example_usage.py     # Example usage script
├── requirements.txt     # Python dependencies
└── README.md           # This file
```

## Error Handling

The API includes comprehensive error handling:
- File not found errors (404)
- Invalid column names (400)
- Processing errors (500)
- Detailed error messages and logging

## Performance Tips

1. **For 900MB+ files**: Use chunk sizes of 10,000-50,000 rows
2. **Column removal**: Process in chunks to avoid memory issues
3. **File splitting**: Choose appropriate size limits based on your use case
4. **Output directories**: Ensure sufficient disk space for processed files

## Dependencies

- **FastAPI**: Modern web framework for building APIs
- **Pandas**: Data manipulation and analysis
- **Pydantic**: Data validation using Python type annotations
- **Uvicorn**: ASGI server for running FastAPI applications

## License

This project is open source and available under the MIT License. 