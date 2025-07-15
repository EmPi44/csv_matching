from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
import os
import logging
from pathlib import Path

from csv_processor import CSVProcessor
from api_models import (
    CSVInfoResponse, ColumnRemovalRequest, ColumnRemovalResponse,
    FileSplitRequest, FileSplitResponse, ProcessAndSplitRequest,
    ProcessAndSplitResponse, ErrorResponse
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="CSV Processing API",
    description="A FastAPI backend for processing large CSV files with column removal and file splitting capabilities",
    version="1.0.0"
)

# Initialize CSV processor
csv_processor = CSVProcessor(chunk_size=10000)

@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "message": "CSV Processing API",
        "version": "1.0.0",
        "endpoints": {
            "get_csv_info": "/csv/info",
            "remove_columns": "/csv/remove-columns",
            "split_file": "/csv/split",
            "process_and_split": "/csv/process-and-split"
        }
    }

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "csv_processor"}

@app.get("/csv/info/{file_path:path}", response_model=CSVInfoResponse)
async def get_csv_info(file_path: str):
    """
    Get information about a CSV file without loading it entirely into memory.
    
    Args:
        file_path: Path to the CSV file (URL encoded)
    """
    try:
        # Decode URL path
        decoded_path = file_path.replace("%2F", "/").replace("%20", " ")
        
        # Validate file exists
        if not os.path.exists(decoded_path):
            raise HTTPException(status_code=404, detail=f"File not found: {decoded_path}")
        
        # Get CSV information
        info = csv_processor.get_csv_info(decoded_path)
        return CSVInfoResponse(**info)
        
    except Exception as e:
        logger.error(f"Error getting CSV info: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/csv/remove-columns", response_model=ColumnRemovalResponse)
async def remove_columns(request: ColumnRemovalRequest):
    """
    Remove specified columns from a CSV file and save to a new file.
    
    Args:
        request: ColumnRemovalRequest containing file paths and columns to remove
    """
    try:
        # Validate input file exists
        if not os.path.exists(request.input_file):
            raise HTTPException(status_code=404, detail=f"Input file not found: {request.input_file}")
        
        # Create output directory if needed
        output_dir = os.path.dirname(request.output_file)
        if output_dir:
            Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        # Process the file
        result = csv_processor.remove_columns(
            request.input_file,
            request.output_file,
            request.columns_to_remove
        )
        
        return ColumnRemovalResponse(**result)
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error removing columns: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/csv/split", response_model=FileSplitResponse)
async def split_file(request: FileSplitRequest):
    """
    Split a CSV file into smaller files based on size limit.
    
    Args:
        request: FileSplitRequest containing file path, output directory, and size limit
    """
    try:
        # Validate input file exists
        if not os.path.exists(request.input_file):
            raise HTTPException(status_code=404, detail=f"Input file not found: {request.input_file}")
        
        # Process the file
        result = csv_processor.split_file_by_size(
            request.input_file,
            request.output_dir,
            request.max_size_mb
        )
        
        return FileSplitResponse(**result)
        
    except Exception as e:
        logger.error(f"Error splitting file: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/csv/process-and-split", response_model=ProcessAndSplitResponse)
async def process_and_split(request: ProcessAndSplitRequest):
    """
    Combined operation: remove columns and then split the file.
    
    Args:
        request: ProcessAndSplitRequest containing all parameters for the combined operation
    """
    try:
        # Validate input file exists
        if not os.path.exists(request.input_file):
            raise HTTPException(status_code=404, detail=f"Input file not found: {request.input_file}")
        
        # Process the file
        result = csv_processor.process_and_split(
            request.input_file,
            request.output_dir,
            request.columns_to_remove,
            request.max_size_mb
        )
        
        return ProcessAndSplitResponse(**result)
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error in process and split: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler for unhandled errors."""
    logger.error(f"Unhandled exception: {exc}")
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error", "detail": str(exc)}
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 