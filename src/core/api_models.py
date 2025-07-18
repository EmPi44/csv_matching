from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any

class CSVInfoResponse(BaseModel):
    """Response model for CSV file information."""
    file_size_mb: float
    total_rows: int
    columns: List[str]
    column_count: int

class ColumnRemovalRequest(BaseModel):
    """Request model for column removal operation."""
    input_file: str = Field(..., description="Path to input CSV file")
    output_file: str = Field(..., description="Path to output CSV file")
    columns_to_remove: List[str] = Field(..., description="List of column names to remove")

class ColumnRemovalResponse(BaseModel):
    """Response model for column removal operation."""
    success: bool
    input_file: str
    output_file: str
    columns_removed: List[str]
    columns_kept: List[str]
    chunks_processed: int
    total_rows_processed: int
    output_size_mb: float

class FileSplitRequest(BaseModel):
    """Request model for file splitting operation."""
    input_file: str = Field(..., description="Path to input CSV file")
    output_dir: str = Field(..., description="Directory to save split files")
    max_size_mb: float = Field(..., gt=0, description="Maximum size in MB for each split file")

class SplitFileInfo(BaseModel):
    """Model for individual split file information."""
    file_path: str
    rows: int
    size_mb: float

class FileSplitResponse(BaseModel):
    """Response model for file splitting operation."""
    success: bool
    input_file: str
    output_directory: str
    split_files: List[SplitFileInfo]
    total_files_created: int
    total_rows_processed: int
    max_size_mb: float

class ProcessAndSplitRequest(BaseModel):
    """Request model for combined process and split operation."""
    input_file: str = Field(..., description="Path to input CSV file")
    output_dir: str = Field(..., description="Directory to save processed files")
    columns_to_remove: List[str] = Field(..., description="List of column names to remove")
    max_size_mb: float = Field(..., gt=0, description="Maximum size in MB for each split file")

class ProcessAndSplitResponse(BaseModel):
    """Response model for combined process and split operation."""
    success: bool
    column_removal: ColumnRemovalResponse
    file_splitting: FileSplitResponse
    final_output_directory: str

class ErrorResponse(BaseModel):
    """Response model for error cases."""
    error: str
    detail: Optional[str] = None 