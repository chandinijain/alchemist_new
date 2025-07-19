"""Data ingestion components."""

import asyncio
import json
import csv
from typing import Any, Dict, List, Optional, Union
from pathlib import Path
import time

from ..core.component import Component, ComponentResult, ComponentStatus


class DataIngestion(Component):
    """Base class for data ingestion components."""
    
    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None):
        super().__init__(name, config)
        self.source_type = config.get("source_type", "file") if config else "file"
        self.source_path = config.get("source_path") if config else None
    
    def validate_config(self) -> bool:
        """Validate ingestion configuration."""
        if not self.source_path:
            return False
        return True


class FileIngestion(DataIngestion):
    """Ingest data from files (JSON, CSV, TXT)."""
    
    async def execute(self, inputs: Dict[str, Any]) -> ComponentResult:
        """Execute file ingestion."""
        start_time = time.time()
        
        try:
            file_path = Path(self.source_path)
            
            if not file_path.exists():
                return ComponentResult(
                    status=ComponentStatus.FAILED,
                    data=None,
                    metadata={},
                    errors=[f"File not found: {self.source_path}"],
                    execution_time=time.time() - start_time
                )
            
            data = await self._read_file(file_path)
            
            return ComponentResult(
                status=ComponentStatus.COMPLETED,
                data=data,
                metadata={
                    "source_path": str(file_path),
                    "file_size": file_path.stat().st_size,
                    "records_count": len(data) if isinstance(data, list) else 1
                },
                errors=[],
                execution_time=time.time() - start_time
            )
            
        except Exception as e:
            return ComponentResult(
                status=ComponentStatus.FAILED,
                data=None,
                metadata={},
                errors=[f"Ingestion failed: {str(e)}"],
                execution_time=time.time() - start_time
            )
    
    async def _read_file(self, file_path: Path) -> Union[List[Dict], Dict, str]:
        """Read file based on extension."""
        suffix = file_path.suffix.lower()
        
        if suffix == ".json":
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        elif suffix == ".csv":
            records = []
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    records.append(row)
            return records
        elif suffix in [".txt", ".md"]:
            with open(file_path, 'r', encoding='utf-8') as f:
                return f.read()
        else:
            raise ValueError(f"Unsupported file type: {suffix}")


class APIIngestion(DataIngestion):
    """Ingest data from REST APIs."""
    
    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None):
        super().__init__(name, config)
        self.url = config.get("url") if config else None
        self.method = config.get("method", "GET") if config else "GET"
        self.headers = config.get("headers", {}) if config else {}
        self.params = config.get("params", {}) if config else {}
    
    def validate_config(self) -> bool:
        """Validate API ingestion configuration."""
        return bool(self.url)
    
    async def execute(self, inputs: Dict[str, Any]) -> ComponentResult:
        """Execute API ingestion."""
        start_time = time.time()
        
        try:
            # For now, simulate API call - replace with actual HTTP client
            await asyncio.sleep(0.1)  # Simulate network delay
            
            # Mock response - replace with actual API call
            mock_data = {
                "status": "success",
                "data": [
                    {"id": 1, "value": "sample_data_1"},
                    {"id": 2, "value": "sample_data_2"}
                ]
            }
            
            return ComponentResult(
                status=ComponentStatus.COMPLETED,
                data=mock_data,
                metadata={
                    "url": self.url,
                    "method": self.method,
                    "response_size": len(str(mock_data))
                },
                errors=[],
                execution_time=time.time() - start_time
            )
            
        except Exception as e:
            return ComponentResult(
                status=ComponentStatus.FAILED,
                data=None,
                metadata={},
                errors=[f"API ingestion failed: {str(e)}"],
                execution_time=time.time() - start_time
            )


class DatabaseIngestion(DataIngestion):
    """Ingest data from databases."""
    
    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None):
        super().__init__(name, config)
        self.connection_string = config.get("connection_string") if config else None
        self.query = config.get("query") if config else None
    
    def validate_config(self) -> bool:
        """Validate database ingestion configuration."""
        return bool(self.connection_string and self.query)
    
    async def execute(self, inputs: Dict[str, Any]) -> ComponentResult:
        """Execute database ingestion."""
        start_time = time.time()
        
        try:
            # Simulate database query - replace with actual database client
            await asyncio.sleep(0.2)  # Simulate query time
            
            # Mock database results
            mock_results = [
                {"id": 1, "name": "Record 1", "timestamp": "2024-01-01T00:00:00Z"},
                {"id": 2, "name": "Record 2", "timestamp": "2024-01-01T01:00:00Z"},
                {"id": 3, "name": "Record 3", "timestamp": "2024-01-01T02:00:00Z"}
            ]
            
            return ComponentResult(
                status=ComponentStatus.COMPLETED,
                data=mock_results,
                metadata={
                    "query": self.query,
                    "records_count": len(mock_results)
                },
                errors=[],
                execution_time=time.time() - start_time
            )
            
        except Exception as e:
            return ComponentResult(
                status=ComponentStatus.FAILED,
                data=None,
                metadata={},
                errors=[f"Database ingestion failed: {str(e)}"],
                execution_time=time.time() - start_time
            )