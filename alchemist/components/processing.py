"""Data processing components."""

import asyncio
import re
from typing import Any, Dict, List, Optional, Union, Callable
import time

from ..core.component import Component, ComponentResult, ComponentStatus


class DataProcessing(Component):
    """Base class for data processing components."""
    
    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None):
        super().__init__(name, config)
        self.operations = config.get("operations", []) if config else []
    
    def validate_config(self) -> bool:
        """Validate processing configuration."""
        return True


class FilterProcessor(DataProcessing):
    """Filter data based on conditions."""
    
    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None):
        super().__init__(name, config)
        self.filter_conditions = config.get("filter_conditions", {}) if config else {}
    
    async def execute(self, inputs: Dict[str, Any]) -> ComponentResult:
        """Execute data filtering."""
        start_time = time.time()
        
        try:
            input_data = None
            for key, value in inputs.items():
                if isinstance(value, (list, dict)):
                    input_data = value
                    break
            
            if input_data is None:
                raise ValueError("No valid input data found")
            
            filtered_data = await self._apply_filters(input_data)
            
            return ComponentResult(
                status=ComponentStatus.COMPLETED,
                data=filtered_data,
                metadata={
                    "original_count": len(input_data) if isinstance(input_data, list) else 1,
                    "filtered_count": len(filtered_data) if isinstance(filtered_data, list) else 1,
                    "filter_conditions": self.filter_conditions
                },
                errors=[],
                execution_time=time.time() - start_time
            )
            
        except Exception as e:
            return ComponentResult(
                status=ComponentStatus.FAILED,
                data=None,
                metadata={},
                errors=[f"Filtering failed: {str(e)}"],
                execution_time=time.time() - start_time
            )
    
    async def _apply_filters(self, data: Union[List, Dict]) -> Union[List, Dict]:
        """Apply filter conditions to data."""
        if isinstance(data, list):
            filtered = []
            for item in data:
                if self._meets_conditions(item):
                    filtered.append(item)
            return filtered
        elif isinstance(data, dict):
            if self._meets_conditions(data):
                return data
            else:
                return {}
        return data
    
    def _meets_conditions(self, item: Dict) -> bool:
        """Check if item meets filter conditions."""
        for field, condition in self.filter_conditions.items():
            if field not in item:
                return False
            
            value = item[field]
            if isinstance(condition, dict):
                op = condition.get("operator", "eq")
                target = condition.get("value")
                
                if op == "eq" and value != target:
                    return False
                elif op == "ne" and value == target:
                    return False
                elif op == "gt" and value <= target:
                    return False
                elif op == "lt" and value >= target:
                    return False
                elif op == "contains" and target not in str(value):
                    return False
            else:
                if value != condition:
                    return False
        
        return True


class TransformProcessor(DataProcessing):
    """Transform data structure and values."""
    
    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None):
        super().__init__(name, config)
        self.transformations = config.get("transformations", {}) if config else {}
    
    async def execute(self, inputs: Dict[str, Any]) -> ComponentResult:
        """Execute data transformation."""
        start_time = time.time()
        
        try:
            input_data = None
            for key, value in inputs.items():
                if isinstance(value, (list, dict)):
                    input_data = value
                    break
            
            if input_data is None:
                raise ValueError("No valid input data found")
            
            transformed_data = await self._apply_transformations(input_data)
            
            return ComponentResult(
                status=ComponentStatus.COMPLETED,
                data=transformed_data,
                metadata={
                    "transformations_applied": list(self.transformations.keys()),
                    "record_count": len(transformed_data) if isinstance(transformed_data, list) else 1
                },
                errors=[],
                execution_time=time.time() - start_time
            )
            
        except Exception as e:
            return ComponentResult(
                status=ComponentStatus.FAILED,
                data=None,
                metadata={},
                errors=[f"Transformation failed: {str(e)}"],
                execution_time=time.time() - start_time
            )
    
    async def _apply_transformations(self, data: Union[List, Dict]) -> Union[List, Dict]:
        """Apply transformations to data."""
        if isinstance(data, list):
            return [self._transform_item(item) for item in data]
        elif isinstance(data, dict):
            return self._transform_item(data)
        return data
    
    def _transform_item(self, item: Dict) -> Dict:
        """Transform a single data item."""
        transformed = item.copy()
        
        for field, transformation in self.transformations.items():
            if field in transformed:
                value = transformed[field]
                
                if transformation == "uppercase":
                    transformed[field] = str(value).upper()
                elif transformation == "lowercase":
                    transformed[field] = str(value).lower()
                elif transformation == "strip":
                    transformed[field] = str(value).strip()
                elif isinstance(transformation, dict):
                    if transformation.get("type") == "regex_replace":
                        pattern = transformation.get("pattern")
                        replacement = transformation.get("replacement", "")
                        transformed[field] = re.sub(pattern, replacement, str(value))
                    elif transformation.get("type") == "map":
                        mapping = transformation.get("mapping", {})
                        transformed[field] = mapping.get(value, value)
        
        return transformed


class AggregationProcessor(DataProcessing):
    """Aggregate data using various functions."""
    
    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None):
        super().__init__(name, config)
        self.aggregation_functions = config.get("aggregation_functions", {}) if config else {}
        self.group_by = config.get("group_by") if config else None
    
    async def execute(self, inputs: Dict[str, Any]) -> ComponentResult:
        """Execute data aggregation."""
        start_time = time.time()
        
        try:
            input_data = None
            for key, value in inputs.items():
                if isinstance(value, list):
                    input_data = value
                    break
            
            if input_data is None:
                raise ValueError("No valid list input data found for aggregation")
            
            aggregated_data = await self._perform_aggregation(input_data)
            
            return ComponentResult(
                status=ComponentStatus.COMPLETED,
                data=aggregated_data,
                metadata={
                    "original_count": len(input_data),
                    "aggregation_functions": list(self.aggregation_functions.keys()),
                    "group_by": self.group_by
                },
                errors=[],
                execution_time=time.time() - start_time
            )
            
        except Exception as e:
            return ComponentResult(
                status=ComponentStatus.FAILED,
                data=None,
                metadata={},
                errors=[f"Aggregation failed: {str(e)}"],
                execution_time=time.time() - start_time
            )
    
    async def _perform_aggregation(self, data: List[Dict]) -> Dict:
        """Perform aggregation on data."""
        if self.group_by:
            return self._group_and_aggregate(data)
        else:
            return self._aggregate_all(data)
    
    def _aggregate_all(self, data: List[Dict]) -> Dict:
        """Aggregate all data without grouping."""
        result = {"total_records": len(data)}
        
        for field, func in self.aggregation_functions.items():
            values = [item.get(field) for item in data if field in item and item[field] is not None]
            
            if not values:
                result[f"{field}_{func}"] = None
                continue
            
            try:
                numeric_values = [float(v) for v in values]
                if func == "sum":
                    result[f"{field}_{func}"] = sum(numeric_values)
                elif func == "avg":
                    result[f"{field}_{func}"] = sum(numeric_values) / len(numeric_values)
                elif func == "min":
                    result[f"{field}_{func}"] = min(numeric_values)
                elif func == "max":
                    result[f"{field}_{func}"] = max(numeric_values)
                elif func == "count":
                    result[f"{field}_{func}"] = len(values)
            except (ValueError, TypeError):
                if func == "count":
                    result[f"{field}_{func}"] = len(values)
                else:
                    result[f"{field}_{func}"] = None
        
        return result
    
    def _group_and_aggregate(self, data: List[Dict]) -> Dict:
        """Group data and aggregate within groups."""
        groups = {}
        
        for item in data:
            group_key = item.get(self.group_by, "unknown")
            if group_key not in groups:
                groups[group_key] = []
            groups[group_key].append(item)
        
        result = {}
        for group_key, group_data in groups.items():
            result[str(group_key)] = self._aggregate_all(group_data)
        
        return result