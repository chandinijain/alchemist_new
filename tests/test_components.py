"""Tests for workflow components."""

import pytest
import asyncio
import json
from pathlib import Path
from unittest.mock import patch, mock_open
from typing import Dict, Any

from alchemist.components.ingestion import FileIngestion, APIIngestion
from alchemist.components.processing import FilterProcessor, TransformProcessor
from alchemist.components.reasoning import DeterministicReasoning, ProbabilisticReasoning
from alchemist.components.aggregation import MergeAggregation, VotingAggregation
from alchemist.components.output import FileOutput, ConsoleOutput
from alchemist.core.component import ComponentStatus


class TestFileIngestion:
    """Test cases for FileIngestion component."""
    
    @pytest.mark.asyncio
    async def test_json_file_ingestion(self, tmp_path):
        """Test ingesting JSON file."""
        # Create test JSON file
        test_data = [{"id": 1, "name": "test"}, {"id": 2, "name": "test2"}]
        json_file = tmp_path / "test.json"
        json_file.write_text(json.dumps(test_data))
        
        # Test ingestion
        ingestion = FileIngestion("test_ingestion", config={
            "source_path": str(json_file),
            "source_type": "file"
        })
        
        result = await ingestion.execute({})
        
        assert result.status == ComponentStatus.COMPLETED
        assert result.data == test_data
        assert result.metadata["records_count"] == 2
    
    @pytest.mark.asyncio
    async def test_file_not_found(self):
        """Test handling of missing file."""
        ingestion = FileIngestion("test_ingestion", config={
            "source_path": "nonexistent.json",
            "source_type": "file"
        })
        
        result = await ingestion.execute({})
        
        assert result.status == ComponentStatus.FAILED
        assert "File not found" in result.errors[0]
    
    def test_validation(self):
        """Test configuration validation."""
        # Valid config
        ingestion = FileIngestion("test", config={"source_path": "test.json"})
        assert ingestion.validate_config() == True
        
        # Invalid config
        ingestion = FileIngestion("test", config={})
        assert ingestion.validate_config() == False


class TestFilterProcessor:
    """Test cases for FilterProcessor component."""
    
    @pytest.mark.asyncio
    async def test_simple_filtering(self):
        """Test basic data filtering."""
        processor = FilterProcessor("filter", config={
            "filter_conditions": {
                "status": "active",
                "score": {"operator": "gt", "value": 80}
            }
        })
        
        input_data = [
            {"id": 1, "status": "active", "score": 85},
            {"id": 2, "status": "inactive", "score": 90},
            {"id": 3, "status": "active", "score": 75},
            {"id": 4, "status": "active", "score": 95}
        ]
        
        result = await processor.execute({"data": input_data})
        
        assert result.status == ComponentStatus.COMPLETED
        assert len(result.data) == 2  # Only records 1 and 4 should match
        assert all(item["status"] == "active" and item["score"] > 80 for item in result.data)
    
    @pytest.mark.asyncio
    async def test_no_matching_records(self):
        """Test filtering with no matching records."""
        processor = FilterProcessor("filter", config={
            "filter_conditions": {"status": "nonexistent"}
        })
        
        input_data = [{"id": 1, "status": "active"}]
        result = await processor.execute({"data": input_data})
        
        assert result.status == ComponentStatus.COMPLETED
        assert len(result.data) == 0


class TestTransformProcessor:
    """Test cases for TransformProcessor component."""
    
    @pytest.mark.asyncio
    async def test_basic_transformations(self):
        """Test basic data transformations."""
        processor = TransformProcessor("transform", config={
            "transformations": {
                "name": "uppercase",
                "email": "lowercase",
                "description": "strip"
            }
        })
        
        input_data = [
            {"name": "john doe", "email": "JOHN@EXAMPLE.COM", "description": "  test  "},
            {"name": "jane smith", "email": "JANE@EXAMPLE.COM", "description": " another "}
        ]
        
        result = await processor.execute({"data": input_data})
        
        assert result.status == ComponentStatus.COMPLETED
        assert result.data[0]["name"] == "JOHN DOE"
        assert result.data[0]["email"] == "john@example.com"
        assert result.data[0]["description"] == "test"
    
    @pytest.mark.asyncio
    async def test_regex_transformation(self):
        """Test regex-based transformations."""
        processor = TransformProcessor("transform", config={
            "transformations": {
                "phone": {
                    "type": "regex_replace",
                    "pattern": r"[^0-9]",
                    "replacement": ""
                }
            }
        })
        
        input_data = [{"phone": "(555) 123-4567"}]
        result = await processor.execute({"data": input_data})
        
        assert result.status == ComponentStatus.COMPLETED
        assert result.data[0]["phone"] == "5551234567"


class TestDeterministicReasoning:
    """Test cases for DeterministicReasoning component."""
    
    @pytest.mark.asyncio
    async def test_rule_evaluation(self):
        """Test rule-based reasoning."""
        reasoning = DeterministicReasoning("reasoning", config={
            "rules": [
                {
                    "name": "high_value",
                    "condition": {
                        "type": "field_value",
                        "field": "amount",
                        "operator": "gt",
                        "value": 1000
                    },
                    "conclusion": "High value transaction"
                },
                {
                    "name": "sufficient_count",
                    "condition": {
                        "type": "count_threshold",
                        "threshold": 3
                    },
                    "conclusion": "Sufficient data for analysis"
                }
            ]
        })
        
        input_data = [
            {"amount": 1500},
            {"amount": 800},
            {"amount": 1200},
            {"amount": 500}
        ]
        
        result = await reasoning.execute({"data": input_data})
        
        assert result.status == ComponentStatus.COMPLETED
        assert len(result.data["rule_evaluations"]) == 2
        
        # Check high value rule triggered
        high_value_rule = next(r for r in result.data["rule_evaluations"] if r["rule_name"] == "high_value")
        assert high_value_rule["triggered"] == True
        
        # Check count threshold rule triggered
        count_rule = next(r for r in result.data["rule_evaluations"] if r["rule_name"] == "sufficient_count")
        assert count_rule["triggered"] == True
    
    @pytest.mark.asyncio
    async def test_function_calls(self):
        """Test function execution."""
        reasoning = DeterministicReasoning("reasoning", config={
            "functions": {
                "total_amount": {
                    "type": "builtin",
                    "operation": "sum",
                    "field": "amount"
                }
            }
        })
        
        input_data = [{"amount": 100}, {"amount": 200}, {"amount": 300}]
        result = await reasoning.execute({"data": input_data})
        
        assert result.status == ComponentStatus.COMPLETED
        assert len(result.data["function_calls"]) == 1
        assert result.data["function_calls"][0]["result"] == 600


class TestMergeAggregation:
    """Test cases for MergeAggregation component."""
    
    @pytest.mark.asyncio
    async def test_merge_multiple_sources(self):
        """Test merging results from multiple sources."""
        aggregation = MergeAggregation("merge", config={
            "aggregation_strategy": "merge"
        })
        
        inputs = {
            "source1": {
                "insights": ["Insight 1", "Insight 2"],
                "recommendations": ["Rec 1"]
            },
            "source2": {
                "insights": ["Insight 3"],
                "recommendations": ["Rec 2", "Rec 3"],
                "conclusions": ["Conclusion 1"]
            }
        }
        
        result = await aggregation.execute(inputs)
        
        assert result.status == ComponentStatus.COMPLETED
        assert len(result.data["combined_data"]["insights"]) == 3
        assert len(result.data["combined_data"]["recommendations"]) == 3
        assert len(result.data["combined_data"]["conclusions"]) == 1


class TestVotingAggregation:
    """Test cases for VotingAggregation component."""
    
    @pytest.mark.asyncio
    async def test_majority_voting(self):
        """Test majority voting aggregation."""
        aggregation = VotingAggregation("voting", config={
            "voting_method": "majority"
        })
        
        inputs = {
            "source1": {"conclusions": ["positive", "high_quality"]},
            "source2": {"conclusions": ["positive", "low_quality"]},
            "source3": {"conclusions": ["positive", "medium_quality"]}
        }
        
        result = await aggregation.execute(inputs)
        
        assert result.status == ComponentStatus.COMPLETED
        assert result.data["winner"] == "positive"  # Should win with 3 votes


class TestFileOutput:
    """Test cases for FileOutput component."""
    
    @pytest.mark.asyncio
    async def test_json_output(self, tmp_path):
        """Test JSON file output."""
        output_path = tmp_path / "output.json"
        
        output = FileOutput("output", config={
            "output_format": "json",
            "output_path": str(output_path)
        })
        
        test_data = {"results": [1, 2, 3], "status": "completed"}
        result = await output.execute({"data": test_data})
        
        assert result.status == ComponentStatus.COMPLETED
        assert output_path.exists()
        
        # Verify file content
        with open(output_path) as f:
            saved_data = json.load(f)
        assert saved_data == test_data
    
    @pytest.mark.asyncio
    async def test_html_output(self, tmp_path):
        """Test HTML file output."""
        output_path = tmp_path / "output.html"
        
        output = FileOutput("output", config={
            "output_format": "html",
            "output_path": str(output_path)
        })
        
        test_data = {
            "workflow_results": {
                "analysis": {
                    "insights": ["Test insight"],
                    "recommendations": ["Test recommendation"]
                }
            }
        }
        
        result = await output.execute(test_data)
        
        assert result.status == ComponentStatus.COMPLETED
        assert output_path.exists()
        
        # Verify HTML content contains expected elements
        html_content = output_path.read_text()
        assert "<!DOCTYPE html>" in html_content
        assert "Test insight" in html_content
        assert "Test recommendation" in html_content


class TestConsoleOutput:
    """Test cases for ConsoleOutput component."""
    
    @pytest.mark.asyncio
    async def test_console_output(self, capsys):
        """Test console output functionality."""
        output = ConsoleOutput("console")
        
        test_data = {
            "analysis": {
                "insights": ["Important insight"],
                "recommendations": ["Key recommendation"],
                "confidence_scores": {"analysis": 0.95}
            }
        }
        
        result = await output.execute(test_data)
        
        assert result.status == ComponentStatus.COMPLETED
        
        # Check console output
        captured = capsys.readouterr()
        assert "ALCHEMIST WORKFLOW RESULTS" in captured.out
        assert "Important insight" in captured.out
        assert "Key recommendation" in captured.out


if __name__ == "__main__":
    pytest.main([__file__])