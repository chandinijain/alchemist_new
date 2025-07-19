"""
Alchemist: A flexible workflow system for data processing and reasoning.

Components:
- Data Ingestion: Load data from various sources
- Data Processing: Transform and prepare data
- Reasoning: Probabilistic (LLM) or deterministic (API/function calls)
- Aggregation: Combine and summarize results
- Output: Export results in various formats
"""

__version__ = "0.1.0"
__author__ = "Chandini Jain"

from .core.workflow import Workflow
from .core.component import Component
from .components.ingestion import DataIngestion
from .components.processing import DataProcessing
from .components.reasoning import ReasoningEngine
from .components.aggregation import ResultAggregation
from .components.output import OutputHandler

__all__ = [
    "Workflow",
    "Component", 
    "DataIngestion",
    "DataProcessing",
    "ReasoningEngine",
    "ResultAggregation", 
    "OutputHandler"
]