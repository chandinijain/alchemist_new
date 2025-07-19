"""Simple workflow example demonstrating basic Alchemist usage."""

import asyncio
import json
from pathlib import Path

from alchemist import Workflow
from alchemist.components.ingestion import FileIngestion
from alchemist.components.processing import FilterProcessor, TransformProcessor
from alchemist.components.reasoning import DeterministicReasoning
from alchemist.components.output import ConsoleOutput


async def create_sample_data():
    """Create sample data file for the example."""
    sample_data = [
        {"id": 1, "name": "Alice", "age": 25, "score": 85, "category": "student"},
        {"id": 2, "name": "Bob", "age": 30, "score": 92, "category": "professional"},
        {"id": 3, "name": "Charlie", "age": 22, "score": 78, "category": "student"},
        {"id": 4, "name": "Diana", "age": 35, "score": 88, "category": "professional"},
        {"id": 5, "name": "Eve", "age": 28, "score": 95, "category": "professional"}
    ]
    
    data_path = Path("sample_data.json")
    with open(data_path, 'w') as f:
        json.dump(sample_data, f, indent=2)
    
    return str(data_path)


async def run_simple_workflow():
    """Run a simple workflow with basic components."""
    
    # Create sample data
    data_file = await create_sample_data()
    
    # Create workflow
    workflow = Workflow("simple_analysis", config={
        "description": "Simple data analysis workflow"
    })
    
    # 1. Data Ingestion
    ingestion = FileIngestion("data_source", config={
        "source_path": data_file,
        "source_type": "file"
    })
    
    # 2. Data Processing - Filter
    filter_processor = FilterProcessor("filter_students", config={
        "filter_conditions": {
            "category": "student"
        }
    })
    
    # 3. Data Processing - Transform
    transform_processor = TransformProcessor("transform_names", config={
        "transformations": {
            "name": "uppercase"
        }
    })
    
    # 4. Reasoning
    reasoning = DeterministicReasoning("analysis", config={
        "rules": [
            {
                "name": "high_scorer",
                "condition": {
                    "type": "field_value",
                    "field": "score",
                    "operator": "gt",
                    "value": 80
                },
                "conclusion": "High performing students identified"
            },
            {
                "name": "sufficient_data",
                "condition": {
                    "type": "count_threshold",
                    "threshold": 2
                },
                "conclusion": "Sufficient data for analysis"
            }
        ],
        "functions": {
            "average_score": {
                "type": "builtin",
                "operation": "sum",
                "field": "score"
            }
        }
    })
    
    # 5. Output
    output = ConsoleOutput("display_results")
    
    # Add components to workflow
    workflow.add_component(ingestion)
    workflow.add_component(filter_processor)
    workflow.add_component(transform_processor)
    workflow.add_component(reasoning)
    workflow.add_component(output)
    
    # Connect components
    workflow.connect_components("data_source", "filter_students")
    workflow.connect_components("filter_students", "transform_names")
    workflow.connect_components("transform_names", "analysis")
    workflow.connect_components("analysis", "display_results")
    
    # Validate workflow
    validation_errors = workflow.validate()
    if validation_errors:
        print("Workflow validation errors:")
        for error in validation_errors:
            print(f"  - {error}")
        return
    
    print(f"Executing workflow: {workflow.name}")
    print(f"Execution order: {workflow.execution_order}")
    
    # Execute workflow
    result = await workflow.execute()
    
    print(f"\nWorkflow Status: {result.status.value}")
    print(f"Execution Time: {result.execution_time:.2f} seconds")
    
    if result.errors:
        print("Errors:")
        for error in result.errors:
            print(f"  - {error}")
    
    # Clean up
    Path(data_file).unlink()


if __name__ == "__main__":
    asyncio.run(run_simple_workflow())