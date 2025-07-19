"""Multi-source workflow example with consensus aggregation."""

import asyncio
import json
from pathlib import Path

from alchemist import Workflow
from alchemist.components.ingestion import FileIngestion, APIIngestion
from alchemist.components.reasoning import DeterministicReasoning, ProbabilisticReasoning
from alchemist.components.aggregation import ConsensusAggregation
from alchemist.components.output import FileOutput


async def create_test_data():
    """Create test data files."""
    # Create first data source
    data1 = [
        {"sensor_id": "A1", "temperature": 22.5, "humidity": 65, "status": "normal"},
        {"sensor_id": "A2", "temperature": 25.1, "humidity": 58, "status": "normal"},
        {"sensor_id": "A3", "temperature": 35.2, "humidity": 45, "status": "warning"}
    ]
    
    # Create second data source  
    data2 = [
        {"device": "B1", "reading": 23.0, "quality": "good"},
        {"device": "B2", "reading": 24.8, "quality": "good"},
        {"device": "B3", "reading": 36.1, "quality": "poor"}
    ]
    
    path1 = Path("sensor_data.json")
    path2 = Path("device_data.json")
    
    with open(path1, 'w') as f:
        json.dump(data1, f, indent=2)
    
    with open(path2, 'w') as f:
        json.dump(data2, f, indent=2)
    
    return str(path1), str(path2)


async def run_multi_source_workflow():
    """Run workflow with multiple data sources and consensus aggregation."""
    
    # Create test data
    sensor_file, device_file = await create_test_data()
    
    # Create workflow
    workflow = Workflow("multi_source_analysis", config={
        "description": "Multi-source environmental data analysis with consensus"
    })
    
    # Data Sources
    sensor_ingestion = FileIngestion("sensor_data", config={
        "source_path": sensor_file,
        "source_type": "file"
    })
    
    device_ingestion = FileIngestion("device_data", config={
        "source_path": device_file,
        "source_type": "file"
    })
    
    # Alternative: API ingestion (commented out for demo)
    # api_ingestion = APIIngestion("api_data", config={
    #     "url": "https://api.example.com/environmental-data",
    #     "method": "GET",
    #     "headers": {"Authorization": "Bearer token"}
    # })
    
    # Reasoning Components
    sensor_reasoning = DeterministicReasoning("sensor_analysis", config={
        "rules": [
            {
                "name": "temperature_alert",
                "condition": {
                    "type": "field_value",
                    "field": "temperature",
                    "operator": "gt",
                    "value": 30
                },
                "conclusion": "High temperature detected"
            },
            {
                "name": "humidity_check",
                "condition": {
                    "type": "field_value",
                    "field": "humidity",
                    "operator": "lt",
                    "value": 50
                },
                "conclusion": "Low humidity levels detected"
            },
            {
                "name": "status_warning",
                "condition": {
                    "type": "field_value",
                    "field": "status",
                    "operator": "eq",
                    "value": "warning"
                },
                "conclusion": "Sensor warning status detected"
            }
        ],
        "functions": {
            "avg_temperature": {
                "type": "builtin",
                "operation": "sum",
                "field": "temperature"
            },
            "sensor_count": {
                "type": "builtin",
                "operation": "count"
            }
        }
    })
    
    device_reasoning = ProbabilisticReasoning("device_analysis", config={
        "model_config": {
            "model_name": "gpt-3.5-turbo",
            "max_tokens": 150
        },
        "prompts": {
            "quality_assessment": {
                "template": "Analyze the device reading quality and provide insights: {data}"
            },
            "anomaly_detection": {
                "template": "Identify any anomalies in this device data: {data}"
            }
        },
        "temperature": 0.3
    })
    
    # Consensus Aggregation
    consensus = ConsensusAggregation("consensus_analysis", config={
        "consensus_threshold": 0.6,
        "min_agreement": 1
    })
    
    # Output
    output = FileOutput("results", config={
        "output_format": "html",
        "output_path": "multi_source_results.html"
    })
    
    # Add components to workflow
    workflow.add_component(sensor_ingestion)
    workflow.add_component(device_ingestion)
    workflow.add_component(sensor_reasoning)
    workflow.add_component(device_reasoning)
    workflow.add_component(consensus)
    workflow.add_component(output)
    
    # Connect components
    workflow.connect_components("sensor_data", "sensor_analysis")
    workflow.connect_components("device_data", "device_analysis")
    workflow.connect_components("sensor_analysis", "consensus_analysis")
    workflow.connect_components("device_analysis", "consensus_analysis")
    workflow.connect_components("consensus_analysis", "results")
    
    # Validate and execute
    validation_errors = workflow.validate()
    if validation_errors:
        print("Workflow validation errors:")
        for error in validation_errors:
            print(f"  - {error}")
        return
    
    print(f"Executing multi-source workflow: {workflow.name}")
    print(f"Component execution order: {workflow.execution_order}")
    
    result = await workflow.execute()
    
    print(f"\nWorkflow Status: {result.status.value}")
    print(f"Total Execution Time: {result.execution_time:.2f} seconds")
    
    # Show component results
    print("\nComponent Results Summary:")
    for comp_name, comp_result in result.component_results.items():
        print(f"  {comp_name}: {comp_result.status.value} ({comp_result.execution_time:.2f}s)")
        if comp_result.errors:
            print(f"    Errors: {comp_result.errors}")
    
    if result.errors:
        print(f"\nWorkflow Errors: {result.errors}")
    else:
        print(f"\nResults saved to: multi_source_results.html")
    
    # Clean up
    Path(sensor_file).unlink()
    Path(device_file).unlink()


if __name__ == "__main__":
    asyncio.run(run_multi_source_workflow())