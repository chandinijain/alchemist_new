"""Configuration-based workflow example using ConfigManager."""

import asyncio
import json
from pathlib import Path

from alchemist import Workflow
from alchemist.config.manager import ConfigManager
from alchemist.components.ingestion import FileIngestion, APIIngestion, DatabaseIngestion
from alchemist.components.processing import FilterProcessor, TransformProcessor, AggregationProcessor
from alchemist.components.reasoning import DeterministicReasoning, ProbabilisticReasoning
from alchemist.components.aggregation import MergeAggregation, VotingAggregation, WeightedAggregation, ConsensusAggregation
from alchemist.components.output import FileOutput, ConsoleOutput, APIOutput


# Component class mapping
COMPONENT_CLASSES = {
    "FileIngestion": FileIngestion,
    "APIIngestion": APIIngestion,
    "DatabaseIngestion": DatabaseIngestion,
    "FilterProcessor": FilterProcessor,
    "TransformProcessor": TransformProcessor,
    "AggregationProcessor": AggregationProcessor,
    "DeterministicReasoning": DeterministicReasoning,
    "ProbabilisticReasoning": ProbabilisticReasoning,
    "MergeAggregation": MergeAggregation,
    "VotingAggregation": VotingAggregation,
    "WeightedAggregation": WeightedAggregation,
    "ConsensusAggregation": ConsensusAggregation,
    "FileOutput": FileOutput,
    "ConsoleOutput": ConsoleOutput,
    "APIOutput": APIOutput
}


async def create_sample_config():
    """Create a sample workflow configuration file."""
    config = {
        "name": "Sales Data Analysis Pipeline",
        "description": "Analyze sales data from multiple sources with ML insights",
        "global_config": {
            "log_level": "INFO",
            "max_retries": 3
        },
        "metadata": {
            "version": "1.0",
            "author": "Data Science Team",
            "created": "2024-01-01"
        },
        "components": [
            {
                "name": "sales_data",
                "type": "file_ingestion",
                "class_name": "FileIngestion",
                "config": {
                    "source_path": "sales_data.json",
                    "source_type": "file"
                },
                "enabled": True
            },
            {
                "name": "customer_data",
                "type": "file_ingestion",
                "class_name": "FileIngestion",
                "config": {
                    "source_path": "customer_data.json",
                    "source_type": "file"
                },
                "enabled": True
            },
            {
                "name": "data_filter",
                "type": "filter_processor",
                "class_name": "FilterProcessor",
                "dependencies": ["sales_data"],
                "config": {
                    "filter_conditions": {
                        "amount": {
                            "operator": "gt",
                            "value": 100
                        },
                        "status": "completed"
                    }
                }
            },
            {
                "name": "data_transform",
                "type": "transform_processor",
                "class_name": "TransformProcessor",
                "dependencies": ["data_filter"],
                "config": {
                    "transformations": {
                        "customer_name": "uppercase",
                        "product": "lowercase"
                    }
                }
            },
            {
                "name": "rule_analysis",
                "type": "deterministic_reasoning",
                "class_name": "DeterministicReasoning",
                "dependencies": ["data_transform"],
                "config": {
                    "rules": [
                        {
                            "name": "high_value_sales",
                            "condition": {
                                "type": "field_value",
                                "field": "amount",
                                "operator": "gt",
                                "value": 1000
                            },
                            "conclusion": "High value sales detected"
                        },
                        {
                            "name": "bulk_orders",
                            "condition": {
                                "type": "count_threshold",
                                "threshold": 5
                            },
                            "conclusion": "Bulk order pattern identified"
                        }
                    ],
                    "functions": {
                        "total_revenue": {
                            "type": "builtin",
                            "operation": "sum",
                            "field": "amount"
                        }
                    }
                }
            },
            {
                "name": "ml_insights",
                "type": "probabilistic_reasoning",
                "class_name": "ProbabilisticReasoning",
                "dependencies": ["customer_data"],
                "config": {
                    "model_config": {
                        "model_name": "gpt-4",
                        "api_key": "your-api-key"
                    },
                    "prompts": {
                        "customer_segmentation": {
                            "template": "Analyze customer data and provide segmentation insights: {data}"
                        },
                        "trend_analysis": {
                            "template": "Identify trends and patterns in this customer data: {data}"
                        }
                    },
                    "temperature": 0.4
                }
            },
            {
                "name": "weighted_consensus",
                "type": "weighted_aggregation",
                "class_name": "WeightedAggregation",
                "dependencies": ["rule_analysis", "ml_insights"],
                "config": {
                    "weighting": {
                        "rule_analysis": 0.6,
                        "ml_insights": 0.4
                    }
                }
            },
            {
                "name": "final_output",
                "type": "file_output",
                "class_name": "FileOutput",
                "dependencies": ["weighted_consensus"],
                "config": {
                    "output_format": "html",
                    "output_path": "sales_analysis_report.html"
                }
            }
        ]
    }
    
    config_path = Path("workflow_config.json")
    with open(config_path, 'w') as f:
        json.dump(config, f, indent=2)
    
    return str(config_path)


async def create_sample_data():
    """Create sample data files for the workflow."""
    sales_data = [
        {"id": 1, "customer_id": "C001", "product": "Laptop", "amount": 1200, "status": "completed"},
        {"id": 2, "customer_id": "C002", "product": "Mouse", "amount": 25, "status": "completed"},
        {"id": 3, "customer_id": "C003", "product": "Monitor", "amount": 800, "status": "completed"},
        {"id": 4, "customer_id": "C001", "product": "Keyboard", "amount": 150, "status": "pending"},
        {"id": 5, "customer_id": "C004", "product": "Tablet", "amount": 600, "status": "completed"}
    ]
    
    customer_data = [
        {"customer_id": "C001", "name": "john doe", "segment": "enterprise", "region": "north"},
        {"customer_id": "C002", "name": "jane smith", "segment": "small_business", "region": "south"},
        {"customer_id": "C003", "name": "bob wilson", "segment": "enterprise", "region": "east"},
        {"customer_id": "C004", "name": "alice brown", "segment": "consumer", "region": "west"}
    ]
    
    with open("sales_data.json", 'w') as f:
        json.dump(sales_data, f, indent=2)
    
    with open("customer_data.json", 'w') as f:
        json.dump(customer_data, f, indent=2)
    
    return "sales_data.json", "customer_data.json"


def create_component_from_config(component_config):
    """Create a component instance from configuration."""
    class_name = component_config.class_name
    
    if class_name not in COMPONENT_CLASSES:
        raise ValueError(f"Unknown component class: {class_name}")
    
    component_class = COMPONENT_CLASSES[class_name]
    return component_class(component_config.name, component_config.config)


async def run_config_based_workflow():
    """Run a workflow based on configuration file."""
    
    # Create sample data and config
    sales_file, customer_file = await create_sample_data()
    config_file = await create_sample_config()
    
    # Load configuration
    config_manager = ConfigManager()
    workflow_config = config_manager.load_workflow_config(config_file)
    
    print(f"Loaded workflow: {workflow_config.name}")
    print(f"Description: {workflow_config.description}")
    
    # Validate configuration
    validation_errors = config_manager.validate_workflow_config(workflow_config)
    if validation_errors:
        print("Configuration validation errors:")
        for error in validation_errors:
            print(f"  - {error}")
        return
    
    # Create workflow
    workflow = Workflow(
        workflow_config.name,
        config=workflow_config.global_config
    )
    
    # Create and add components
    for comp_config in workflow_config.components:
        if not comp_config.enabled:
            continue
        
        component = create_component_from_config(comp_config)
        workflow.add_component(component)
        
        # Add dependencies
        for dependency in comp_config.dependencies:
            workflow.connect_components(dependency, comp_config.name)
    
    # Validate workflow
    workflow_errors = workflow.validate()
    if workflow_errors:
        print("Workflow validation errors:")
        for error in workflow_errors:
            print(f"  - {error}")
        return
    
    print(f"\\nExecuting workflow with {len(workflow.components)} components")
    print(f"Execution order: {workflow.execution_order}")
    
    # Execute workflow
    result = await workflow.execute()
    
    print(f"\\nWorkflow completed with status: {result.status.value}")
    print(f"Total execution time: {result.execution_time:.2f} seconds")
    
    # Display component results
    print("\\nComponent Results:")
    for comp_name, comp_result in result.component_results.items():
        status_icon = "✅" if comp_result.status.value == "completed" else "❌"
        print(f"  {status_icon} {comp_name}: {comp_result.status.value} ({comp_result.execution_time:.2f}s)")
        
        if comp_result.errors:
            for error in comp_result.errors:
                print(f"    ❌ {error}")
    
    if result.errors:
        print(f"\\nWorkflow errors: {result.errors}")
    else:
        print("\\n🎉 Workflow completed successfully!")
        print("📄 Report saved to: sales_analysis_report.html")
    
    # Show configuration templates
    print("\\n" + "="*50)
    print("Available workflow templates:")
    for template_name in config_manager.workflow_templates.keys():
        print(f"  - {template_name}")
    
    # Clean up
    Path(sales_file).unlink()
    Path(customer_file).unlink()
    Path(config_file).unlink()


async def demonstrate_templates():
    """Demonstrate workflow creation from templates."""
    print("\\n" + "="*50)
    print("TEMPLATE DEMONSTRATION")
    print("="*50)
    
    config_manager = ConfigManager()
    
    # Create workflow from template
    workflow_config = config_manager.create_workflow_from_template(
        "simple_analysis",
        input_file="demo_data.json"
    )
    
    print(f"Created workflow from template: {workflow_config.name}")
    print(f"Components: {[comp.name for comp in workflow_config.components]}")
    
    # Save the template-based config
    config_manager.save_workflow_config(workflow_config, "template_workflow.json")
    print("Saved template-based workflow to: template_workflow.json")
    
    # Clean up
    Path("template_workflow.json").unlink()


if __name__ == "__main__":
    asyncio.run(run_config_based_workflow())
    asyncio.run(demonstrate_templates())