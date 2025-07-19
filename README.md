# Alchemist Workflow System

A flexible, modular workflow system for data processing, reasoning, and analysis. Alchemist combines data ingestion, processing, probabilistic and deterministic reasoning, result aggregation, and output generation into configurable workflows.

## Features

### Core Components
- **Data Ingestion**: Support for files (JSON, CSV, TXT), APIs, and databases
- **Data Processing**: Filtering, transformation, and aggregation capabilities
- **Reasoning Engine**: Both deterministic (rule-based) and probabilistic (LLM-based) reasoning
- **Result Aggregation**: Multiple strategies including voting, consensus, and weighted aggregation
- **Output Handling**: Export to files, console, APIs, and HTML reports

### Key Capabilities
- **Async Execution**: Full async/await support for concurrent processing
- **Configuration-Driven**: YAML/JSON configuration files for workflow definition
- **Modular Architecture**: Easily extensible component system
- **Error Handling**: Comprehensive error tracking and validation
- **Template System**: Pre-built workflow templates for common patterns

## Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/chandinijain/alchemist_new.git
cd alchemist_new

# Install dependencies
pip install -r requirements.txt

# Or install in development mode
pip install -e .
```

### Basic Usage

```python
import asyncio
from alchemist import Workflow
from alchemist.components.ingestion import FileIngestion
from alchemist.components.processing import FilterProcessor
from alchemist.components.reasoning import DeterministicReasoning
from alchemist.components.output import ConsoleOutput

async def simple_workflow():
    # Create workflow
    workflow = Workflow("data_analysis")
    
    # Add components
    ingestion = FileIngestion("data_source", config={
        "source_path": "data.json"
    })
    
    processor = FilterProcessor("filter", config={
        "filter_conditions": {"status": "active"}
    })
    
    reasoning = DeterministicReasoning("analysis", config={
        "rules": [{
            "name": "high_value",
            "condition": {"type": "field_value", "field": "value", "operator": "gt", "value": 100},
            "conclusion": "High value items detected"
        }]
    })
    
    output = ConsoleOutput("results")
    
    # Build workflow
    workflow.add_component(ingestion)
    workflow.add_component(processor)
    workflow.add_component(reasoning)
    workflow.add_component(output)
    
    # Connect components
    workflow.connect_components("data_source", "filter")
    workflow.connect_components("filter", "analysis")
    workflow.connect_components("analysis", "results")
    
    # Execute
    result = await workflow.execute()
    print(f"Workflow completed: {result.status}")

# Run the workflow
asyncio.run(simple_workflow())
```

### Configuration-Based Workflows

```python
from alchemist.config.manager import ConfigManager

# Load workflow from configuration
config_manager = ConfigManager()
workflow_config = config_manager.load_workflow_config("workflow.json")

# Create workflow from template
workflow_config = config_manager.create_workflow_from_template(
    "simple_analysis",
    input_file="data.json",
    output_path="results.html"
)
```

## Component Types

### Data Ingestion
- `FileIngestion`: Read from JSON, CSV, TXT files
- `APIIngestion`: Fetch data from REST APIs
- `DatabaseIngestion`: Query databases (SQL support)

### Data Processing
- `FilterProcessor`: Filter data based on conditions
- `TransformProcessor`: Transform data values and structure
- `AggregationProcessor`: Aggregate data with grouping and functions

### Reasoning
- `DeterministicReasoning`: Rule-based logic with function calls
- `ProbabilisticReasoning`: LLM-powered analysis and insights

### Result Aggregation
- `MergeAggregation`: Combine results from multiple sources
- `VotingAggregation`: Democratic voting on outcomes
- `WeightedAggregation`: Weight-based result combination
- `ConsensusAggregation`: Find consensus across multiple analyses

### Output
- `FileOutput`: Export to JSON, CSV, TXT, HTML
- `ConsoleOutput`: Display results in terminal
- `APIOutput`: Send results to external APIs

## Examples

See the `examples/` directory for complete workflow demonstrations:

- `simple_workflow.py`: Basic data processing pipeline
- `multi_source_workflow.py`: Multi-source analysis with consensus
- `config_based_workflow.py`: Configuration-driven workflow creation

## Configuration

Workflows can be defined using JSON or YAML configuration files:

```json
{
  "name": "Data Analysis Pipeline",
  "description": "Process and analyze sales data",
  "components": [
    {
      "name": "data_source",
      "type": "file_ingestion",
      "config": {
        "source_path": "sales.json"
      }
    },
    {
      "name": "processor",
      "type": "filter_processor",
      "dependencies": ["data_source"],
      "config": {
        "filter_conditions": {
          "amount": {"operator": "gt", "value": 100}
        }
      }
    },
    {
      "name": "analysis",
      "type": "deterministic_reasoning",
      "dependencies": ["processor"],
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
          }
        ]
      }
    },
    {
      "name": "output",
      "type": "file_output",
      "dependencies": ["analysis"],
      "config": {
        "output_format": "html",
        "output_path": "report.html"
      }
    }
  ]
}
```

## Architecture

The Alchemist system is built around several key concepts:

### Workflow
The main orchestrator that manages component execution order, handles dependencies, and coordinates data flow between components.

### Component
Base class for all workflow components. Each component implements:
- `execute()`: Main processing logic
- `validate_config()`: Configuration validation
- Dependency management
- Error handling

### ComponentResult
Standardized result structure containing:
- Execution status
- Output data
- Metadata
- Error information
- Execution time

### Configuration Management
Centralized system for:
- Loading/saving workflow configurations
- Component templates
- Validation rules
- Template-based workflow creation

## Extending Alchemist

### Custom Components

Create custom components by extending the base `Component` class:

```python
from alchemist.core.component import Component, ComponentResult, ComponentStatus
import time

class CustomProcessor(Component):
    def validate_config(self) -> bool:
        return "custom_param" in self.config
    
    async def execute(self, inputs: Dict[str, Any]) -> ComponentResult:
        start_time = time.time()
        
        try:
            # Your custom logic here
            result_data = self.process_data(inputs)
            
            return ComponentResult(
                status=ComponentStatus.COMPLETED,
                data=result_data,
                metadata={"custom_info": "processed"},
                errors=[],
                execution_time=time.time() - start_time
            )
        except Exception as e:
            return ComponentResult(
                status=ComponentStatus.FAILED,
                data=None,
                metadata={},
                errors=[str(e)],
                execution_time=time.time() - start_time
            )
    
    def process_data(self, inputs):
        # Implement your processing logic
        pass
```

### Custom Aggregation Strategies

Extend `ResultAggregation` for custom aggregation logic:

```python
from alchemist.components.aggregation import ResultAggregation

class CustomAggregation(ResultAggregation):
    async def execute(self, inputs: Dict[str, Any]) -> ComponentResult:
        # Implement custom aggregation logic
        pass
```

## Development

### Running Tests

```bash
# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run with coverage
pytest --cov=alchemist
```

### Code Formatting

```bash
# Format code
black alchemist/

# Check style
flake8 alchemist/

# Type checking
mypy alchemist/
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request

## License

MIT License - see LICENSE file for details.

## Roadmap

- [ ] Web UI for workflow design and monitoring
- [ ] Integration with popular ML frameworks (scikit-learn, PyTorch)
- [ ] Real-time streaming data support
- [ ] Distributed execution capabilities
- [ ] Advanced visualization and reporting
- [ ] Plugin system for third-party extensions