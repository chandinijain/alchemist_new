"""Configuration management system."""

import json
import yaml
from typing import Any, Dict, List, Optional, Union
from pathlib import Path
from dataclasses import dataclass, field


@dataclass
class ComponentConfig:
    """Configuration for a single component."""
    name: str
    type: str
    class_name: str
    config: Dict[str, Any] = field(default_factory=dict)
    dependencies: List[str] = field(default_factory=list)
    enabled: bool = True


@dataclass
class WorkflowConfig:
    """Configuration for an entire workflow."""
    name: str
    description: str = ""
    components: List[ComponentConfig] = field(default_factory=list)
    global_config: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


class ConfigManager:
    """Manages workflow and component configurations."""
    
    def __init__(self):
        self.component_templates = self._load_component_templates()
        self.workflow_templates = self._load_workflow_templates()
    
    def load_workflow_config(self, config_path: Union[str, Path]) -> WorkflowConfig:
        """Load workflow configuration from file."""
        config_path = Path(config_path)
        
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        if config_path.suffix.lower() == '.json':
            with open(config_path, 'r', encoding='utf-8') as f:
                config_data = json.load(f)
        elif config_path.suffix.lower() in ['.yml', '.yaml']:
            with open(config_path, 'r', encoding='utf-8') as f:
                config_data = yaml.safe_load(f)
        else:
            raise ValueError(f"Unsupported configuration file format: {config_path.suffix}")
        
        return self._parse_workflow_config(config_data)
    
    def save_workflow_config(self, workflow_config: WorkflowConfig, config_path: Union[str, Path]) -> None:
        """Save workflow configuration to file."""
        config_path = Path(config_path)
        config_path.parent.mkdir(parents=True, exist_ok=True)
        
        config_data = self._serialize_workflow_config(workflow_config)
        
        if config_path.suffix.lower() == '.json':
            with open(config_path, 'w', encoding='utf-8') as f:
                json.dump(config_data, f, indent=2, ensure_ascii=False)
        elif config_path.suffix.lower() in ['.yml', '.yaml']:
            with open(config_path, 'w', encoding='utf-8') as f:
                yaml.dump(config_data, f, default_flow_style=False, allow_unicode=True)
        else:
            raise ValueError(f"Unsupported configuration file format: {config_path.suffix}")
    
    def create_workflow_from_template(self, template_name: str, **kwargs) -> WorkflowConfig:
        """Create a workflow configuration from a template."""
        if template_name not in self.workflow_templates:
            raise ValueError(f"Unknown workflow template: {template_name}")
        
        template = self.workflow_templates[template_name].copy()
        
        # Replace template variables
        for key, value in kwargs.items():
            template = self._replace_template_variables(template, key, value)
        
        return self._parse_workflow_config(template)
    
    def get_component_config_template(self, component_type: str) -> Dict[str, Any]:
        """Get configuration template for a component type."""
        if component_type not in self.component_templates:
            raise ValueError(f"Unknown component type: {component_type}")
        
        return self.component_templates[component_type].copy()
    
    def validate_workflow_config(self, workflow_config: WorkflowConfig) -> List[str]:
        """Validate workflow configuration and return list of errors."""
        errors = []
        
        # Check component dependencies
        component_names = {comp.name for comp in workflow_config.components}
        
        for component in workflow_config.components:
            for dependency in component.dependencies:
                if dependency not in component_names:
                    errors.append(f"Component '{component.name}' depends on missing component '{dependency}'")
        
        # Validate individual component configurations
        for component in workflow_config.components:
            component_errors = self._validate_component_config(component)
            errors.extend(component_errors)
        
        return errors
    
    def _parse_workflow_config(self, config_data: Dict[str, Any]) -> WorkflowConfig:
        """Parse workflow configuration from dictionary."""
        components = []
        
        for comp_data in config_data.get("components", []):
            component = ComponentConfig(
                name=comp_data["name"],
                type=comp_data["type"],
                class_name=comp_data.get("class_name", self._get_default_class_name(comp_data["type"])),
                config=comp_data.get("config", {}),
                dependencies=comp_data.get("dependencies", []),
                enabled=comp_data.get("enabled", True)
            )
            components.append(component)
        
        return WorkflowConfig(
            name=config_data["name"],
            description=config_data.get("description", ""),
            components=components,
            global_config=config_data.get("global_config", {}),
            metadata=config_data.get("metadata", {})
        )
    
    def _serialize_workflow_config(self, workflow_config: WorkflowConfig) -> Dict[str, Any]:
        """Serialize workflow configuration to dictionary."""
        components_data = []
        
        for component in workflow_config.components:
            comp_data = {
                "name": component.name,
                "type": component.type,
                "class_name": component.class_name,
                "config": component.config,
                "dependencies": component.dependencies,
                "enabled": component.enabled
            }
            components_data.append(comp_data)
        
        return {
            "name": workflow_config.name,
            "description": workflow_config.description,
            "components": components_data,
            "global_config": workflow_config.global_config,
            "metadata": workflow_config.metadata
        }
    
    def _validate_component_config(self, component: ComponentConfig) -> List[str]:
        """Validate individual component configuration."""
        errors = []
        
        # Check if component type exists
        if component.type not in self.component_templates:
            errors.append(f"Unknown component type: {component.type}")
            return errors
        
        # Validate required configuration fields
        template = self.component_templates[component.type]
        required_fields = template.get("required_config", [])
        
        for field in required_fields:
            if field not in component.config:
                errors.append(f"Component '{component.name}' missing required config field: {field}")
        
        # Validate configuration field types
        field_types = template.get("config_types", {})
        for field, expected_type in field_types.items():
            if field in component.config:
                actual_value = component.config[field]
                if not isinstance(actual_value, expected_type):
                    errors.append(f"Component '{component.name}' field '{field}' should be {expected_type.__name__}, got {type(actual_value).__name__}")
        
        return errors
    
    def _get_default_class_name(self, component_type: str) -> str:
        """Get default class name for component type."""
        class_mapping = {
            "file_ingestion": "FileIngestion",
            "api_ingestion": "APIIngestion",
            "database_ingestion": "DatabaseIngestion",
            "filter_processor": "FilterProcessor",
            "transform_processor": "TransformProcessor",
            "aggregation_processor": "AggregationProcessor",
            "deterministic_reasoning": "DeterministicReasoning",
            "probabilistic_reasoning": "ProbabilisticReasoning",
            "merge_aggregation": "MergeAggregation",
            "voting_aggregation": "VotingAggregation",
            "weighted_aggregation": "WeightedAggregation",
            "consensus_aggregation": "ConsensusAggregation",
            "file_output": "FileOutput",
            "console_output": "ConsoleOutput",
            "api_output": "APIOutput"
        }
        return class_mapping.get(component_type, component_type)
    
    def _load_component_templates(self) -> Dict[str, Any]:
        """Load component configuration templates."""
        return {
            "file_ingestion": {
                "required_config": ["source_path"],
                "config_types": {
                    "source_path": str,
                    "source_type": str
                },
                "default_config": {
                    "source_type": "file"
                }
            },
            "api_ingestion": {
                "required_config": ["url"],
                "config_types": {
                    "url": str,
                    "method": str,
                    "headers": dict,
                    "params": dict
                },
                "default_config": {
                    "method": "GET",
                    "headers": {},
                    "params": {}
                }
            },
            "database_ingestion": {
                "required_config": ["connection_string", "query"],
                "config_types": {
                    "connection_string": str,
                    "query": str
                }
            },
            "filter_processor": {
                "required_config": [],
                "config_types": {
                    "filter_conditions": dict
                },
                "default_config": {
                    "filter_conditions": {}
                }
            },
            "transform_processor": {
                "required_config": [],
                "config_types": {
                    "transformations": dict
                },
                "default_config": {
                    "transformations": {}
                }
            },
            "aggregation_processor": {
                "required_config": [],
                "config_types": {
                    "aggregation_functions": dict,
                    "group_by": str
                }
            },
            "deterministic_reasoning": {
                "required_config": [],
                "config_types": {
                    "rules": list,
                    "functions": dict
                },
                "default_config": {
                    "rules": [],
                    "functions": {}
                }
            },
            "probabilistic_reasoning": {
                "required_config": [],
                "config_types": {
                    "model_config": dict,
                    "prompts": dict,
                    "temperature": (int, float)
                },
                "default_config": {
                    "model_config": {},
                    "prompts": {},
                    "temperature": 0.7
                }
            },
            "merge_aggregation": {
                "required_config": [],
                "config_types": {
                    "aggregation_strategy": str,
                    "weighting": dict
                },
                "default_config": {
                    "aggregation_strategy": "merge",
                    "weighting": {}
                }
            },
            "voting_aggregation": {
                "required_config": [],
                "config_types": {
                    "voting_method": str,
                    "confidence_threshold": (int, float)
                },
                "default_config": {
                    "voting_method": "majority",
                    "confidence_threshold": 0.5
                }
            },
            "weighted_aggregation": {
                "required_config": [],
                "config_types": {
                    "weighting": dict
                },
                "default_config": {
                    "weighting": {}
                }
            },
            "consensus_aggregation": {
                "required_config": [],
                "config_types": {
                    "consensus_threshold": (int, float),
                    "min_agreement": int
                },
                "default_config": {
                    "consensus_threshold": 0.7,
                    "min_agreement": 2
                }
            },
            "file_output": {
                "required_config": [],
                "config_types": {
                    "output_format": str,
                    "output_path": str
                },
                "default_config": {
                    "output_format": "json"
                }
            },
            "console_output": {
                "required_config": [],
                "config_types": {},
                "default_config": {}
            },
            "api_output": {
                "required_config": ["api_endpoint"],
                "config_types": {
                    "api_endpoint": str,
                    "api_method": str,
                    "api_headers": dict
                },
                "default_config": {
                    "api_method": "POST",
                    "api_headers": {}
                }
            }
        }
    
    def _load_workflow_templates(self) -> Dict[str, Any]:
        """Load workflow configuration templates."""
        return {
            "simple_analysis": {
                "name": "Simple Data Analysis",
                "description": "Basic data ingestion, processing, and output workflow",
                "components": [
                    {
                        "name": "data_source",
                        "type": "file_ingestion",
                        "config": {
                            "source_path": "{input_file}",
                            "source_type": "file"
                        }
                    },
                    {
                        "name": "processor",
                        "type": "filter_processor",
                        "dependencies": ["data_source"],
                        "config": {
                            "filter_conditions": {}
                        }
                    },
                    {
                        "name": "output",
                        "type": "console_output",
                        "dependencies": ["processor"]
                    }
                ]
            },
            "ml_pipeline": {
                "name": "Machine Learning Pipeline",
                "description": "Data ingestion, processing, ML reasoning, and output",
                "components": [
                    {
                        "name": "data_ingestion",
                        "type": "file_ingestion",
                        "config": {
                            "source_path": "{dataset_path}",
                            "source_type": "file"
                        }
                    },
                    {
                        "name": "data_processing",
                        "type": "transform_processor",
                        "dependencies": ["data_ingestion"],
                        "config": {
                            "transformations": {
                                "text": "lowercase"
                            }
                        }
                    },
                    {
                        "name": "ml_reasoning",
                        "type": "probabilistic_reasoning",
                        "dependencies": ["data_processing"],
                        "config": {
                            "model_config": {
                                "model_name": "{model_name}",
                                "api_key": "{api_key}"
                            },
                            "prompts": {
                                "analysis": {
                                    "template": "Analyze this data: {data}"
                                }
                            },
                            "temperature": 0.3
                        }
                    },
                    {
                        "name": "results_output",
                        "type": "file_output",
                        "dependencies": ["ml_reasoning"],
                        "config": {
                            "output_format": "json",
                            "output_path": "{output_path}"
                        }
                    }
                ]
            },
            "multi_source_analysis": {
                "name": "Multi-Source Analysis",
                "description": "Combine multiple data sources with consensus reasoning",
                "components": [
                    {
                        "name": "source1",
                        "type": "file_ingestion",
                        "config": {
                            "source_path": "{source1_path}",
                            "source_type": "file"
                        }
                    },
                    {
                        "name": "source2",
                        "type": "api_ingestion",
                        "config": {
                            "url": "{api_url}",
                            "method": "GET"
                        }
                    },
                    {
                        "name": "reasoning1",
                        "type": "deterministic_reasoning",
                        "dependencies": ["source1"],
                        "config": {
                            "rules": [
                                {
                                    "name": "threshold_check",
                                    "condition": {
                                        "type": "count_threshold",
                                        "threshold": 10
                                    },
                                    "conclusion": "Sufficient data for analysis"
                                }
                            ]
                        }
                    },
                    {
                        "name": "reasoning2",
                        "type": "probabilistic_reasoning",
                        "dependencies": ["source2"],
                        "config": {
                            "prompts": {
                                "classification": {
                                    "template": "Classify this data: {data}"
                                }
                            }
                        }
                    },
                    {
                        "name": "consensus",
                        "type": "consensus_aggregation",
                        "dependencies": ["reasoning1", "reasoning2"],
                        "config": {
                            "consensus_threshold": 0.7,
                            "min_agreement": 1
                        }
                    },
                    {
                        "name": "final_output",
                        "type": "file_output",
                        "dependencies": ["consensus"],
                        "config": {
                            "output_format": "html",
                            "output_path": "{output_file}"
                        }
                    }
                ]
            }
        }
    
    def _replace_template_variables(self, template: Any, key: str, value: Any) -> Any:
        """Replace template variables recursively."""
        if isinstance(template, dict):
            return {k: self._replace_template_variables(v, key, value) for k, v in template.items()}
        elif isinstance(template, list):
            return [self._replace_template_variables(item, key, value) for item in template]
        elif isinstance(template, str):
            return template.replace(f"{{{key}}}", str(value))
        else:
            return template