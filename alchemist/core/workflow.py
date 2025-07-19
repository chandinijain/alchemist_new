"""Workflow orchestration system."""

import asyncio
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from enum import Enum
import time
import logging

from .component import Component, ComponentResult, ComponentStatus


class WorkflowStatus(Enum):
    """Status of workflow execution."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class WorkflowResult:
    """Result of workflow execution."""
    status: WorkflowStatus
    component_results: Dict[str, ComponentResult] = field(default_factory=dict)
    execution_time: float = 0.0
    errors: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class Workflow:
    """Orchestrates execution of workflow components."""
    
    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None):
        self.name = name
        self.config = config or {}
        self.components: Dict[str, Component] = {}
        self.execution_order: List[str] = []
        self.status = WorkflowStatus.PENDING
        self.logger = logging.getLogger(f"alchemist.workflow.{name}")
        
    def add_component(self, component: Component) -> None:
        """Add a component to the workflow."""
        if component.name in self.components:
            raise ValueError(f"Component '{component.name}' already exists in workflow")
        
        self.components[component.name] = component
        self._update_execution_order()
    
    def remove_component(self, name: str) -> None:
        """Remove a component from the workflow."""
        if name not in self.components:
            raise ValueError(f"Component '{name}' not found in workflow")
        
        del self.components[name]
        self._update_execution_order()
    
    def connect_components(self, source: str, target: str) -> None:
        """Connect two components (source -> target)."""
        if source not in self.components:
            raise ValueError(f"Source component '{source}' not found")
        if target not in self.components:
            raise ValueError(f"Target component '{target}' not found")
        
        self.components[target].add_dependency(source)
        self.components[source].add_output(target)
        self._update_execution_order()
    
    def _update_execution_order(self) -> None:
        """Update the execution order based on dependencies using topological sort."""
        # Simple topological sort implementation
        visited = set()
        temp_visited = set()
        order = []
        
        def visit(node: str) -> None:
            if node in temp_visited:
                raise ValueError(f"Circular dependency detected involving '{node}'")
            if node in visited:
                return
            
            temp_visited.add(node)
            for dependency in self.components[node].get_dependencies():
                if dependency in self.components:
                    visit(dependency)
            temp_visited.remove(node)
            visited.add(node)
            order.append(node)
        
        for component_name in self.components:
            if component_name not in visited:
                visit(component_name)
        
        self.execution_order = order
    
    async def execute(self, initial_inputs: Optional[Dict[str, Any]] = None) -> WorkflowResult:
        """Execute the workflow."""
        start_time = time.time()
        self.status = WorkflowStatus.RUNNING
        self.logger.info(f"Starting workflow '{self.name}' execution")
        
        result = WorkflowResult(status=WorkflowStatus.RUNNING)
        component_outputs = initial_inputs or {}
        
        try:
            for component_name in self.execution_order:
                component = self.components[component_name]
                
                self.logger.info(f"Executing component '{component_name}'")
                component.status = ComponentStatus.RUNNING
                
                # Prepare inputs for this component
                component_inputs = self._prepare_component_inputs(
                    component, component_outputs
                )
                
                # Execute component
                component_result = await component.execute(component_inputs)
                result.component_results[component_name] = component_result
                
                if component_result.status == ComponentStatus.FAILED:
                    self.logger.error(f"Component '{component_name}' failed")
                    result.errors.extend(component_result.errors)
                    self.status = WorkflowStatus.FAILED
                    result.status = WorkflowStatus.FAILED
                    break
                
                # Store outputs for next components
                component_outputs[component_name] = component_result.data
                
                self.logger.info(f"Component '{component_name}' completed successfully")
            
            if self.status == WorkflowStatus.RUNNING:
                self.status = WorkflowStatus.COMPLETED
                result.status = WorkflowStatus.COMPLETED
                self.logger.info(f"Workflow '{self.name}' completed successfully")
        
        except Exception as e:
            self.logger.error(f"Workflow execution failed: {str(e)}")
            self.status = WorkflowStatus.FAILED
            result.status = WorkflowStatus.FAILED
            result.errors.append(str(e))
        
        result.execution_time = time.time() - start_time
        return result
    
    def _prepare_component_inputs(self, component: Component, available_outputs: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare inputs for a component based on its dependencies."""
        inputs = {}
        
        for dependency in component.get_dependencies():
            if dependency in available_outputs:
                inputs[dependency] = available_outputs[dependency]
        
        return inputs
    
    def validate(self) -> List[str]:
        """Validate the workflow configuration."""
        errors = []
        
        # Check for missing dependencies
        for component_name, component in self.components.items():
            for dependency in component.get_dependencies():
                if dependency not in self.components:
                    errors.append(f"Component '{component_name}' depends on missing component '{dependency}'")
        
        # Validate individual components
        for component_name, component in self.components.items():
            if not component.validate_config():
                errors.append(f"Component '{component_name}' has invalid configuration")
        
        return errors
    
    def get_component_graph(self) -> Dict[str, List[str]]:
        """Get the component dependency graph."""
        graph = {}
        for name, component in self.components.items():
            graph[name] = component.get_dependencies()
        return graph
    
    def __str__(self) -> str:
        return f"Workflow(name='{self.name}', components={len(self.components)}, status={self.status.value})"