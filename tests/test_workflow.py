"""Tests for the Workflow orchestration system."""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from typing import Dict, Any

from alchemist.core.workflow import Workflow, WorkflowStatus
from alchemist.core.component import Component, ComponentResult, ComponentStatus


class MockComponent(Component):
    """Mock component for testing."""
    
    def __init__(self, name: str, config: Dict[str, Any] = None, should_fail: bool = False):
        super().__init__(name, config)
        self.should_fail = should_fail
        self.execute_called = False
        
    def validate_config(self) -> bool:
        return True
    
    async def execute(self, inputs: Dict[str, Any]) -> ComponentResult:
        self.execute_called = True
        
        if self.should_fail:
            return ComponentResult(
                status=ComponentStatus.FAILED,
                data=None,
                metadata={},
                errors=["Mock component failure"],
                execution_time=0.1
            )
        
        return ComponentResult(
            status=ComponentStatus.COMPLETED,
            data={"result": f"processed by {self.name}", "inputs": inputs},
            metadata={"component": self.name},
            errors=[],
            execution_time=0.1
        )


class TestWorkflow:
    """Test cases for Workflow class."""
    
    def test_workflow_creation(self):
        """Test basic workflow creation."""
        workflow = Workflow("test_workflow")
        
        assert workflow.name == "test_workflow"
        assert workflow.status == WorkflowStatus.PENDING
        assert len(workflow.components) == 0
        assert len(workflow.execution_order) == 0
    
    def test_add_component(self):
        """Test adding components to workflow."""
        workflow = Workflow("test_workflow")
        component = MockComponent("test_component")
        
        workflow.add_component(component)
        
        assert "test_component" in workflow.components
        assert workflow.components["test_component"] == component
        assert workflow.execution_order == ["test_component"]
    
    def test_add_duplicate_component(self):
        """Test adding duplicate component raises error."""
        workflow = Workflow("test_workflow")
        component1 = MockComponent("test_component")
        component2 = MockComponent("test_component")
        
        workflow.add_component(component1)
        
        with pytest.raises(ValueError, match="Component 'test_component' already exists"):
            workflow.add_component(component2)
    
    def test_remove_component(self):
        """Test removing components from workflow."""
        workflow = Workflow("test_workflow")
        component = MockComponent("test_component")
        
        workflow.add_component(component)
        workflow.remove_component("test_component")
        
        assert "test_component" not in workflow.components
        assert len(workflow.execution_order) == 0
    
    def test_remove_nonexistent_component(self):
        """Test removing non-existent component raises error."""
        workflow = Workflow("test_workflow")
        
        with pytest.raises(ValueError, match="Component 'nonexistent' not found"):
            workflow.remove_component("nonexistent")
    
    def test_connect_components(self):
        """Test connecting components with dependencies."""
        workflow = Workflow("test_workflow")
        comp1 = MockComponent("component1")
        comp2 = MockComponent("component2")
        
        workflow.add_component(comp1)
        workflow.add_component(comp2)
        workflow.connect_components("component1", "component2")
        
        assert "component1" in comp2.get_dependencies()
        assert "component2" in comp1.get_outputs()
        assert workflow.execution_order == ["component1", "component2"]
    
    def test_connect_nonexistent_components(self):
        """Test connecting non-existent components raises error."""
        workflow = Workflow("test_workflow")
        
        with pytest.raises(ValueError, match="Source component 'source' not found"):
            workflow.connect_components("source", "target")
    
    def test_circular_dependency_detection(self):
        """Test circular dependency detection."""
        workflow = Workflow("test_workflow")
        comp1 = MockComponent("component1")
        comp2 = MockComponent("component2")
        comp3 = MockComponent("component3")
        
        workflow.add_component(comp1)
        workflow.add_component(comp2)
        workflow.add_component(comp3)
        
        workflow.connect_components("component1", "component2")
        workflow.connect_components("component2", "component3")
        
        with pytest.raises(ValueError, match="Circular dependency detected"):
            workflow.connect_components("component3", "component1")
    
    @pytest.mark.asyncio
    async def test_simple_workflow_execution(self):
        """Test executing a simple linear workflow."""
        workflow = Workflow("test_workflow")
        comp1 = MockComponent("component1")
        comp2 = MockComponent("component2")
        
        workflow.add_component(comp1)
        workflow.add_component(comp2)
        workflow.connect_components("component1", "component2")
        
        result = await workflow.execute()
        
        assert result.status == WorkflowStatus.COMPLETED
        assert len(result.component_results) == 2
        assert comp1.execute_called
        assert comp2.execute_called
        assert result.execution_time > 0
    
    @pytest.mark.asyncio
    async def test_workflow_with_failure(self):
        """Test workflow execution with component failure."""
        workflow = Workflow("test_workflow")
        comp1 = MockComponent("component1", should_fail=True)
        comp2 = MockComponent("component2")
        
        workflow.add_component(comp1)
        workflow.add_component(comp2)
        workflow.connect_components("component1", "component2")
        
        result = await workflow.execute()
        
        assert result.status == WorkflowStatus.FAILED
        assert len(result.errors) > 0
        assert comp1.execute_called
        assert not comp2.execute_called  # Should not execute after failure
    
    @pytest.mark.asyncio
    async def test_workflow_with_initial_inputs(self):
        """Test workflow execution with initial inputs."""
        workflow = Workflow("test_workflow")
        component = MockComponent("component1")
        workflow.add_component(component)
        
        initial_inputs = {"input_data": [1, 2, 3]}
        result = await workflow.execute(initial_inputs)
        
        assert result.status == WorkflowStatus.COMPLETED
        component_result = result.component_results["component1"]
        assert component_result.data["inputs"] == initial_inputs
    
    def test_workflow_validation(self):
        """Test workflow validation."""
        workflow = Workflow("test_workflow")
        comp1 = MockComponent("component1")
        comp2 = MockComponent("component2")
        
        workflow.add_component(comp1)
        workflow.add_component(comp2)
        
        # Add dependency to non-existent component
        comp2.add_dependency("nonexistent")
        
        errors = workflow.validate()
        assert len(errors) > 0
        assert any("depends on missing component" in error for error in errors)
    
    def test_get_component_graph(self):
        """Test getting component dependency graph."""
        workflow = Workflow("test_workflow")
        comp1 = MockComponent("component1")
        comp2 = MockComponent("component2")
        comp3 = MockComponent("component3")
        
        workflow.add_component(comp1)
        workflow.add_component(comp2)
        workflow.add_component(comp3)
        
        workflow.connect_components("component1", "component2")
        workflow.connect_components("component1", "component3")
        
        graph = workflow.get_component_graph()
        
        assert graph["component1"] == []
        assert graph["component2"] == ["component1"]
        assert graph["component3"] == ["component1"]
    
    def test_complex_execution_order(self):
        """Test complex dependency resolution."""
        workflow = Workflow("test_workflow")
        
        # Create components with complex dependencies
        # A -> B -> D
        # A -> C -> D
        # E (independent)
        
        comps = [MockComponent(f"component_{letter}") for letter in "ABCDE"]
        for comp in comps:
            workflow.add_component(comp)
        
        workflow.connect_components("component_A", "component_B")
        workflow.connect_components("component_A", "component_C")
        workflow.connect_components("component_B", "component_D")
        workflow.connect_components("component_C", "component_D")
        
        execution_order = workflow.execution_order
        
        # A should come before B and C
        assert execution_order.index("component_A") < execution_order.index("component_B")
        assert execution_order.index("component_A") < execution_order.index("component_C")
        
        # B and C should come before D
        assert execution_order.index("component_B") < execution_order.index("component_D")
        assert execution_order.index("component_C") < execution_order.index("component_D")
        
        # E can be anywhere (no dependencies)
        assert "component_E" in execution_order


if __name__ == "__main__":
    pytest.main([__file__])