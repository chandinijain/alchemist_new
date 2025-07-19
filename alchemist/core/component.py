"""Base component class for all workflow components."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from dataclasses import dataclass
from enum import Enum


class ComponentStatus(Enum):
    """Status of a component execution."""
    PENDING = "pending"
    RUNNING = "running" 
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class ComponentResult:
    """Result of a component execution."""
    status: ComponentStatus
    data: Any
    metadata: Dict[str, Any]
    errors: List[str]
    execution_time: float


class Component(ABC):
    """Base class for all workflow components."""
    
    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None):
        self.name = name
        self.config = config or {}
        self.status = ComponentStatus.PENDING
        self.dependencies: List[str] = []
        self.outputs: List[str] = []
        
    @abstractmethod
    async def execute(self, inputs: Dict[str, Any]) -> ComponentResult:
        """Execute the component with given inputs."""
        pass
    
    @abstractmethod
    def validate_config(self) -> bool:
        """Validate component configuration."""
        pass
    
    def add_dependency(self, dependency: str) -> None:
        """Add a dependency component."""
        if dependency not in self.dependencies:
            self.dependencies.append(dependency)
    
    def add_output(self, output: str) -> None:
        """Add an output identifier."""
        if output not in self.outputs:
            self.outputs.append(output)
    
    def get_dependencies(self) -> List[str]:
        """Get list of dependency components."""
        return self.dependencies.copy()
    
    def get_outputs(self) -> List[str]:
        """Get list of output identifiers."""
        return self.outputs.copy()
    
    def __str__(self) -> str:
        return f"{self.__class__.__name__}(name='{self.name}', status={self.status.value})"