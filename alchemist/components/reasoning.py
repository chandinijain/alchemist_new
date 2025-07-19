"""Reasoning components for probabilistic and deterministic analysis."""

import asyncio
import json
from typing import Any, Dict, List, Optional, Union, Callable
import time

from ..core.component import Component, ComponentResult, ComponentStatus


class ReasoningEngine(Component):
    """Base class for reasoning components."""
    
    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None):
        super().__init__(name, config)
        self.reasoning_type = config.get("reasoning_type", "deterministic") if config else "deterministic"
    
    def validate_config(self) -> bool:
        """Validate reasoning configuration."""
        return self.reasoning_type in ["deterministic", "probabilistic"]


class DeterministicReasoning(ReasoningEngine):
    """Deterministic reasoning using rules and function calls."""
    
    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None):
        super().__init__(name, config)
        self.rules = config.get("rules", []) if config else []
        self.functions = config.get("functions", {}) if config else {}
    
    async def execute(self, inputs: Dict[str, Any]) -> ComponentResult:
        """Execute deterministic reasoning."""
        start_time = time.time()
        
        try:
            input_data = None
            for key, value in inputs.items():
                if isinstance(value, (list, dict)):
                    input_data = value
                    break
            
            if input_data is None:
                raise ValueError("No valid input data found")
            
            reasoning_results = await self._apply_rules(input_data)
            
            return ComponentResult(
                status=ComponentStatus.COMPLETED,
                data=reasoning_results,
                metadata={
                    "reasoning_type": "deterministic",
                    "rules_applied": len(self.rules),
                    "functions_used": list(self.functions.keys())
                },
                errors=[],
                execution_time=time.time() - start_time
            )
            
        except Exception as e:
            return ComponentResult(
                status=ComponentStatus.FAILED,
                data=None,
                metadata={},
                errors=[f"Deterministic reasoning failed: {str(e)}"],
                execution_time=time.time() - start_time
            )
    
    async def _apply_rules(self, data: Union[List, Dict]) -> Dict:
        """Apply deterministic rules to data."""
        results = {
            "input_summary": self._summarize_input(data),
            "rule_evaluations": [],
            "function_calls": [],
            "conclusions": []
        }
        
        # Apply each rule
        for rule in self.rules:
            rule_result = self._evaluate_rule(rule, data)
            results["rule_evaluations"].append(rule_result)
            
            if rule_result["triggered"]:
                results["conclusions"].append(rule_result["conclusion"])
        
        # Execute function calls if any
        for func_name, func_config in self.functions.items():
            func_result = await self._call_function(func_name, func_config, data)
            results["function_calls"].append(func_result)
        
        return results
    
    def _summarize_input(self, data: Union[List, Dict]) -> Dict:
        """Summarize input data."""
        if isinstance(data, list):
            return {
                "type": "list",
                "count": len(data),
                "sample": data[:3] if len(data) > 0 else []
            }
        elif isinstance(data, dict):
            return {
                "type": "dict",
                "keys": list(data.keys())[:10],
                "size": len(data)
            }
        else:
            return {"type": type(data).__name__, "value": str(data)[:100]}
    
    def _evaluate_rule(self, rule: Dict, data: Union[List, Dict]) -> Dict:
        """Evaluate a single rule against data."""
        rule_name = rule.get("name", "unnamed_rule")
        condition = rule.get("condition", {})
        conclusion = rule.get("conclusion", "No conclusion")
        
        triggered = self._check_condition(condition, data)
        
        return {
            "rule_name": rule_name,
            "triggered": triggered,
            "condition": condition,
            "conclusion": conclusion if triggered else None
        }
    
    def _check_condition(self, condition: Dict, data: Union[List, Dict]) -> bool:
        """Check if condition is met by data."""
        condition_type = condition.get("type", "always")
        
        if condition_type == "always":
            return True
        elif condition_type == "count_threshold":
            threshold = condition.get("threshold", 0)
            if isinstance(data, list):
                return len(data) >= threshold
            return 1 >= threshold
        elif condition_type == "field_value":
            field = condition.get("field")
            value = condition.get("value")
            operator = condition.get("operator", "eq")
            
            if isinstance(data, list):
                return any(self._check_field_condition(item, field, value, operator) for item in data)
            elif isinstance(data, dict):
                return self._check_field_condition(data, field, value, operator)
        
        return False
    
    def _check_field_condition(self, item: Dict, field: str, value: Any, operator: str) -> bool:
        """Check field condition for a single item."""
        if field not in item:
            return False
        
        item_value = item[field]
        
        if operator == "eq":
            return item_value == value
        elif operator == "ne":
            return item_value != value
        elif operator == "gt":
            return item_value > value
        elif operator == "lt":
            return item_value < value
        elif operator == "contains":
            return value in str(item_value)
        
        return False
    
    async def _call_function(self, func_name: str, func_config: Dict, data: Union[List, Dict]) -> Dict:
        """Execute a function call."""
        func_type = func_config.get("type", "builtin")
        
        if func_type == "builtin":
            return await self._call_builtin_function(func_name, func_config, data)
        elif func_type == "api":
            return await self._call_api_function(func_name, func_config, data)
        else:
            return {
                "function_name": func_name,
                "status": "error",
                "error": f"Unknown function type: {func_type}"
            }
    
    async def _call_builtin_function(self, func_name: str, func_config: Dict, data: Union[List, Dict]) -> Dict:
        """Call a built-in function."""
        operation = func_config.get("operation", "count")
        
        if operation == "count":
            result = len(data) if isinstance(data, list) else 1
        elif operation == "sum" and isinstance(data, list):
            field = func_config.get("field")
            values = [item.get(field, 0) for item in data if isinstance(item, dict) and field in item]
            result = sum(float(v) for v in values if isinstance(v, (int, float)) or str(v).isdigit())
        else:
            result = f"Unknown operation: {operation}"
        
        return {
            "function_name": func_name,
            "operation": operation,
            "result": result,
            "status": "success"
        }
    
    async def _call_api_function(self, func_name: str, func_config: Dict, data: Union[List, Dict]) -> Dict:
        """Call an external API function."""
        # Simulate API call
        await asyncio.sleep(0.1)
        
        return {
            "function_name": func_name,
            "api_endpoint": func_config.get("endpoint", "unknown"),
            "result": f"API result for {func_name}",
            "status": "success"
        }


class ProbabilisticReasoning(ReasoningEngine):
    """Probabilistic reasoning using LLM calls."""
    
    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None):
        super().__init__(name, config)
        self.model_config = config.get("model_config", {}) if config else {}
        self.prompts = config.get("prompts", {}) if config else {}
        self.temperature = config.get("temperature", 0.7) if config else 0.7
    
    async def execute(self, inputs: Dict[str, Any]) -> ComponentResult:
        """Execute probabilistic reasoning."""
        start_time = time.time()
        
        try:
            input_data = None
            for key, value in inputs.items():
                if isinstance(value, (list, dict)):
                    input_data = value
                    break
            
            if input_data is None:
                raise ValueError("No valid input data found")
            
            reasoning_results = await self._perform_llm_reasoning(input_data)
            
            return ComponentResult(
                status=ComponentStatus.COMPLETED,
                data=reasoning_results,
                metadata={
                    "reasoning_type": "probabilistic",
                    "model_config": self.model_config,
                    "temperature": self.temperature,
                    "prompts_used": list(self.prompts.keys())
                },
                errors=[],
                execution_time=time.time() - start_time
            )
            
        except Exception as e:
            return ComponentResult(
                status=ComponentStatus.FAILED,
                data=None,
                metadata={},
                errors=[f"Probabilistic reasoning failed: {str(e)}"],
                execution_time=time.time() - start_time
            )
    
    async def _perform_llm_reasoning(self, data: Union[List, Dict]) -> Dict:
        """Perform LLM-based reasoning."""
        # Simulate LLM processing
        await asyncio.sleep(0.5)
        
        data_summary = self._prepare_data_for_llm(data)
        
        results = {
            "input_analysis": data_summary,
            "llm_responses": [],
            "confidence_scores": {},
            "insights": [],
            "recommendations": []
        }
        
        # Process each prompt
        for prompt_name, prompt_config in self.prompts.items():
            llm_response = await self._call_llm(prompt_name, prompt_config, data_summary)
            results["llm_responses"].append(llm_response)
            results["confidence_scores"][prompt_name] = llm_response.get("confidence", 0.5)
        
        # Generate insights and recommendations
        results["insights"] = await self._generate_insights(data_summary, results["llm_responses"])
        results["recommendations"] = await self._generate_recommendations(results["insights"])
        
        return results
    
    def _prepare_data_for_llm(self, data: Union[List, Dict]) -> str:
        """Prepare data for LLM processing."""
        if isinstance(data, list):
            if len(data) > 10:
                sample_data = data[:10]
                summary = f"List of {len(data)} items. Sample: {json.dumps(sample_data, indent=2)}"
            else:
                summary = json.dumps(data, indent=2)
        elif isinstance(data, dict):
            summary = json.dumps(data, indent=2)
        else:
            summary = str(data)
        
        return summary
    
    async def _call_llm(self, prompt_name: str, prompt_config: Dict, data_summary: str) -> Dict:
        """Simulate LLM API call."""
        # This would be replaced with actual LLM API calls
        await asyncio.sleep(0.2)
        
        prompt_template = prompt_config.get("template", "Analyze this data: {data}")
        full_prompt = prompt_template.format(data=data_summary)
        
        # Mock LLM response
        mock_responses = {
            "analysis": "The data shows interesting patterns with potential correlations between variables.",
            "classification": "Based on the input characteristics, this appears to be a positive case.",
            "summarization": "Key findings: 1) Data quality is good, 2) Trends are upward, 3) No anomalies detected.",
            "extraction": "Important entities found: dates, numerical values, and categorical classifications."
        }
        
        response_text = mock_responses.get(prompt_name, "Generic analysis response")
        
        return {
            "prompt_name": prompt_name,
            "prompt": full_prompt[:200] + "..." if len(full_prompt) > 200 else full_prompt,
            "response": response_text,
            "confidence": 0.85,
            "tokens_used": len(full_prompt.split()) + len(response_text.split())
        }
    
    async def _generate_insights(self, data_summary: str, llm_responses: List[Dict]) -> List[str]:
        """Generate insights from LLM responses."""
        insights = [
            "Data quality appears to be high based on structure analysis",
            "Pattern recognition suggests seasonal trends",
            "Anomaly detection shows minimal outliers"
        ]
        
        # Add insights based on LLM responses
        for response in llm_responses:
            if "positive" in response["response"].lower():
                insights.append("Positive sentiment/classification detected")
            if "trend" in response["response"].lower():
                insights.append("Trending patterns identified")
        
        return insights
    
    async def _generate_recommendations(self, insights: List[str]) -> List[str]:
        """Generate recommendations based on insights."""
        recommendations = [
            "Continue monitoring data quality metrics",
            "Consider implementing automated trend analysis",
            "Set up alerts for significant pattern changes"
        ]
        
        if any("positive" in insight.lower() for insight in insights):
            recommendations.append("Leverage positive indicators for strategic planning")
        
        if any("trend" in insight.lower() for insight in insights):
            recommendations.append("Develop predictive models based on identified trends")
        
        return recommendations