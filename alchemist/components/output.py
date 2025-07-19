"""Output handling components."""

import asyncio
import json
import csv
from typing import Any, Dict, List, Optional, Union
from pathlib import Path
import time

from ..core.component import Component, ComponentResult, ComponentStatus


class OutputHandler(Component):
    """Base class for output handling components."""
    
    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None):
        super().__init__(name, config)
        self.output_format = config.get("output_format", "json") if config else "json"
        self.output_path = config.get("output_path") if config else None
    
    def validate_config(self) -> bool:
        """Validate output configuration."""
        valid_formats = ["json", "csv", "txt", "html", "console"]
        return self.output_format in valid_formats


class FileOutput(OutputHandler):
    """Output results to files."""
    
    async def execute(self, inputs: Dict[str, Any]) -> ComponentResult:
        """Execute file output."""
        start_time = time.time()
        
        try:
            output_data = self._prepare_output_data(inputs)
            output_path = await self._write_to_file(output_data)
            
            return ComponentResult(
                status=ComponentStatus.COMPLETED,
                data={"output_path": output_path, "format": self.output_format},
                metadata={
                    "output_format": self.output_format,
                    "output_path": output_path,
                    "file_size": Path(output_path).stat().st_size if Path(output_path).exists() else 0
                },
                errors=[],
                execution_time=time.time() - start_time
            )
            
        except Exception as e:
            return ComponentResult(
                status=ComponentStatus.FAILED,
                data=None,
                metadata={},
                errors=[f"File output failed: {str(e)}"],
                execution_time=time.time() - start_time
            )
    
    def _prepare_output_data(self, inputs: Dict[str, Any]) -> Union[Dict, List, str]:
        """Prepare data for output."""
        if len(inputs) == 1:
            # Single input - output directly
            return list(inputs.values())[0]
        else:
            # Multiple inputs - create structured output
            return {
                "workflow_results": inputs,
                "metadata": {
                    "timestamp": time.time(),
                    "input_count": len(inputs),
                    "output_format": self.output_format
                }
            }
    
    async def _write_to_file(self, data: Union[Dict, List, str]) -> str:
        """Write data to file based on format."""
        if not self.output_path:
            # Generate default output path
            timestamp = int(time.time())
            self.output_path = f"output_{timestamp}.{self.output_format}"
        
        output_path = Path(self.output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        if self.output_format == "json":
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False, default=str)
        
        elif self.output_format == "csv":
            with open(output_path, 'w', newline='', encoding='utf-8') as f:
                if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
                    # List of dictionaries - write as CSV
                    fieldnames = data[0].keys()
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(data)
                else:
                    # Convert other data types to CSV-friendly format
                    writer = csv.writer(f)
                    if isinstance(data, dict):
                        for key, value in data.items():
                            writer.writerow([key, value])
                    else:
                        writer.writerow([str(data)])
        
        elif self.output_format == "txt":
            with open(output_path, 'w', encoding='utf-8') as f:
                if isinstance(data, (dict, list)):
                    f.write(json.dumps(data, indent=2, ensure_ascii=False, default=str))
                else:
                    f.write(str(data))
        
        elif self.output_format == "html":
            html_content = self._generate_html_report(data)
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
        
        return str(output_path)
    
    def _generate_html_report(self, data: Union[Dict, List, str]) -> str:
        """Generate HTML report from data."""
        html = """<!DOCTYPE html>
<html>
<head>
    <title>Alchemist Workflow Results</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .section { margin-bottom: 20px; padding: 15px; border: 1px solid #ddd; border-radius: 5px; }
        .section h2 { margin-top: 0; color: #333; }
        .json-data { background-color: #f5f5f5; padding: 10px; border-radius: 3px; white-space: pre-wrap; font-family: monospace; }
        .metadata { font-size: 0.9em; color: #666; }
        ul { margin: 10px 0; }
        li { margin: 5px 0; }
    </style>
</head>
<body>
    <h1>Alchemist Workflow Results</h1>
"""
        
        if isinstance(data, dict):
            if "workflow_results" in data:
                # Structured workflow output
                html += self._format_workflow_results_html(data["workflow_results"])
                if "metadata" in data:
                    html += f'<div class="section"><h2>Metadata</h2><div class="json-data">{json.dumps(data["metadata"], indent=2)}</div></div>'
            else:
                # Single component result
                html += f'<div class="section"><h2>Results</h2><div class="json-data">{json.dumps(data, indent=2, default=str)}</div></div>'
        else:
            html += f'<div class="section"><h2>Data</h2><div class="json-data">{json.dumps(data, indent=2, default=str)}</div></div>'
        
        html += """
</body>
</html>"""
        return html
    
    def _format_workflow_results_html(self, workflow_results: Dict[str, Any]) -> str:
        """Format workflow results for HTML display."""
        html = ""
        
        for component_name, component_data in workflow_results.items():
            html += f'<div class="section"><h2>Component: {component_name}</h2>'
            
            if isinstance(component_data, dict):
                # Format specific sections
                if "insights" in component_data:
                    html += "<h3>Insights</h3><ul>"
                    insights = component_data["insights"]
                    if isinstance(insights, list):
                        for insight in insights:
                            html += f"<li>{insight}</li>"
                    html += "</ul>"
                
                if "recommendations" in component_data:
                    html += "<h3>Recommendations</h3><ul>"
                    recommendations = component_data["recommendations"]
                    if isinstance(recommendations, list):
                        for rec in recommendations:
                            html += f"<li>{rec}</li>"
                    html += "</ul>"
                
                if "conclusions" in component_data:
                    html += "<h3>Conclusions</h3><ul>"
                    conclusions = component_data["conclusions"]
                    if isinstance(conclusions, list):
                        for conclusion in conclusions:
                            html += f"<li>{conclusion}</li>"
                    html += "</ul>"
                
                # Show raw data for other fields
                other_data = {k: v for k, v in component_data.items() 
                             if k not in ["insights", "recommendations", "conclusions"]}
                if other_data:
                    html += f'<h3>Additional Data</h3><div class="json-data">{json.dumps(other_data, indent=2, default=str)}</div>'
            else:
                html += f'<div class="json-data">{json.dumps(component_data, indent=2, default=str)}</div>'
            
            html += "</div>"
        
        return html


class ConsoleOutput(OutputHandler):
    """Output results to console."""
    
    async def execute(self, inputs: Dict[str, Any]) -> ComponentResult:
        """Execute console output."""
        start_time = time.time()
        
        try:
            self._print_to_console(inputs)
            
            return ComponentResult(
                status=ComponentStatus.COMPLETED,
                data={"output_method": "console", "displayed": True},
                metadata={
                    "output_format": "console",
                    "input_count": len(inputs)
                },
                errors=[],
                execution_time=time.time() - start_time
            )
            
        except Exception as e:
            return ComponentResult(
                status=ComponentStatus.FAILED,
                data=None,
                metadata={},
                errors=[f"Console output failed: {str(e)}"],
                execution_time=time.time() - start_time
            )
    
    def _print_to_console(self, inputs: Dict[str, Any]) -> None:
        """Print results to console in a formatted way."""
        print("=" * 60)
        print("ALCHEMIST WORKFLOW RESULTS")
        print("=" * 60)
        
        for component_name, component_data in inputs.items():
            print(f"\n📊 Component: {component_name.upper()}")
            print("-" * 40)
            
            if isinstance(component_data, dict):
                self._print_structured_data(component_data)
            else:
                print(f"Data: {component_data}")
        
        print("\n" + "=" * 60)
    
    def _print_structured_data(self, data: Dict[str, Any]) -> None:
        """Print structured data to console."""
        # Print insights
        if "insights" in data and isinstance(data["insights"], list):
            print("💡 Insights:")
            for i, insight in enumerate(data["insights"], 1):
                print(f"   {i}. {insight}")
            print()
        
        # Print recommendations
        if "recommendations" in data and isinstance(data["recommendations"], list):
            print("🎯 Recommendations:")
            for i, rec in enumerate(data["recommendations"], 1):
                print(f"   {i}. {rec}")
            print()
        
        # Print conclusions
        if "conclusions" in data and isinstance(data["conclusions"], list):
            print("📝 Conclusions:")
            for i, conclusion in enumerate(data["conclusions"], 1):
                print(f"   {i}. {conclusion}")
            print()
        
        # Print confidence scores
        if "confidence_scores" in data and isinstance(data["confidence_scores"], dict):
            print("📈 Confidence Scores:")
            for key, score in data["confidence_scores"].items():
                if isinstance(score, (int, float)):
                    print(f"   {key}: {score:.2f}")
                else:
                    print(f"   {key}: {score}")
            print()
        
        # Print other relevant data
        relevant_keys = ["voting_results", "winner", "weighted_scores", "consensus_items"]
        for key in relevant_keys:
            if key in data:
                print(f"📋 {key.replace('_', ' ').title()}:")
                if isinstance(data[key], dict):
                    for k, v in data[key].items():
                        print(f"   {k}: {v}")
                else:
                    print(f"   {data[key]}")
                print()


class APIOutput(OutputHandler):
    """Output results to external APIs."""
    
    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None):
        super().__init__(name, config)
        self.api_endpoint = config.get("api_endpoint") if config else None
        self.api_method = config.get("api_method", "POST") if config else "POST"
        self.api_headers = config.get("api_headers", {}) if config else {}
    
    def validate_config(self) -> bool:
        """Validate API output configuration."""
        return bool(self.api_endpoint)
    
    async def execute(self, inputs: Dict[str, Any]) -> ComponentResult:
        """Execute API output."""
        start_time = time.time()
        
        try:
            # Simulate API call - replace with actual HTTP client
            await asyncio.sleep(0.1)
            
            payload = self._prepare_api_payload(inputs)
            
            # Mock API response
            api_response = {
                "status": "success",
                "message": "Data successfully sent to API",
                "endpoint": self.api_endpoint,
                "payload_size": len(str(payload))
            }
            
            return ComponentResult(
                status=ComponentStatus.COMPLETED,
                data=api_response,
                metadata={
                    "api_endpoint": self.api_endpoint,
                    "api_method": self.api_method,
                    "payload_size": len(str(payload))
                },
                errors=[],
                execution_time=time.time() - start_time
            )
            
        except Exception as e:
            return ComponentResult(
                status=ComponentStatus.FAILED,
                data=None,
                metadata={},
                errors=[f"API output failed: {str(e)}"],
                execution_time=time.time() - start_time
            )
    
    def _prepare_api_payload(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare payload for API submission."""
        return {
            "workflow_results": inputs,
            "timestamp": time.time(),
            "format_version": "1.0"
        }