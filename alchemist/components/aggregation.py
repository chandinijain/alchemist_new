"""Result aggregation components."""

import asyncio
from typing import Any, Dict, List, Optional, Union
import time
import json

from ..core.component import Component, ComponentResult, ComponentStatus


class ResultAggregation(Component):
    """Base class for result aggregation components."""
    
    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None):
        super().__init__(name, config)
        self.aggregation_strategy = config.get("aggregation_strategy", "merge") if config else "merge"
        self.weighting = config.get("weighting", {}) if config else {}
    
    def validate_config(self) -> bool:
        """Validate aggregation configuration."""
        valid_strategies = ["merge", "vote", "weighted_average", "consensus", "priority"]
        return self.aggregation_strategy in valid_strategies


class MergeAggregation(ResultAggregation):
    """Merge results from multiple sources."""
    
    async def execute(self, inputs: Dict[str, Any]) -> ComponentResult:
        """Execute result merging."""
        start_time = time.time()
        
        try:
            merged_results = await self._merge_inputs(inputs)
            
            return ComponentResult(
                status=ComponentStatus.COMPLETED,
                data=merged_results,
                metadata={
                    "aggregation_strategy": "merge",
                    "input_sources": list(inputs.keys()),
                    "total_inputs": len(inputs)
                },
                errors=[],
                execution_time=time.time() - start_time
            )
            
        except Exception as e:
            return ComponentResult(
                status=ComponentStatus.FAILED,
                data=None,
                metadata={},
                errors=[f"Merge aggregation failed: {str(e)}"],
                execution_time=time.time() - start_time
            )
    
    async def _merge_inputs(self, inputs: Dict[str, Any]) -> Dict:
        """Merge all inputs into a single result."""
        merged = {
            "sources": {},
            "combined_data": {},
            "metadata": {
                "merge_timestamp": time.time(),
                "source_count": len(inputs)
            }
        }
        
        for source_name, source_data in inputs.items():
            merged["sources"][source_name] = source_data
            
            # Extract specific data types for combination
            if isinstance(source_data, dict):
                if "insights" in source_data:
                    if "insights" not in merged["combined_data"]:
                        merged["combined_data"]["insights"] = []
                    if isinstance(source_data["insights"], list):
                        merged["combined_data"]["insights"].extend(source_data["insights"])
                
                if "recommendations" in source_data:
                    if "recommendations" not in merged["combined_data"]:
                        merged["combined_data"]["recommendations"] = []
                    if isinstance(source_data["recommendations"], list):
                        merged["combined_data"]["recommendations"].extend(source_data["recommendations"])
                
                if "conclusions" in source_data:
                    if "conclusions" not in merged["combined_data"]:
                        merged["combined_data"]["conclusions"] = []
                    if isinstance(source_data["conclusions"], list):
                        merged["combined_data"]["conclusions"].extend(source_data["conclusions"])
        
        return merged


class VotingAggregation(ResultAggregation):
    """Aggregate results using voting mechanisms."""
    
    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None):
        super().__init__(name, config)
        self.voting_method = config.get("voting_method", "majority") if config else "majority"
        self.confidence_threshold = config.get("confidence_threshold", 0.5) if config else 0.5
    
    async def execute(self, inputs: Dict[str, Any]) -> ComponentResult:
        """Execute voting aggregation."""
        start_time = time.time()
        
        try:
            voting_results = await self._perform_voting(inputs)
            
            return ComponentResult(
                status=ComponentStatus.COMPLETED,
                data=voting_results,
                metadata={
                    "aggregation_strategy": "voting",
                    "voting_method": self.voting_method,
                    "confidence_threshold": self.confidence_threshold,
                    "voter_count": len(inputs)
                },
                errors=[],
                execution_time=time.time() - start_time
            )
            
        except Exception as e:
            return ComponentResult(
                status=ComponentStatus.FAILED,
                data=None,
                metadata={},
                errors=[f"Voting aggregation failed: {str(e)}"],
                execution_time=time.time() - start_time
            )
    
    async def _perform_voting(self, inputs: Dict[str, Any]) -> Dict:
        """Perform voting on input results."""
        votes = {}
        confidence_scores = {}
        
        # Extract votable items from inputs
        for source_name, source_data in inputs.items():
            if isinstance(source_data, dict):
                # Extract confidence scores if available
                if "confidence_scores" in source_data:
                    confidence_scores[source_name] = source_data["confidence_scores"]
                
                # Extract conclusions/classifications for voting
                if "conclusions" in source_data and isinstance(source_data["conclusions"], list):
                    for conclusion in source_data["conclusions"]:
                        if conclusion not in votes:
                            votes[conclusion] = []
                        votes[conclusion].append(source_name)
                
                # Extract binary classifications
                if "classification" in source_data:
                    classification = source_data["classification"]
                    if classification not in votes:
                        votes[classification] = []
                    votes[classification].append(source_name)
        
        # Determine winner based on voting method
        winner = self._determine_winner(votes)
        
        return {
            "voting_results": votes,
            "winner": winner,
            "confidence_scores": confidence_scores,
            "voting_summary": {
                "total_votes": sum(len(voters) for voters in votes.values()),
                "unique_options": len(votes),
                "winning_margin": len(votes.get(winner, [])) if winner else 0
            }
        }
    
    def _determine_winner(self, votes: Dict[str, List[str]]) -> Optional[str]:
        """Determine winner based on voting method."""
        if not votes:
            return None
        
        if self.voting_method == "majority":
            # Simple majority - most votes wins
            return max(votes.keys(), key=lambda k: len(votes[k]))
        elif self.voting_method == "plurality":
            # Plurality - highest count wins (same as majority for this implementation)
            return max(votes.keys(), key=lambda k: len(votes[k]))
        elif self.voting_method == "unanimous":
            # Unanimous - only win if all votes agree
            if len(votes) == 1:
                return list(votes.keys())[0]
            return None
        
        return None


class WeightedAggregation(ResultAggregation):
    """Aggregate results using weighted averaging."""
    
    async def execute(self, inputs: Dict[str, Any]) -> ComponentResult:
        """Execute weighted aggregation."""
        start_time = time.time()
        
        try:
            weighted_results = await self._perform_weighted_aggregation(inputs)
            
            return ComponentResult(
                status=ComponentStatus.COMPLETED,
                data=weighted_results,
                metadata={
                    "aggregation_strategy": "weighted_average",
                    "weighting_scheme": self.weighting,
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
                errors=[f"Weighted aggregation failed: {str(e)}"],
                execution_time=time.time() - start_time
            )
    
    async def _perform_weighted_aggregation(self, inputs: Dict[str, Any]) -> Dict:
        """Perform weighted aggregation of inputs."""
        weighted_scores = {}
        total_weight = 0
        
        # Calculate weighted averages for numerical values
        for source_name, source_data in inputs.items():
            weight = self.weighting.get(source_name, 1.0)
            total_weight += weight
            
            if isinstance(source_data, dict):
                # Extract numerical confidence scores
                if "confidence_scores" in source_data:
                    for key, score in source_data["confidence_scores"].items():
                        if isinstance(score, (int, float)):
                            if key not in weighted_scores:
                                weighted_scores[key] = {"total": 0, "weight_sum": 0}
                            weighted_scores[key]["total"] += score * weight
                            weighted_scores[key]["weight_sum"] += weight
        
        # Calculate final weighted averages
        final_scores = {}
        for key, data in weighted_scores.items():
            if data["weight_sum"] > 0:
                final_scores[key] = data["total"] / data["weight_sum"]
        
        # Aggregate textual content with weights
        weighted_content = self._aggregate_textual_content(inputs, self.weighting)
        
        return {
            "weighted_scores": final_scores,
            "aggregated_content": weighted_content,
            "weighting_applied": self.weighting,
            "total_weight": total_weight
        }
    
    def _aggregate_textual_content(self, inputs: Dict[str, Any], weights: Dict[str, float]) -> Dict:
        """Aggregate textual content based on weights."""
        content_sections = {
            "high_weight_insights": [],
            "medium_weight_insights": [],
            "low_weight_insights": [],
            "all_recommendations": []
        }
        
        for source_name, source_data in inputs.items():
            weight = weights.get(source_name, 1.0)
            
            if isinstance(source_data, dict):
                # Categorize insights by weight
                if "insights" in source_data and isinstance(source_data["insights"], list):
                    if weight >= 0.8:
                        content_sections["high_weight_insights"].extend(source_data["insights"])
                    elif weight >= 0.5:
                        content_sections["medium_weight_insights"].extend(source_data["insights"])
                    else:
                        content_sections["low_weight_insights"].extend(source_data["insights"])
                
                # Collect all recommendations
                if "recommendations" in source_data and isinstance(source_data["recommendations"], list):
                    for rec in source_data["recommendations"]:
                        content_sections["all_recommendations"].append({
                            "recommendation": rec,
                            "source": source_name,
                            "weight": weight
                        })
        
        # Sort recommendations by weight
        content_sections["all_recommendations"].sort(key=lambda x: x["weight"], reverse=True)
        
        return content_sections


class ConsensusAggregation(ResultAggregation):
    """Aggregate results by finding consensus."""
    
    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None):
        super().__init__(name, config)
        self.consensus_threshold = config.get("consensus_threshold", 0.7) if config else 0.7
        self.min_agreement = config.get("min_agreement", 2) if config else 2
    
    async def execute(self, inputs: Dict[str, Any]) -> ComponentResult:
        """Execute consensus aggregation."""
        start_time = time.time()
        
        try:
            consensus_results = await self._find_consensus(inputs)
            
            return ComponentResult(
                status=ComponentStatus.COMPLETED,
                data=consensus_results,
                metadata={
                    "aggregation_strategy": "consensus",
                    "consensus_threshold": self.consensus_threshold,
                    "min_agreement": self.min_agreement,
                    "participant_count": len(inputs)
                },
                errors=[],
                execution_time=time.time() - start_time
            )
            
        except Exception as e:
            return ComponentResult(
                status=ComponentStatus.FAILED,
                data=None,
                metadata={},
                errors=[f"Consensus aggregation failed: {str(e)}"],
                execution_time=time.time() - start_time
            )
    
    async def _find_consensus(self, inputs: Dict[str, Any]) -> Dict:
        """Find consensus among input results."""
        consensus_items = {
            "strong_consensus": [],
            "weak_consensus": [],
            "no_consensus": [],
            "agreement_matrix": {}
        }
        
        # Extract common elements across inputs
        all_insights = []
        all_recommendations = []
        
        for source_name, source_data in inputs.items():
            if isinstance(source_data, dict):
                if "insights" in source_data and isinstance(source_data["insights"], list):
                    all_insights.extend([(insight, source_name) for insight in source_data["insights"]])
                
                if "recommendations" in source_data and isinstance(source_data["recommendations"], list):
                    all_recommendations.extend([(rec, source_name) for rec in source_data["recommendations"]])
        
        # Find consensus in insights
        insight_consensus = self._analyze_consensus(all_insights, len(inputs))
        consensus_items["insights_consensus"] = insight_consensus
        
        # Find consensus in recommendations
        rec_consensus = self._analyze_consensus(all_recommendations, len(inputs))
        consensus_items["recommendations_consensus"] = rec_consensus
        
        return consensus_items
    
    def _analyze_consensus(self, items: List[tuple], total_sources: int) -> Dict:
        """Analyze consensus for a list of items."""
        item_counts = {}
        
        for item, source in items:
            item_key = item.lower().strip()
            if item_key not in item_counts:
                item_counts[item_key] = {"count": 0, "sources": [], "original": item}
            item_counts[item_key]["count"] += 1
            if source not in item_counts[item_key]["sources"]:
                item_counts[item_key]["sources"].append(source)
        
        consensus_results = {
            "strong_consensus": [],
            "weak_consensus": [],
            "no_consensus": []
        }
        
        for item_key, data in item_counts.items():
            agreement_ratio = len(data["sources"]) / total_sources
            
            if agreement_ratio >= self.consensus_threshold and len(data["sources"]) >= self.min_agreement:
                consensus_results["strong_consensus"].append({
                    "item": data["original"],
                    "agreement_ratio": agreement_ratio,
                    "supporting_sources": data["sources"]
                })
            elif len(data["sources"]) >= self.min_agreement:
                consensus_results["weak_consensus"].append({
                    "item": data["original"],
                    "agreement_ratio": agreement_ratio,
                    "supporting_sources": data["sources"]
                })
            else:
                consensus_results["no_consensus"].append({
                    "item": data["original"],
                    "agreement_ratio": agreement_ratio,
                    "supporting_sources": data["sources"]
                })
        
        return consensus_results