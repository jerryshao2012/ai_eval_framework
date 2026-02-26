from __future__ import annotations

# Continuous Monitoring metric names (taxonomy-native, no name mapping).
SAFETY_TOXICITY = "safety_toxicity"
SAFETY_BIAS_FAIRNESS = "safety_bias_fairness"
SAFETY_ROBUSTNESS = "safety_robustness"
SAFETY_COMPLIANCE = "safety_compliance"
PERFORMANCE_GROUNDEDNESS_FAITHFULNESS = "performance_groundedness_faithfulness"
PERFORMANCE_RELEVANCE = "performance_relevance"
PERFORMANCE_PRECISION_COHERENCE = "performance_precision_coherence"
PERFORMANCE_READABILITY_FLUENCY_STYLE = "performance_readability_fluency_style"
SYSTEM_RELIABILITY_LATENCY = "system_reliability_latency"
SYSTEM_RELIABILITY_AVAILABILITY_RESOURCE_HEALTH = "system_reliability_availability_resource_health"

CONTINUOUS_MONITORING_METRICS = [
    SAFETY_TOXICITY,
    SAFETY_BIAS_FAIRNESS,
    SAFETY_ROBUSTNESS,
    SAFETY_COMPLIANCE,
    PERFORMANCE_GROUNDEDNESS_FAITHFULNESS,
    PERFORMANCE_RELEVANCE,
    PERFORMANCE_PRECISION_COHERENCE,
    PERFORMANCE_READABILITY_FLUENCY_STYLE,
    SYSTEM_RELIABILITY_LATENCY,
    SYSTEM_RELIABILITY_AVAILABILITY_RESOURCE_HEALTH,
]
