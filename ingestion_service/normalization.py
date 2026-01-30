"""Normalization adapters and registry."""
from abc import ABC, abstractmethod
from typing import List, Optional
from ingestion_service.schemas import AlertBase, AlertLegacy

# --- Adapters ---

class BaseAdapter(ABC):
    """Abstract base class for alert normalization adapters."""
    @abstractmethod
    def normalize(self, item: dict) -> AlertBase:
        """Normalize a raw dictionary item into an AlertBase object."""
        pass

class StandardAdapter(BaseAdapter):
    """Adapter for treating standard alert formats (pass-through)."""
    def normalize(self, item: dict) -> AlertBase:
        """Normalize standard alert items."""
        # Standard Format Group A
        # "id", "source", "severity", "description", "src_ip", "dst_ip", "created_at"
        return AlertBase(**item)

class LegacyAdapter(BaseAdapter):
    """Adapter for legacy alert formats requiring mapping."""
    def normalize(self, item: dict) -> AlertBase:
        """Normalize legacy alert items."""
        # Legacy Format Group B
        # "alertId", "source", "criticality" (int), "summary", "sourceAddress", "destinationAddress", "timestamp"
        legacy = AlertLegacy(**item)
        
        # Map Criticality
        severity_map = {1: "low", 2: "medium", 3: "high", 4: "critical"}
        severity = severity_map.get(legacy.criticality, "low")
        
        return AlertBase(
            id=legacy.alertId,
            source=legacy.source,
            severity=severity,
            description=legacy.summary,
            src_ip=legacy.sourceAddress,
            dst_ip=legacy.destinationAddress,
            created_at=legacy.timestamp
        )

class AdapterRegistry:
    """Registry to map source types to specific adapters."""
    def __init__(self):
        self._adapters = {}

    def register(self, sources: List[str], adapter: BaseAdapter):
        """Register an adapter for a list of source strings."""
        for source in sources:
            self._adapters[source] = adapter

    def get_adapter(self, source: str) -> Optional[BaseAdapter]:
        """Retrieve an adapter for a given source."""
        return self._adapters.get(source)

# Global Registry Instance
registry = AdapterRegistry()

# Register Standard Sources
_STANDARD_SOURCES = ["firewall", "ngfw", "ips", "vpn_gateway", "router_acl"]
registry.register(_STANDARD_SOURCES, StandardAdapter())

# Register Legacy Sources
_LEGACY_SOURCES = ["waf", "swg", "nac", "api_gateway", "load_balancer"]
registry.register(_LEGACY_SOURCES, LegacyAdapter())
