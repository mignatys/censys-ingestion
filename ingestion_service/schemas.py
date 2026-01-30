"""Pydantic schemas for the ingestion service."""
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, ConfigDict, Field, IPvAnyAddress

class AlertBase(BaseModel):
    """Raw fields expected from upstream."""
    id: str
    source: str
    severity: str
    description: str
    src_ip: IPvAnyAddress
    dst_ip: IPvAnyAddress
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)

class AlertLegacy(BaseModel):
    """Legacy format from older vendors."""
    alertId: str
    source: str
    criticality: int
    summary: str
    sourceAddress: str
    destinationAddress: str
    timestamp: datetime

class AlertEnriched(AlertBase):
    """Enriched fields added by the ingestion service."""
    country: Optional[str] = Field(None, description="Country code for the destination IP")
    user: Optional[str] = Field(None, description="User associated with the source IP")

class ConfigUpdate(BaseModel):
    """Schema for configuration updates."""
    sync_interval_minutes: int
