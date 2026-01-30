"""SQLAlchemy models for the ingestion service."""
from datetime import datetime
from sqlalchemy import String, DateTime, func, Integer, Index
from sqlalchemy.dialects.postgresql import INET
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    """Base class for all SQLAlchemy models."""

class Alert(Base):
    """SQLAlchemy model for the 'alerts' table."""
    __tablename__ = "alerts"

    # Fields from Upstream
    id: Mapped[str] = mapped_column(String, primary_key=True)
    source: Mapped[str] = mapped_column(String)
    severity: Mapped[str] = mapped_column(String)
    description: Mapped[str] = mapped_column(String)
    src_ip: Mapped[str] = mapped_column(INET)
    dst_ip: Mapped[str] = mapped_column(INET)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    
    # Enriched Fields
    country: Mapped[str | None] = mapped_column(String, nullable=True)
    user: Mapped[str | None] = mapped_column(String, nullable=True)

    # pylint: disable=not-callable
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    
    __table_args__ = (
        Index("ix_alerts_severity_ingested", "severity", "ingested_at"),
        Index("ix_alerts_country_ingested", "country", "ingested_at"),
        Index("ix_alerts_user_ingested", "user", "ingested_at"),
        Index("ix_alerts_ingested", "ingested_at"),
    )

class ServiceState(Base):
    """Model to track the state of the ingestion service (checkpoints)."""
    __tablename__ = "service_state"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    last_sync_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True) # Checkpoint High-water mark
    last_success_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    last_event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True) # Data High Water Mark (created_at of last alert)
    last_error: Mapped[str | None] = mapped_column(String, nullable=True)
    current_status: Mapped[str] = mapped_column(String, default="idle")

class SystemConfig(Base):
    """Model for dynamic system configuration."""
    __tablename__ = "system_config"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    sync_interval_minutes: Mapped[int] = mapped_column(Integer, default=30)
