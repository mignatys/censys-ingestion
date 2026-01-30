"""Database configuration and session management."""
import os
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

# Default to a local connection string if env var not set
# (useful for local dev/testing without docker)
DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "postgresql+asyncpg://user:password@localhost/dbname"
)

engine = create_async_engine(DATABASE_URL, echo=False)

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False
)

async def get_db():
    """Dependency to provide an async database session."""
    async with AsyncSessionLocal() as session:
        yield session

# --- DATA ACCESS LAYER ---

from datetime import datetime, timezone, timedelta
from typing import List, Optional, Any
from sqlalchemy import select, update, delete, func, text
from sqlalchemy.dialects.postgresql import insert
from ingestion_service.models import SystemConfig, ServiceState, Alert

# --- Configuration ---

async def get_system_config(session: AsyncSession) -> Optional[SystemConfig]:
    """Get the current system configuration."""
    result = await session.execute(select(SystemConfig).limit(1))
    return result.scalars().first()

async def ensure_system_config(session: AsyncSession, default_interval: int = 30) -> SystemConfig:
    """Get config or create default if missing."""
    config = await get_system_config(session)
    if not config:
        config = SystemConfig(sync_interval_minutes=default_interval)
        session.add(config)
        await session.flush()
    return config

async def update_system_config(session: AsyncSession, sync_interval_minutes: int) -> SystemConfig:
    """Update system configuration."""
    config = await ensure_system_config(session)
    config.sync_interval_minutes = sync_interval_minutes
    # session.commit() is left to the caller usually, but for single-purpose API we can commit
    # However, to compose, we should probably return the object and let caller commit?
    # User asked for "API for them... operations we are doing all the time".
    await session.commit()
    await session.refresh(config)
    return config

# --- Service State ---

async def get_service_state(session: AsyncSession) -> Optional[ServiceState]:
    """Get the current service state."""
    result = await session.execute(select(ServiceState).limit(1))
    return result.scalars().first()

async def ensure_service_state(session: AsyncSession) -> ServiceState:
    """Get state or create default if missing."""
    state = await get_service_state(session)
    if not state:
        state = ServiceState(
            last_sync_time=datetime.now(timezone.utc),
            last_event_time=datetime.now(timezone.utc) - timedelta(hours=24),
            current_status="idle"
        )
        session.add(state)
        await session.flush()
    return state

async def update_service_state(session: AsyncSession, **kwargs):
    """Update service state fields. kwargs match ServiceState columns."""
    # This updates all rows, effectively singleton
    await session.execute(update(ServiceState).values(**kwargs))

# --- Alerts ---

async def delete_old_alerts(session: AsyncSession, cutoff: datetime):
    """Delete alerts ingested before cutoff."""
    stmt = delete(Alert).where(Alert.ingested_at < cutoff)
    await session.execute(stmt)

async def insert_alerts_batch(session: AsyncSession, alerts: List[dict]):
    """Bulk insert alerts, ignoring duplicates on ID."""
    if not alerts:
        return
    stmt = insert(Alert)
    do_nothing_stmt = stmt.on_conflict_do_nothing(index_elements=['id'])
    await session.execute(do_nothing_stmt, alerts)

async def get_alerts_filtered(
    session: AsyncSession, 
    limit: int = 100, 
    hours: int = 1,
    user: Optional[str] = None,
    country: Optional[str] = None,
    criticality: Optional[str] = None
) -> List[Alert]:
    """Get alerts with filters."""
    query = select(Alert).limit(limit).order_by(Alert.ingested_at.desc())
    
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    query = query.where(Alert.ingested_at >= cutoff)

    if user:
        query = query.where(Alert.user == user)
    if country:
        query = query.where(Alert.country == country)
    if criticality:
        query = query.where(Alert.severity == criticality.lower())
        
    result = await session.execute(query)
    return result.scalars().all()
