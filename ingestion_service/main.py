import os
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import List, Optional

"""Main FastAPI application entry point."""

import httpx
from fastapi import FastAPI, BackgroundTasks, HTTPException, Query, Depends
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy import select, text, delete
from sqlalchemy.ext.asyncio import AsyncSession

from ingestion_service.database import get_db, engine, AsyncSessionLocal
from ingestion_service.models import ServiceState, Alert, SystemConfig, Base
from ingestion_service.schemas import AlertEnriched, ConfigUpdate
from ingestion_service.ingestor import AlertIngestor, IngestionStatus

from pydantic import BaseModel

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Config
UPSTREAM_URL = os.getenv("UPSTREAM_URL", "http://localhost:8000")

# Scheduler
scheduler = AsyncIOScheduler()
ingestor = AlertIngestor(upstream_url=UPSTREAM_URL)

async def run_sync_task(source: str = "scheduled"):
    """Unified wrapper for running sync tasks (scheduled or manual)."""
    logger.info(f"Running sync task... Source: {source}")
    try:
        # Simple Retention Policy: Clean up before sync
        async with AsyncSessionLocal() as session:
             async with session.begin():
                 cutoff = datetime.now(timezone.utc) - timedelta(hours=48)
                 stmt = delete(Alert).where(Alert.ingested_at < cutoff)
                 await session.execute(stmt)
                 logger.info(f"Pre-sync cleanup: Removed alerts older than 48h ({cutoff})")

        await ingestor.sync()
    except Exception as e:
        logger.error(f"Sync task failed (Source: {source}): {e}")



async def apply_config(config: SystemConfig):
    """Sync config to Scheduler and Mock API."""
    # 1. Update Scheduler
    try:
        job = scheduler.get_job("sync_job")
        if job:
             scheduler.reschedule_job("sync_job", trigger="interval", minutes=config.sync_interval_minutes)
        else:
             scheduler.add_job(run_sync_task, "interval", minutes=config.sync_interval_minutes, id="sync_job", args=["scheduled"])
    except Exception as e:
        logger.error(f"Scheduler update error: {e}")



@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting Service...")
    
    # Initialize DB
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    # Load and Apply Config
    async with AsyncSessionLocal() as session:
        async with session.begin():
            res = await session.execute(select(SystemConfig).limit(1))
            config = res.scalars().first()
            if not config:
                logger.info("No config found, creating default.")
                config = SystemConfig(sync_interval_minutes=30)
                session.add(config)
                await session.flush()
            
            logger.info(f"Applying Config: Interval={config.sync_interval_minutes}")
            await apply_config(config)

    scheduler.start()
    yield
    scheduler.shutdown()


app = FastAPI(title="Ingestion Service", lifespan=lifespan)

@app.get("/health")
async def health_check(db: AsyncSession = Depends(get_db)):
    """
    Check service health, DB connectivity, and sync status.
    Returns unified `system_status` and `db_status`.
    """
    response = {
        "system_status": ingestor.status, # Default to in-memory status
        "db_status": "Unknown", 
        "last_sync": None,
        "last_success": None
    }
    
    try:
        # Check DB & Sync State in one go
        result = await db.execute(select(ServiceState).limit(1))
        state = result.scalars().first()
        
        response["db_status"] = "Connected"
        
        if state:
            response["last_sync"] = state.last_sync_time
            response["last_success"] = state.last_success_time
            
            # DB State determines "system_status" unless in-memory is 'retrying' (transient visibility)
            if ingestor.status == IngestionStatus.RETRYING:
                response["system_status"] = "retrying"
            else:
                response["system_status"] = state.current_status
        else:
            # DB Connected but no state initialized
            response["system_status"] = "idle"

    except Exception as e:
        response["db_status"] = "Disconnected"
        # If DB is down, we can't be sure of system consistency, flag as failure
        response["system_status"] = "system_failure" 
        logger.error(f"Health check failed: {e}")

    return response



@app.post("/sync")
async def trigger_sync(background_tasks: BackgroundTasks):
    """
    Manually trigger a sync in the background.
    """
    if ingestor.status in [IngestionStatus.SYNCING, IngestionStatus.RETRYING]:
        raise HTTPException(status_code=409, detail=f"Sync already in progress (Status: {ingestor.status})")

    # Set status immediately to block race conditions
    ingestor.status = IngestionStatus.SYNCING
    background_tasks.add_task(run_sync_task, source="manual")
    return {"message": "Sync triggered in background"}



@app.get("/config")
async def get_config(db: AsyncSession = Depends(get_db)):
    """Retrieve current system configuration."""
    result = await db.execute(select(SystemConfig).limit(1))
    config = result.scalars().first()
    if not config:
        config = SystemConfig(sync_interval_minutes=5)
    return {
        "sync_interval_minutes": config.sync_interval_minutes
    }

@app.post("/config")
async def update_config(update: ConfigUpdate, db: AsyncSession = Depends(get_db)):
    """Update and apply system configuration."""
    # Upsert
    result = await db.execute(select(SystemConfig).limit(1))
    config = result.scalars().first()
    if not config:
         config = SystemConfig()
         db.add(config)
    
    config.sync_interval_minutes = update.sync_interval_minutes
    await db.commit()
    await db.refresh(config)
    
    # Apply changes
    await apply_config(config)
    
    return {"message": "Configuration saved and applied", "config": {
        "sync_interval_minutes": config.sync_interval_minutes
    }}



@app.get("/alerts", response_model=List[AlertEnriched])
async def get_alerts(
    user: Optional[str] = Query(None, description="Filter by user"),
    country: Optional[str] = Query(None, description="Filter by country"),
    criticality: Optional[str] = Query(None, description="Filter by severity (criticality)"),
    hours: int = Query(1, description="Filter alerts from the last N hours", ge=1),
    limit: int = 100,
    db: AsyncSession = Depends(get_db)
):
    """
    Retrieve alerts from the database with filtering.
    """
    # Base Query
    query = select(Alert).limit(limit).order_by(Alert.ingested_at.desc())
    
    # Time Filter (Default 1 hour)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    query = query.where(Alert.ingested_at >= cutoff)

    # Optional Filters
    if user:
        query = query.where(Alert.user == user)
    
    if country:
        query = query.where(Alert.country == country)
        
    if criticality:
        # The DB stores "low", "medium", "high", "critical" (lowercase)
        query = query.where(Alert.severity == criticality.lower())
        
    result = await db.execute(query)
    alerts = result.scalars().all()
    
    # Helper to convert ORM objects to Pydantic (AlertEnriched expects 'id' but model has 'alert_id')
    # Use list comprehension with mapping
    return [AlertEnriched.model_validate(a) for a in alerts]
