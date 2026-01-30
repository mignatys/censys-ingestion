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

from ingestion_service.database import (
    get_db, engine, AsyncSessionLocal,
    get_system_config, ensure_system_config, update_system_config,
    get_service_state, delete_old_alerts, get_alerts_filtered
)
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
                 await delete_old_alerts(session, cutoff)
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
            config = await ensure_system_config(session, default_interval=30)
            
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
        state = await get_service_state(db)
        
        response["db_status"] = "Connected"
        
        if state:
            response["last_sync"] = state.last_sync_time
            response["last_success"] = state.last_success_time
            response["last_event_time"] = state.last_event_time
            response["last_error"] = state.last_error
            
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
    config = await ensure_system_config(db)
    return {
        "sync_interval_minutes": config.sync_interval_minutes
    }

@app.post("/config")
async def update_config(update: ConfigUpdate, db: AsyncSession = Depends(get_db)):
    """Update and apply system configuration."""
    # Upsert
    config = await update_system_config(db, update.sync_interval_minutes)
    
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
    alerts = await get_alerts_filtered(
        db, 
        limit=limit, 
        hours=hours, 
        user=user, 
        country=country, 
        criticality=criticality
    )
    
    # Helper to convert ORM objects to Pydantic (AlertEnriched expects 'id' but model has 'alert_id')
    # Use list comprehension with mapping
    return [AlertEnriched.model_validate(a) for a in alerts]
