"""Core ingestion logic for fetching and processing alerts."""
import random
import string
import logging
import asyncio
import threading
from datetime import datetime, timedelta, timezone
from typing import List, Optional, AsyncGenerator
import httpx
import ijson
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from sqlalchemy import select, update, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError

from ingestion_service.database import (
    AsyncSessionLocal, 
    ensure_service_state, 
    update_service_state, 
    insert_alerts_batch
)
from ingestion_service.models import Alert, ServiceState
from ingestion_service.schemas import AlertBase, AlertEnriched, AlertLegacy
from ingestion_service.normalization import registry

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



from enum import Enum

class IngestionStatus(str, Enum):
    IDLE = "idle"
    SYNCING = "syncing"
    RETRYING = "retrying"
    FAILED = "failed"

def _before_sleep(retry_state):
    """Callback to update status to RETRYING before waiting."""
    try:
        # args[0] is 'self' for instance methods
        ingestor = retry_state.args[0]
        ingestor.status = IngestionStatus.RETRYING
        logger.warning(f"Retrying sync... Attempt {retry_state.attempt_number}")
    except Exception as e:
        logger.error(f"Failed to update retry status: {e}")

class ResponseStream:
    """Helper to adapt httpx bytes iterator to a file-like object for ijson."""
    def __init__(self, iter_bytes):
        self._iter = iter_bytes
        self._buffer = bytearray()

    def read(self, n=None):
        while not self._buffer:
            try:
                chunk = next(self._iter)
                self._buffer.extend(chunk)
            except StopIteration:
                break
        
        if n is None or n < 0:
            # Read all 
            result = bytes(self._buffer)
            self._buffer = bytearray()
            # Consume rest
            for chunk in self._iter:
                result += chunk
            return result
        
        result = bytes(self._buffer[:n])
        del self._buffer[:n] # Efficient atomic delete from start
        return result

class AlertIngestor:
    """Ingestion controller for fetching and processing upstream alerts."""
    def __init__(self, upstream_url: str = "http://localhost:8000"):
        self.upstream_url = upstream_url
        self.status = IngestionStatus.IDLE

    # Streaming logic in a background thread
    async def fetch_alerts(self, since: Optional[datetime] = None) -> AsyncGenerator[dict, None]:
        """
        Fetch alerts from upstream with retries.
        Streams data using ijson in a background thread to allow async consumption.
        """
        params = {}
        if since:
            params["since"] = since.isoformat()
        
        # Queue for passing items from thread to async loop
        queue = asyncio.Queue(maxsize=1000)
        loop = asyncio.get_event_loop()
        
        
        # Exception container to propagate errors from thread
        thread_error = []

        # Start producer thread
        producer_thread = threading.Thread(
            target=self._stream_producer, 
            args=(params, queue, loop, thread_error),
            daemon=True
        )
        producer_thread.start()

        # Consume from queue
        while True:
            batch = await queue.get()
            if batch is None:
                if thread_error:
                    raise thread_error[0]
                break
            
            # Yield raw items
            for item in batch:
                yield item

    def _normalize_item(self, item: dict) -> Optional[AlertBase]:
        """Normalize a dictionary item into an AlertBase object using Adapters."""
        try:
            source = item.get("source")
            if not source:
                logger.error(f"Invalid alert format: missing source field: {item}")
                return None

            adapter = registry.get_adapter(source)
            if not adapter:
                logger.error(f"No adapter found for source: {source}")
                return None
                
            return adapter.normalize(item)
            
        except Exception as e:
            logger.error(f"Failed to normalize item: {item}. Error: {e}")
            return None

    def _stream_producer(self, params, queue, loop, thread_error):
        """Executed in a background thread to fetch and stream alerts."""
        try:
            # Use synchronous client for ijson compatibility
            with httpx.Client() as client:
                # Stream the response
                with client.stream("GET", f"{self.upstream_url}/alerts", params=params) as response:
                    response.raise_for_status()
                    
                    # ijson expects a file-like object with read()
                    # response.iter_bytes() yields chunks
                    f = ResponseStream(response.iter_bytes())
                    
                    # ijson.items yields python objects from the stream
                    batch_payload = []
                    BATCH_SIZE = 100
                    
                    for item in ijson.items(f, 'item'):
                        batch_payload.append(item)
                        if len(batch_payload) >= BATCH_SIZE:
                            asyncio.run_coroutine_threadsafe(queue.put(list(batch_payload)), loop).result()
                            batch_payload.clear()
                    
                    # Flush remaining
                    if batch_payload:
                            asyncio.run_coroutine_threadsafe(queue.put(list(batch_payload)), loop).result()
                        
        except Exception as e:
            logger.error(f"Stream producer error: {e}")
            thread_error.append(e)
        finally:
            # Signal end of stream
            asyncio.run_coroutine_threadsafe(queue.put(None), loop).result()

    def enrich_alert(self, alert: AlertBase) -> AlertEnriched:
        """Enrich a raw alert with country and user data.
           We assume having a local geodb mapping and userdb access here for simplicity.
        """
        # Geo/Country Simulation
        country = random.choice(["US", "DE", "FR", "CN", "RU", "BR", "IN"])
        
        # User Simulation
        user = "user-" + "".join(random.choices(string.ascii_lowercase + string.digits, k=6))

        return AlertEnriched(
            **alert.model_dump(),
            country=country,
            user=user
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=4),
        retry=retry_if_exception_type((httpx.HTTPError, ConnectionError, OSError)),
        reraise=True,
        before_sleep=_before_sleep
    )
    async def sync(self):
        """Execute the sync process with incremental bookmark."""
        self.status = IngestionStatus.SYNCING
        
        async with AsyncSessionLocal() as session:
            # 1. Get last_event_time from ServiceState (Data High Water Mark)
            async with session.begin():
                state = await ensure_service_state(session)
                
                last_event_time = state.last_event_time

                # Update Start State (Mark as running NOW)
                await update_service_state(session, current_status="syncing", last_sync_time=datetime.now(timezone.utc))
            
            logger.info(f"Starting sync from bookmark: {last_event_time}")

            alert_payloads = []
            BATCH_SIZE = 100
            total_synced = 0
            
            # Track high-water mark for this run
            current_batch_high_water = last_event_time

            try:
                # 2. Fetch Alerts (Stream)
                async for raw_item in self.fetch_alerts(since=last_event_time):
                    # 3. Process (Normalize & Enrich)
                    norm = self._normalize_item(raw_item)
                    if not norm:
                        logger.warning(f"Normalization Failed. Skipping invalid item: {raw_item}")
                        continue
                        
                    enriched = self.enrich_alert(norm)
                    
                    alert_dict = enriched.model_dump()
                    alert_dict['ingested_at'] = datetime.now(timezone.utc)
                    alert_payloads.append(alert_dict)
                    
                    # Track high-water mark from stream data for logging
                    if enriched.created_at and (current_batch_high_water is None or enriched.created_at > current_batch_high_water):
                        current_batch_high_water = enriched.created_at

                    # Bulk Insert Batch
                    if len(alert_payloads) >= BATCH_SIZE:
                        # Update DB with new high-water mark
                        await self._process_batch(session, alert_payloads, current_batch_high_water)
                        total_synced += len(alert_payloads)
                        alert_payloads.clear() # Clear memory
                
                # Insert remaining
                if alert_payloads:
                    await self._process_batch(session, alert_payloads, current_batch_high_water)
                    total_synced += len(alert_payloads)
                    alert_payloads.clear()

                # Update Final Success State
                await update_service_state(session, current_status="idle", last_success_time=datetime.now(timezone.utc),
                                           last_sync_time=datetime.now(timezone.utc), last_event_time=current_batch_high_water, last_error=None)
                await session.commit()
                self.status = IngestionStatus.IDLE

            except SQLAlchemyError as e:
                self.status = IngestionStatus.FAILED
                logger.error(f"Database error during sync: {e}")
                # Record Error
                await update_service_state(session, current_status="failed", last_sync_time=datetime.now(timezone.utc), last_error=str(e))
                await session.commit()
                raise  
            except Exception as e:
                self.status = IngestionStatus.FAILED
                logger.error(f"Unexpected error during sync: {e}")
                 # Record Error
                await update_service_state(session, current_status="failed", last_sync_time=datetime.now(timezone.utc), last_error=str(e))
                await session.commit()
                raise
            
            logger.info(f"Synced {total_synced} alerts. Last Data Timestamp: {current_batch_high_water}")

    async def _process_batch(self, session: AsyncSession, alerts: List[dict], last_event_time: datetime):
        """Helper to insert a batch and update state atomically."""
        try:
            # Upsert Alerts
            await insert_alerts_batch(session, alerts)
            await update_service_state(session, current_status="syncing", last_event_time=last_event_time)
            await session.commit()
        except SQLAlchemyError:
            await session.rollback()
            raise
