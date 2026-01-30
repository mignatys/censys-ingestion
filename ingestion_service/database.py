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
