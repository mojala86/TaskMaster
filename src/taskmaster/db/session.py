from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine


class Database:
    """Owns the async engine and session factory for one process."""

    def __init__(self, url: str, *, pool_size: int = 10, connect_timeout: int = 5) -> None:
        self.engine = create_async_engine(
            url,
            pool_pre_ping=True,
            pool_size=pool_size,
            # Fail fast when Postgres is unreachable; the API maps that to 503.
            pool_timeout=10,
            connect_args={"connect_timeout": connect_timeout},
        )
        self.sessionmaker = async_sessionmaker(self.engine, expire_on_commit=False)

    def session(self) -> AsyncSession:
        return self.sessionmaker()

    async def dispose(self) -> None:
        await self.engine.dispose()
