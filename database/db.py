import os

from sqlalchemy import create_engine, event
from sqlalchemy.orm import DeclarativeBase, scoped_session, sessionmaker
from sqlalchemy.pool import QueuePool

DATABASE_URL = os.getenv("DATABASE_URL")

connect_args = {
    "connect_timeout": 10,
}

if DATABASE_URL and "sslmode=" not in DATABASE_URL and "localhost" not in DATABASE_URL and "127.0.0.1" not in DATABASE_URL:
    connect_args["sslmode"] = "require"

engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
    pool_recycle=300,
    pool_pre_ping=True,
    connect_args=connect_args,
)


@event.listens_for(engine, "connect")
def _set_statement_timeout(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("SET statement_timeout TO 30000")
    cursor.close()

Session = scoped_session(
    sessionmaker(bind=engine, autocommit=False, autoflush=False)
)


class Base(DeclarativeBase):
    pass


Base.query = Session.query_property()
