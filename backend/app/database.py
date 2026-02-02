from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.core.config import settings
from app.models.database import Base

# Get database URL and fix for SQLAlchemy compatibility
database_url = settings.database_url

# Railway uses postgres:// but SQLAlchemy needs postgresql://
if database_url.startswith("postgres://"):
    database_url = database_url.replace("postgres://", "postgresql://", 1)

# Create database engine
connect_args = {}
if database_url.startswith("sqlite"):
    # SQLite needs check_same_thread=False
    connect_args["check_same_thread"] = False
elif database_url.startswith("postgresql"):
    # Railway PostgreSQL needs SSL
    connect_args["sslmode"] = "require"

engine = create_engine(database_url, connect_args=connect_args, pool_pre_ping=True)

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create tables
def create_tables():
    Base.metadata.create_all(bind=engine)

# Dependency to get database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
