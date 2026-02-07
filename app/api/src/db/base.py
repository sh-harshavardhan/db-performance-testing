"""This module is responsible for setting up the database connection and session management using SQLAlchemy."""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.v1.config import settings

engine = create_engine(**settings.DB_CONN_URL)
engine.dialect.supports_sane_rowcount = False
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
