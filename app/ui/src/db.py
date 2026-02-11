"""This module is responsible for setting up the database connection and session management using SQLAlchemy."""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.url import URL

engine = create_engine(**{"url": URL.create(drivername="duckdb", database="//ui-duck.db")})
engine.dialect.supports_sane_rowcount = False
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
