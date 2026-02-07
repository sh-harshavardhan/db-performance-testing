"""All Databases dependencies are defined here"""

from src.db.base import SessionLocal


def get_db():
    """Provides a database session for the duration of a request."""
    db = SessionLocal()
    try:
        yield db
    except Exception as e:
        db.rollback()
        print(f"Database session rollback due to exception: {e}")
        raise e
    finally:
        db.close()
