"""Common Utilities"""

from functools import cache
from pathlib import Path
import time


@cache
def project_path() -> str:
    """Returns the absolute path of the project directory."""
    return str(Path(__file__).parent.parent.parent.parent.parent)


@cache
def performance_path() -> str:
    """Returns the absolute path of the performance directory."""
    return str(Path(Path(__file__).parent.parent.parent.parent, "performance"))


@cache
def api_path() -> str:
    """Returns the absolute path of the api directory."""
    return str(Path(Path(__file__).parent.parent.parent.parent, "api"))


def timer(func):
    """Decorator to log the execution time of a function"""

    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"Execution time for {func.__name__}: {end_time - start_time:.4f} seconds", flush=True)
        return result

    return wrapper
