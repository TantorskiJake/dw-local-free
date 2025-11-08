"""
Pytest configuration and shared fixtures.
"""

import pytest
import os


@pytest.fixture(autouse=True)
def skip_db_tests_if_requested():
    """Skip database tests if SKIP_DB_TESTS is set."""
    if os.getenv("SKIP_DB_TESTS") == "true":
        pytest.skip("Database tests skipped via SKIP_DB_TESTS environment variable")

