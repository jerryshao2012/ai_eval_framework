"""Shared pytest configuration for the test suite."""
from __future__ import annotations

import pytest


# Make pytest-asyncio automatically handle async test functions without
# requiring @pytest.mark.asyncio on every test.
def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "asyncio: mark test as async (handled by pytest-asyncio)",
    )
