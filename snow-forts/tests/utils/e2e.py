import functools
import pytest
import logging

logger = logging.getLogger(__name__)


def e2e_only(func):
    """Decorator to mark functions that should only run in e2e tests"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if not pytest.config.getoption('--e2e'):
            pytest.skip("This test requires --e2e flag to run")
        return func(*args, **kwargs)
    return wrapper


def e2e_assert(condition, message):
    """Assertion that only runs in e2e tests"""
    if not pytest.config.getoption('--e2e'):
        logger.debug(f"Skipping e2e assertion: {message}")
        return True
    assert condition, message
