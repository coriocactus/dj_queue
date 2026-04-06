import os

import pytest

DB_BACKEND = os.environ.get("DB_BACKEND", "sqlite")


def pytest_collection_modifyitems(items):
    for item in items:
        for marker in ("postgres", "mysql", "mariadb"):
            if marker in item.keywords and DB_BACKEND != marker:
                item.add_marker(pytest.mark.skip(reason=f"requires DB_BACKEND={marker}"))
