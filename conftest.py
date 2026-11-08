"""
Test setup: every test runs against a throwaway SQLite database, never the one in .env
(that is the production database). Set TEST_DATABASE_URL to use another test database.

These variables are set before any app module is imported; load_dotenv() never overrides
variables that are already set, so .env can't switch the tests back to production.
"""

import os
import tempfile

_db_file = os.path.join(tempfile.mkdtemp(prefix="flick-tests-"), "test.db")
os.environ["DATABASE_URL"] = os.environ.get("TEST_DATABASE_URL") or f"sqlite+aiosqlite:///{_db_file}?timeout=30"
os.environ["REDIS_URL"] = "redis://127.0.0.1:1/0"  # unreachable: the app falls back to in-memory
os.environ["SEED_DEMO_USERS"] = "false"
os.environ.setdefault("JWT_SECRET_KEY", "test-secret-not-used-in-production")
os.environ["RENDER_EXTERNAL_URL"] = "http://127.0.0.1:1"  # the keep-alive self-ping must not reach the real server
