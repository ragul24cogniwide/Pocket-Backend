"""Baseline: the schema as it was when migrations were introduced (November 2026).

Revision ID: 0001_baseline
Revises:
Create Date: 2026-11-07

Every table and column that existed then is created by database.init_db's create_all and its
legacy column patches, which still run first on every start. So this revision changes nothing:
on an existing database (production) it only marks the starting point, and every later
schema change gets its own revision after it.
"""
from typing import Sequence, Union

revision: str = "0001_baseline"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
