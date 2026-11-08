# Database migrations (Alembic)

The schema is changed only through migration files in `migrations/versions/`, never by
editing tables by hand or adding `ALTER TABLE` lines to `database.py`.

The app applies any missing migrations by itself on startup (`database.run_migrations`, called
from `init_db`). On Render that means: deploy, and the new columns / tables are there.

## Changing the schema

1. Change the model in `models.py`.
2. Generate a migration from the difference between the models and your database:

       alembic revision --autogenerate -m "add pinned flag to messages"

   (uses `DATABASE_URL` from `.env`; point it at a local / test database, not production)
3. Read the generated file in `migrations/versions/` and fix what autogenerate can't know
   (renames show up as drop + add; data back-fills must be written by hand).
4. Try it: `alembic upgrade head`, then `alembic downgrade -1` and `alembic upgrade head` again.
5. Commit the model change and the migration together.

## Useful commands

    alembic current          # which revision the database is at
    alembic history          # every revision
    alembic upgrade head --sql   # print the SQL instead of running it (review before a big change)

## The baseline

`0001_baseline` is the schema as it was when migrations were introduced (November 2026). It
changes nothing: those tables and columns are still created by `init_db` (`create_all` plus the
old column patches), which runs before the migrations. An existing database (production) is
simply marked as being at the baseline the first time it starts with this code.
