"""
Copies photos and voice notes stored in the database to Cloudflare R2.

    python migrate_media_to_r2.py              copy every file to R2 (the database keeps its copy)
    python migrate_media_to_r2.py --free-db    copy, then empty each database copy once R2 has the
                                               file at the right size (frees database storage)
    python migrate_media_to_r2.py --dry-run    only count what would be copied

Safe to run again: files already emptied in the database are skipped, and a file is only emptied
after R2 confirms it has the same size. Needs the R2_* variables (see media_store.py).
"""

import argparse
import asyncio
import sys

from sqlalchemy import func, select, update

import media_store
from database import async_session_factory
from models import MediaFile


async def main(free_db: bool, dry_run: bool) -> int:
    if not media_store.r2_enabled():
        print("R2 is not configured. Set R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY and R2_BUCKET.")
        return 1

    async with async_session_factory() as db:
        rows = (
            await db.execute(
                select(MediaFile.name, MediaFile.kind, MediaFile.size)
                .where(func.length(MediaFile.data) > 0)
                .order_by(MediaFile.created_at)
            )
        ).all()
    total = sum(r.size or 0 for r in rows)
    print(f"{len(rows)} files in the database ({total / 1_000_000:.1f} MB)")
    if dry_run:
        return 0

    copied = freed = failed = 0
    for name, kind, size in rows:
        try:
            async with async_session_factory() as db:
                row = await db.get(MediaFile, name)
                data, content_type = row.data, row.content_type
            if await media_store.r2_size(kind, name) != len(data):
                await media_store.r2_put(kind, name, data, content_type)
            if await media_store.r2_size(kind, name) != len(data):
                raise RuntimeError("R2 size check failed after upload")
            copied += 1
            if free_db:
                async with async_session_factory() as db:
                    await db.execute(update(MediaFile).where(MediaFile.name == name).values(data=b""))
                    await db.commit()
                freed += 1
            print(f"  ok  {kind}/{name} ({len(data)} bytes)")
        except Exception as e:
            failed += 1
            print(f"  FAILED {kind}/{name}: {e}")

    print(f"\nCopied {copied}, emptied in the database {freed}, failed {failed}")
    if copied and not free_db:
        print("The database still has its copies. Run again with --free-db to free that space.")
    return 1 if failed else 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--free-db", action="store_true", help="empty database copies once they are safely in R2")
    parser.add_argument("--dry-run", action="store_true", help="only count the files")
    args = parser.parse_args()
    sys.exit(asyncio.run(main(args.free_db, args.dry_run)))
