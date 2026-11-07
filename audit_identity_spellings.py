"""
Read-only check to run BEFORE deploying the identity fix (identity.py).

The old code matched people by the last 10 digits of their number; the new code matches only the
same full number (in any of its usual spellings). This lists every sender / receiver id stored in
`messages` that isn't exactly an account id or phone number, and says which account it belongs
to now, and which it matched before:

  OK        -> same account as before: those messages keep showing exactly as today
  CHANGED   -> belonged to a different account before (the collision this fix removes)
  ORPHANED  -> matched an account before, matches none now: those messages would disappear from
               that person's chat. Tell Claude / fix these before deploying.

It only runs SELECT queries; nothing is written.

    python audit_identity_spellings.py            # uses DATABASE_URL from .env
"""

import asyncio
from collections import Counter

from sqlalchemy import select, union

from database import async_session_factory
from identity import id_forms
from models import Message, User


def _old_match(value: str, users: list[tuple[str, str]]) -> set[str]:
    """Accounts the old last-10-digits rule tied this id to."""
    digits = "".join(ch for ch in value if ch.isdigit())
    last10 = digits[-10:] if len(digits) >= 7 else digits
    out = set()
    for uid, phone in users:
        if value in (uid, phone):
            out.add(uid)
        elif last10 and (str(uid).endswith(last10) or str(phone or "").endswith(last10)):
            out.add(uid)
    return out


def _new_match(value: str, users: list[tuple[str, str]]) -> set[str]:
    forms = id_forms(value)
    return {uid for uid, phone in users if uid in forms or phone in forms}


async def main() -> None:
    async with async_session_factory() as db:
        users = [(u, p) for u, p in (await db.execute(select(User.id, User.phone_number))).all()]
        exact = {x for pair in users for x in pair if x}
        ids_q = union(select(Message.sender_id.label("v")), select(Message.receiver_id.label("v")))
        stored = [v for (v,) in (await db.execute(ids_q)).all() if v]
        counts = Counter()
        for col in (Message.sender_id, Message.receiver_id):
            for v, in (await db.execute(select(col))).all():
                if v and v not in exact:
                    counts[v] += 1

    odd = sorted(v for v in stored if v not in exact)
    print(f"{len(users)} accounts, {len(stored)} distinct ids in messages, {len(odd)} not an exact account id/phone.\n")
    problems = 0
    for v in odd:
        before, now = _old_match(v, users), _new_match(v, users)
        if before == now:
            verdict = "OK"
        elif not now:
            verdict, problems = "ORPHANED", problems + 1
        else:
            verdict, problems = "CHANGED", problems + 1
        print(f"{verdict:9} {v!r:24} rows={counts[v]:<6} before={sorted(before)} now={sorted(now)}")
    print("\nNothing to fix: safe to deploy." if not problems else f"\n{problems} id(s) need a look before deploying.")


if __name__ == "__main__":
    asyncio.run(main())
