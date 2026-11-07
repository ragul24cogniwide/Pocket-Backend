"""
Who is who: one rule for deciding whether two ids / phone numbers belong to the same account.

Before, ids were compared by their last 10 digits, so +1 415 555 1234 and +91 41555 51234 counted
as the same person: each could see the other's chat history and receive their live messages.

Now every phone number is reduced to one full international form (E.164, "+<country><number>")
and only exact matches count. The rules are the app's own (contactSync.normalizePhone), with
India (+91) as the default country, as everywhere else in Flick:

    "+91 98765 43210", "919876543210", "9876543210", "09876543210", "0091 98765 43210"
        -> "+919876543210"
    "+1 415 555 1234", "14155551234"  -> "+14155551234"

So every spelling an older app version may have stored for an Indian number still matches that
number (old chats keep working), but a number from another country is never mixed up with it.

Ids that aren't phone numbers ("ai_assistant", "grp_...", demo ids like "1") are compared exactly.
"""

import re
from typing import Iterable, Optional

DEFAULT_COUNTRY_CODE = "91"

# Only digits, an optional leading "+", and the usual separators people type
_PHONE_LIKE = re.compile(r"^\+?[0-9][0-9 ()\-.]*$")


def canonical_id(value: Optional[str]) -> str:
    """The single form an id is compared in: E.164 for phone numbers, otherwise the id itself."""
    text = str(value or "").strip()
    if not text or not _PHONE_LIKE.match(text):
        return text
    digits = "".join(ch for ch in text if ch.isdigit())
    if text.startswith("+"):
        return f"+{digits}" if len(digits) >= 8 else text
    if digits.startswith("00"):
        digits = digits[2:]  # international prefix: 0091 98765 43210
    elif len(digits) == 11 and digits.startswith("0"):
        digits = DEFAULT_COUNTRY_CODE + digits[1:]  # trunk 0: 098765 43210
    elif len(digits) == 10:
        digits = DEFAULT_COUNTRY_CODE + digits
    return f"+{digits}" if len(digits) >= 11 else text


def is_phone_id(value: Optional[str]) -> bool:
    c = canonical_id(value)
    return c.startswith("+") and c[1:].isdigit()


def _spellings(canonical: str) -> set[str]:
    """
    Every spelling that reduces to `canonical` (and so belongs to that number only).
    Candidates are checked with canonical_id itself, so two different numbers can never share
    a spelling: e.g. the bare 10 digits belong to the +91 number, never to a +1 number.
    """
    if not is_phone_id(canonical):
        return {canonical} if canonical else set()
    digits = canonical[1:]
    candidates = {canonical, digits, "00" + digits}
    if digits.startswith(DEFAULT_COUNTRY_CODE):
        national = digits[len(DEFAULT_COUNTRY_CODE):]
        candidates.update({national, "0" + national})
    return {s for s in candidates if canonical_id(s) == canonical}


def id_forms(*ids: Optional[str]) -> set[str]:
    """
    Every stored spelling that refers to the same account as these ids: each id exactly as given,
    plus the spellings of its E.164 form. Use with SQL `IN (...)`; never match by suffix.
    """
    forms: set[str] = set()
    for value in ids:
        raw = str(value or "").strip()
        if not raw:
            continue
        forms.add(raw)
        forms.update(_spellings(canonical_id(raw)))
    return forms


def ids_forms(ids: Iterable[Optional[str]]) -> set[str]:
    return id_forms(*list(ids))


def same_identity(a: Optional[str], b: Optional[str]) -> bool:
    """True when two ids / phone numbers refer to the same account."""
    ca, cb = canonical_id(a), canonical_id(b)
    return bool(ca) and ca == cb
