"""When to send a repeating alert, and when to stay quiet — policy D2.

Deliberately free of AppDaemon imports and of any clock of its own: `now` is
always passed in. That makes every rule directly testable, including the
midnight-wrapping quiet window, which is the part most likely to be wrong.

The rules, in the order they are applied to one condition:

1. **A condition not seen before is always sent, whatever the hour.** A dead app
   at 23:00 still matters. Quiet hours suppress *nagging*, not *news*.
2. Within the repeat window, stay silent.
3. Past the repeat window but inside quiet hours, stay silent and do not
   consume the repeat — so the alert lands promptly once the window opens
   rather than waiting another full interval.
4. Otherwise, send.

A condition that clears is forgotten, so if it returns it is news again.

**Rule 0, before all of the above: the presence gate (T-71).** Every send
states its urgency, and a NON_URGENT send to a recipient who is not home is
dropped. The gate runs first, per recipient, and a drop never touches D2 state:
if the condition is still true when the person comes home, the app's next
regular check sends it (owner ruling IQ-13, 2026-10-29). `uncommit` exists for
`apply()` callers, which mark a key sent before they know whether anyone was
reached.

State is a plain dict of `{key: last_sent_epoch}`, JSON-serialisable so the
caller can persist it across restarts. That matters: without it a reload resets
every cooldown and re-notifies everything, which is the "redundant restart-time
re-alerts" problem D2 names explicitly.
"""

DEFAULT_QUIET_START = 22  # inclusive, local hour
DEFAULT_QUIET_END = 7  # exclusive, local hour
DEFAULT_REPEAT_AFTER = 6 * 3600  # seconds

SEND_FIRST = "first occurrence"
SEND_REPEAT = "repeat interval elapsed"
HOLD_RECENT = "within repeat interval"
HOLD_QUIET = "quiet hours"


def in_quiet_hours(hour, start=DEFAULT_QUIET_START, end=DEFAULT_QUIET_END):
    """True if `hour` falls in the quiet window, which may wrap past midnight.

    A window like 22->07 wraps; one like 01->06 does not. Getting this wrong in
    the wrapping case is the classic bug, so both forms are handled explicitly.
    """
    if start == end:
        return False
    if start < end:
        return start <= hour < end
    return hour >= start or hour < end


def decide(
    key,
    now_epoch,
    now_hour,
    state,
    quiet_start=DEFAULT_QUIET_START,
    quiet_end=DEFAULT_QUIET_END,
    repeat_after=DEFAULT_REPEAT_AFTER,
):
    """Return (send: bool, reason: str) for one condition. Does not mutate state."""
    last_sent = state.get(key)

    if last_sent is None:
        return True, SEND_FIRST

    if now_epoch - last_sent < repeat_after:
        return False, HOLD_RECENT

    if in_quiet_hours(now_hour, quiet_start, quiet_end):
        return False, HOLD_QUIET

    return True, SEND_REPEAT


def apply(
    keys,
    now_epoch,
    now_hour,
    state,
    quiet_start=DEFAULT_QUIET_START,
    quiet_end=DEFAULT_QUIET_END,
    repeat_after=DEFAULT_REPEAT_AFTER,
):
    """Decide for a whole set of currently-active conditions.

    Returns (to_send, held, new_state). Keys absent from `keys` are dropped from
    the state, so a condition that clears and returns is treated as news.
    """
    to_send, held, new_state = [], [], {}

    for key in sorted(keys):
        send, reason = decide(
            key, now_epoch, now_hour, state,
            quiet_start=quiet_start, quiet_end=quiet_end, repeat_after=repeat_after,
        )
        if send:
            to_send.append((key, reason))
            new_state[key] = now_epoch
        else:
            held.append((key, reason))
            # Preserve the original timestamp: holding must not extend the window.
            new_state[key] = state[key]

    return to_send, held, new_state


# --- Presence gate (T-71; owner decisions 2026-10-29, docs/planning/T-69-interview.md) ---

URGENT = "urgent"
NON_URGENT = "non_urgent"

SEND_URGENT = "urgent"
SEND_URGENT_AWAY = "urgent, recipient away"
SEND_HOME = "recipient home"
SEND_PRESENCE_UNKNOWN = "presence unknown, fail open"
DROP_AWAY = "non-urgent, recipient away"


def should_notify(urgency, in_home):
    """Return (send: bool, reason: str) for one notification to one recipient.

    `urgency` is mandatory and must be URGENT or NON_URGENT (IQ-12): anything
    else raises, so a typo fails loud at the first send instead of quietly
    picking a class. `in_home` is what irisone_bus.in_home_for_target returned:
    True, False, or None when presence is missing, stale or unknown. None sends.
    Only NON_URGENT with in_home False is dropped.
    """
    if urgency not in (URGENT, NON_URGENT):
        raise ValueError(f"urgency must be {URGENT!r} or {NON_URGENT!r}, got {urgency!r}")
    if urgency == URGENT:
        return True, SEND_URGENT_AWAY if in_home is False else SEND_URGENT
    if in_home is None:
        return True, SEND_PRESENCE_UNKNOWN
    if in_home:
        return True, SEND_HOME
    return False, DROP_AWAY


def drop_log_due(key, target, presence_since, logged):
    """True the first time (key, target) is dropped in one away episode.

    `presence_since` is the presence entity's `since` attribute, which changes
    on every published transition, so a new episode logs again. Mutates
    `logged`. This keeps a 60 s re-check loop to one INFO line per away
    episode per notification (O3), not 60 an hour.
    """
    marker = logged.get((key, target))
    if marker is not None and marker == presence_since:
        return False
    logged[(key, target)] = presence_since
    return True


def uncommit(new_state, old_state, key):
    """Undo `apply()`'s mark for `key` when the send reached nobody.

    Restores the previous timestamp, or removes the key if it was new, so a
    dropped send does not start the repeat interval. Returns a new dict.
    """
    out = dict(new_state)
    if key in old_state:
        out[key] = old_state[key]
    else:
        out.pop(key, None)
    return out
