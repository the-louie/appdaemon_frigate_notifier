"""Read and write `irisone.*` bus entities — the convention in PROJECT_RULES §4.

Pure: no AppDaemon import, no clock of its own. Callers pass what they already
read (`get_state(entity, attribute="all")`, or `get_state("irisone")` for the
whole domain) and the current epoch. That keeps this usable from async apps,
where `get_state` returns a coroutine (battery_checker), and makes every rule
testable without a stub.

The one rule that matters: **anything we cannot trust reads as None.** Missing,
`unknown`, `unavailable`, empty, undated, naive-dated, older than `max_age_s`,
or dated in the future beyond a small skew: all None. A bus entity silently
read as false is how a gate drops a message it should have sent.

Canonical copy: `appdaemon_irisone/irisone_bus.py`. Consumers carry byte-identical
copies, guarded by tests/test_policy_copies_match.py.
"""
from datetime import datetime

DOMAIN = "irisone"
DEFAULT_MAX_AGE_S = 900  # 3 x the 300 s publisher heartbeat
FUTURE_SKEW_S = 60  # clock skew tolerated between publisher and reader

# Same set as ha_states.HA_UNAVAILABLE_STATES (a test pins them equal). Kept as a
# literal so a consumer needs one module for the bus, not two.
_NOT_REPORTING = frozenset({"unavailable", "unknown", "none", ""})


def build_attributes(source: str, now: datetime, **facts) -> dict:
    """Attributes for a bus write: `updated`, `source`, then the facts.

    Raises ValueError on a naive `now`: an undated-zone timestamp would read as
    stale or fresh depending on the reader's zone, which is a silent bug.
    """
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("irisone attributes need a tz-aware time; use self.get_now()")
    if not source:
        raise ValueError("irisone attributes need a source")
    return {"updated": now.isoformat(), "source": source, **facts}


def _fresh(state: dict | None, now_epoch: float, max_age_s: float) -> bool:
    if not isinstance(state, dict):
        return False
    value = state.get("state")
    if value is None or str(value).strip().lower() in _NOT_REPORTING:
        return False
    updated = (state.get("attributes") or {}).get("updated")
    if not isinstance(updated, str):
        return False
    try:
        when = datetime.fromisoformat(updated)
    except ValueError:
        return False
    if when.tzinfo is None or when.utcoffset() is None:
        return False
    age = now_epoch - when.timestamp()
    if age >= max_age_s:
        return False
    return age >= -FUTURE_SKEW_S


def read_fact(state: dict | None, now_epoch: float,
              max_age_s: float = DEFAULT_MAX_AGE_S) -> str | None:
    """The entity's state string, or None if it cannot be trusted (see module doc).

    `state` is `get_state(entity_id, attribute="all")`. The boundary is exclusive:
    exactly `max_age_s` old is already stale.
    """
    if not _fresh(state, now_epoch, max_age_s):
        return None
    return str(state["state"])


def read_attr(state: dict | None, name: str, now_epoch: float,
              max_age_s: float = DEFAULT_MAX_AGE_S):
    """`attributes[name]` under the same freshness rules, else None."""
    if not _fresh(state, now_epoch, max_age_s):
        return None
    return state["attributes"].get(name)


def normalise_target(target: str) -> str:
    """`notify/mobile_app_x`, `notify.mobile_app_x` and `mobile_app_x` are one target."""
    target = str(target).strip()
    for prefix in ("notify/", "notify."):
        if target.startswith(prefix):
            return target[len(prefix):]
    return target


def presence_for_target(domain_states: dict | None, target: str, now_epoch: float,
                        max_age_s: float = DEFAULT_MAX_AGE_S) -> tuple:
    """(in_home, since) for the person behind notify `target`.

    `in_home` is True / False / None (cannot tell). `since` is the presence
    entity's last transition time, which callers pass to
    notification_policy.drop_log_due so a drop is logged once per away episode.
    `domain_states` is `get_state("irisone")`: {entity_id: state dict}. The person is
    found through the `notify_targets` attribute that PresencePublisher writes on each
    `irisone.presence_<key>`. (None, None) when no fresh entity lists the target, when
    two do (a config error that must not be guessed at), or when `in_home` is not a bool.
    """
    if not isinstance(domain_states, dict):
        return None, None
    want = normalise_target(target)
    found = []
    for entity_id, state in domain_states.items():
        if not entity_id.startswith(f"{DOMAIN}.presence_") or entity_id.endswith("_anyone_home"):
            continue
        targets = read_attr(state, "notify_targets", now_epoch, max_age_s)
        if isinstance(targets, list) and want in {normalise_target(t) for t in targets}:
            found.append(state)
    if len(found) != 1:
        return None, None
    attrs = found[0]["attributes"]
    in_home = attrs.get("in_home")
    if not isinstance(in_home, bool):
        return None, None
    return in_home, attrs.get("since")


def in_home_for_target(domain_states: dict | None, target: str, now_epoch: float,
                       max_age_s: float = DEFAULT_MAX_AGE_S) -> bool | None:
    """Is the person behind notify `target` home? See presence_for_target."""
    return presence_for_target(domain_states, target, now_epoch, max_age_s)[0]


# --- Gate outcome reporting (T-66a A-66.9) ---------------------------------
# Every consumer that calls notification_policy.should_notify reports the
# outcome with `self.fire_event(GATE_EVENT, **gate_event(...))`. The irisone
# housekeeping app counts them into irisone.notify_gate_stats, which is the
# T-69 week-of-evidence source (IQ-28). An event keeps consumers decoupled: if
# the housekeeping app is down, nothing in the consumer changes.

GATE_EVENT = "irisone_gate"


def gate_event(app: str, key: str, target: str, urgency: str, send: bool, reason: str) -> dict:
    """The event payload for one gate decision."""
    return {"app": str(app), "key": str(key), "target": normalise_target(target),
            "urgency": str(urgency), "send": bool(send), "reason": str(reason)}
