"""Is this JPEG complete? Structural checks, no dependencies.

Replaces `MIN_FILE_SIZE_BYTES = 50000`, which was a five-minute patch for a real
problem and had outlived it.

**The problem it was patching.** An early version of the notifier triggered
before Frigate had finished writing, loaded a half-written file, and put a
corrupt image in front of the household. A hard size floor made that stop.

**Why the floor had to go.** A size threshold cannot tell a small valid image
from a truncated one. Measured 2026-09-05, it was rejecting six snapshots a day
at 19–44 KB, and all six were complete: SOI present, EOI present, Content-Length
matching the body exactly. `doorbell_cam` detects at **640×360**, where a JPEG
lands at 20–45 KB, while every other camera runs 1280×720 and clears 50 KB by
two to five times. So in practice the floor was a doorbell-person-image filter —
on the one camera in the house where the picture matters most.

**Why this is not merely a tidy-up.** Notifying on `new` instead of `end`
reintroduces exactly the race the floor was patching. EOI is the precise version
of the check the floor approximated, so this module is what makes notifying
early safe.

**No new dependencies, deliberately.** Pillow would mean a `requirements.txt`
under `/conf`, and AppDaemon pip-installs every one it finds at boot — on
Alpine/musl that cost 75 seconds and failed anyway (H-23). Everything here is
stdlib and runs in microseconds on a 40 KB buffer.
"""

SOI = b"\xff\xd8"
EOI = b"\xff\xd9"

# How far back to look for the end marker. Some encoders leave padding after
# EOI, so requiring it to be the final two bytes rejects valid files.
EOI_TAIL = 32

# Start Of Frame markers that carry dimensions. C4 (DHT), C8 (JPG) and CC (DAC)
# sit in the same range and do not, which is why this is a set and not a range.
SOF_MARKERS = frozenset({0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7,
                         0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF})

OK = "ok"
EMPTY = "empty"
NOT_JPEG = "not a JPEG"
TRUNCATED = "truncated"
SHORT_READ = "short read"


def check(content, content_length=None):
    """Return `(ok: bool, reason: str)` for a downloaded JPEG body.

    `content_length` is the value of the HTTP header, when there was one. A body
    shorter than the header promised is a transport failure and worth retrying —
    unlike everything else here, which is deterministic and is not.
    """
    if not content:
        return False, EMPTY

    if content_length is not None:
        try:
            expected = int(content_length)
        except (TypeError, ValueError):
            expected = None
        if expected is not None and len(content) != expected:
            return False, "{} ({} of {} bytes)".format(
                SHORT_READ, len(content), expected)

    if not content.startswith(SOI):
        # An error page, a JSON body, an HTML redirect. Not a small image --
        # a different thing entirely, and the size floor could not tell them apart.
        return False, NOT_JPEG

    if EOI not in content[-EOI_TAIL:]:
        return False, TRUNCATED

    return True, OK


def is_transient(reason):
    """True when re-fetching could plausibly produce a different answer.

    This is the distinction the old retry ladder lacked. It treated every
    failure identically, so a *size* rejection got three fetches of identical
    bytes plus ~6 s of backoff to reach the identical answer -- which is where
    one bad snapshot turning into three ERROR lines came from.

    Truncation and a short read are worth retrying: the writer may since have
    finished. "Not a JPEG" and "empty" are not -- the endpoint returned what it
    meant to, and asking again is just noise with a delay attached.
    """
    return reason == TRUNCATED or reason.startswith(SHORT_READ)


def dimensions(content):
    """`(width, height)` from the first SOF marker, or None.

    Cheap, and it is how a camera silently changing resolution becomes visible:
    `doorbell_cam` moving to 640×360 is what pushed its snapshots under the old
    size floor, and nothing noticed for days because nothing was looking at the
    pictures' shape.

    Walks the marker chain rather than scanning for a byte pattern, because
    `FF C0` occurs in entropy-coded data often enough to make scanning wrong.
    """
    if not content or not content.startswith(SOI):
        return None
    i = 2
    n = len(content)
    while i + 3 < n:
        if content[i] != 0xFF:
            i += 1
            continue
        marker = content[i + 1]
        # Padding fill bytes, and the standalone markers that carry no length.
        if marker == 0xFF:
            i += 1
            continue
        if marker in (0xD8, 0xD9) or 0xD0 <= marker <= 0xD7:
            i += 2
            continue
        if i + 3 >= n:
            return None
        length = int.from_bytes(content[i + 2:i + 4], "big")
        if length < 2:
            return None
        if marker in SOF_MARKERS:
            if i + 9 > n:
                return None
            height = int.from_bytes(content[i + 5:i + 7], "big")
            width = int.from_bytes(content[i + 7:i + 9], "big")
            return width, height
        if marker == 0xDA:      # start of scan: entropy data follows, stop.
            return None
        i += 2 + length
    return None
