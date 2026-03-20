import hashlib
import random
from datetime import datetime, timezone

import numpy as np


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def percentile(values, q):
    if not values:
        return 0.0
    return float(np.percentile(np.asarray(values, dtype=float), q * 100))


def jitter_delay(mean):
    return random.expovariate(1 / mean)


def parse_socket_list(raw):
    if not raw:
        return []
    return [x.strip() for x in raw.split(",") if x.strip()]
