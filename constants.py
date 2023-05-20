from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


NUM_GPUS = 1
NUM_CPUS = 1000
NUM_EXECUTING_CPUS = 20

NUM_EVENT_NAMES = 1000
NUM_USERS = 2_000_000
NUM_EVENTS = 10 * NUM_USERS
