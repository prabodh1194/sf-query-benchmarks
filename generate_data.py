import logging
import random
import uuid
from datetime import datetime, timedelta, time
from typing import Callable

import pyarrow as pa
import pyarrow.parquet as pq
import randomname
import ray
from ray import ObjectRef

import constants

logger = logging.getLogger(__name__)

ray.init(num_cpus=constants.NUM_EXECUTING_CPUS)


def generate_strings(generator: Callable[..., str], size: int) -> ObjectRef:
    return ray.put([generator() for _ in range(size)])


def random_date(start_date: datetime, end_date: datetime) -> datetime:
    # Generate a random number of days between the start and end dates
    days = random.randint(0, (end_date - start_date).days + 1)

    # Return a date that is `days` days after the start date
    return start_date + timedelta(days=days)


def random_datetime() -> datetime:
    # Generate a random date between 2020-01-01 and today
    date = random_date(datetime(2020, 1, 1), datetime.now())

    # Generate a random time of day
    tme = time(
        hour=random.randint(0, 23),
        minute=random.randint(0, 59),
        second=random.randint(0, 59),
        microsecond=random.randint(0, 999999),
    )

    return datetime.combine(date, tme)


user_ids = generate_strings(lambda: str(uuid.uuid4()), constants.NUM_USERS)
event_names = generate_strings(lambda: randomname.get_name(sep=""), constants.NUM_EVENT_NAMES)


@ray.remote
def generate_data(k: int, size: int) -> None:
    e = []
    t = []
    u = []

    _event_names = ray.get(event_names)
    _user_ids = ray.get(user_ids)

    print(f"Generating %s rows of data %s", size, k)

    # Generate `size` rows of data
    for i in range(size):
        ev_fetch = -1 + random.randint(1, constants.NUM_EVENT_NAMES)
        u_fetch = -1 + random.randint(1, constants.NUM_USERS)

        e.append(_event_names[ev_fetch])
        t.append(random_datetime())
        u.append(_user_ids[u_fetch])

    print(f"Generated %s rows of data %s", size, k)

    columns = ["event_name", "timestamp", "user_id"]
    table = pa.table([e, t, u], names=columns)

    print(f"Writing %s rows of data %s", size, k)

    pq.write_table(table, f"pq/data_{k}.parquet")


k = 0
for _ in range(constants.NUM_CPUS // constants.NUM_EXECUTING_CPUS):
    v = []
    for _ in range(constants.NUM_EXECUTING_CPUS):
        k += 1
        v.append(generate_data.remote(k, constants.NUM_EVENTS // constants.NUM_CPUS))

    ray.get(v)
