import random
import uuid
from datetime import datetime, timedelta, time
from typing import Callable

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import randomname
import ray
from ray.data.random_access_dataset import RandomAccessDataset

import constants

ray.init()


def generate_strings(name, generator: Callable[..., str], size: int) -> RandomAccessDataset:
    ds = ray.data.range_table(size)
    ds = ds.add_column(name, lambda b: pd.Series([generator() for _ in range(len(b))]))

    return ds.to_random_access_dataset(key="value", num_workers=constants.NUM_CPUS)


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


user_ids = generate_strings("user_id", lambda: str(uuid.uuid4()), constants.NUM_USERS)
event_names = generate_strings("event_name", lambda: randomname.get_name(sep=""), constants.NUM_EVENT_NAMES)


@ray.remote
def generate_data(size: int) -> pa.Table:
    e = []
    t = []
    u = []

    # Generate `size` rows of data
    for i in range(size):
        ev_fetch = -1 + random.randint(1, constants.NUM_EVENT_NAMES)
        u_fetch = -1 + random.randint(1, constants.NUM_USERS)

        e.append((ray.get(event_names.get_async(ev_fetch)) or {}).get("event_name", randomname.get_name(sep="")))
        t.append(random_datetime())
        u.append((ray.get(user_ids.get_async(u_fetch)) or {}).get("user_id", str(uuid.uuid4())))

    columns = ["event_name", "timestamp", "user_id"]
    return pa.table([e, t, u], names=columns)


# Generate data in parallel
tasks = [generate_data.remote(constants.NUM_EVENTS // constants.NUM_CPUS) for _ in range(constants.NUM_CPUS)]

# Combine the results into a single PyArrow table
tables = ray.get(tasks)
table = pa.concat_tables(tables)

# table to compressed parquet
pq.write_table(table, "data.parquet", compression="snappy")
