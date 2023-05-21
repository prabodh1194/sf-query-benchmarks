from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from typing import Sequence

from snowflake.connector.cursor import SnowflakeCursor

from snowflake_connection import con

logger = logging.getLogger(__name__)


class PivotQueryInterface:
    @staticmethod
    def get_unique_events(*, cur: SnowflakeCursor) -> Sequence[str]:
        cur.execute("select distinct event_name from events_data")
        return [f"'{row[0]}'" for row in cur.fetchall()]

    @contextmanager
    def cursor(self):
        with con.cursor() as cur:
            cur.execute("alter session set use_cached_result = false")
            cur.execute("USE DATABASE test")
            yield cur

    def get_pivot_query(self) -> None:
        ...

    def main(self):
        start = time.perf_counter_ns()
        self.get_pivot_query()
        end = time.perf_counter_ns()

        print(f"Query : {self.__class__.__name__}")
        print(f"Time taken: {(end - start) / 1e9} seconds")


class SuboptimalPivotQuery(PivotQueryInterface):
    def get_pivot_query(self):
        with self.cursor() as cur:
            cur.execute(f"""
            select * from (
                select user_id, user_id as users, event_name from events_data
            ) pivot(
                count(users)
                for event_name in ({','.join(self.get_unique_events(cur=cur))})
            )
            """)


class OptimalPivotQuery(PivotQueryInterface):
    def get_batched_pivot_query(self, cur, idx, all_events):
        sum_queries = [
            f'sum(iff(event_name = {event}, 1, 0)) as "{event}"' for event in all_events
        ]

        cur.execute(f"""
            create or replace transient table agg_events as
            select * from agg_events a full outer join (select user_id, {','.join(sum_queries)} from events_data group by user_id) b using(user_id)
        """)

        return cur.sfqid

    def get_pivot_query(self):
        with self.cursor() as cur:
            cur.execute("create or replace transient table agg_events as select distinct user_id from events_data")

            all_events = self.get_unique_events(cur=cur)

            # break into 10 chunks
            chunk_size = len(all_events) // 10
            chunks = [all_events[i:i + chunk_size] for i in range(0, len(all_events), chunk_size)]

            for chunk in enumerate(chunks):
                self.get_batched_pivot_query(cur, *chunk)


if __name__ == '__main__':
    OptimalPivotQuery().main()
    SuboptimalPivotQuery().main()
