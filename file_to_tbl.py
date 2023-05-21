from __future__ import annotations

import logging

from snowflake_connection import get_connector

logger = logging.getLogger(__name__)

with get_connector().cursor() as cur:
    cur.execute("CREATE DATABASE IF NOT EXISTS test")
    cur.execute("USE DATABASE test")
    cur.execute("CREATE STAGE IF NOT EXISTS test")
    cur.execute("PUT file://merge.parquet @test")
    cur.execute("create table if not exists events_data (event_name string, timestamp timestamp, user_id string)")
    cur.execute("COPY INTO events_data FROM @test/merge.parquet "
                "FILE_FORMAT = (TYPE = 'parquet') "
                "MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE';")
