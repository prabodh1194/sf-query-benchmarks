from __future__ import annotations

import logging

from snowflake import connector

logger = logging.getLogger(__name__)
con = connector.Connect(
    user='',
    password='',
    account='',
)
