#
#  Copyright 2025 The InfiniFlow Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

"""Custom monkey-patches for ``rag.utils.redis_conn``.

This module injects GraphRAG-specific behaviour into the official
``RedisDB`` and ``RedisDistributedLock`` classes without modifying
``redis_conn.py`` itself:

* ``RedisDB.ttl(k)`` – query the remaining TTL of a key.
* ``RedisDistributedLock.spin_acquire(stop_event)`` – honour an
  ``asyncio.Event`` so that long lock waits can be cancelled cleanly.

Importing this module (e.g. from ``rag.graphrag.config``) is sufficient to
apply the patches; no call sites need to be changed.
"""

import asyncio
import logging

# Ensure ``common.settings`` is fully loaded first; it bootstraps the
# underlying Redis connection and avoids a partial-initialization circular
# import when this patch module is imported before settings.
from common import settings  # noqa: F401
from rag.utils.redis_conn import REDIS_CONN, RedisDB, RedisDistributedLock


def _redis_db_ttl(self, k):
    """Return the remaining TTL (in seconds) for ``k``.

    Return values (matches the redis-py / Valkey spec):
      * ``-2`` — key does not exist.
      * ``-1`` — key exists but has no associated expire.
      * ``>= 0`` — remaining time-to-live in seconds.

    Notes
    -----
    Returns ``None`` if the Redis connection is unavailable. The caller
    MUST be prepared to handle three distinct integer sentinels and the
    ``None`` fallback — do NOT treat ``None`` the same as ``-2``.
    """
    if not self.REDIS:
        return None
    try:
        return self.REDIS.ttl(k)
    except Exception as e:
        logging.warning("RedisDB.ttl " + str(k) + " got exception: " + str(e))
        self.__open__()


async def _redis_distributed_lock_spin_acquire(self, stop_event=None):
    REDIS_CONN.delete_if_equal(self.lock_key, self.lock_value)
    while True:
        if stop_event is not None and stop_event.is_set():
            raise asyncio.CancelledError(
                f"spin_acquire aborted by stop_event for {self.lock_key}"
            )
        if self.lock.acquire(token=self.lock_value):
            break
        await asyncio.sleep(10)


RedisDB.ttl = _redis_db_ttl
RedisDistributedLock.spin_acquire = _redis_distributed_lock_spin_acquire
