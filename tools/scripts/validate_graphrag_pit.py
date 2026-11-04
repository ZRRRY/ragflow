"""Opt-in PIT smoke check against ES using only a fresh, disposable index.

Run in a separate process with ES_HOST, ES_PORT and ELASTIC_PASSWORD set.
No application services are bootstrapped and no existing index is modified.
"""

import asyncio
import importlib
import json
import os
import sys
from contextlib import aclosing
from types import ModuleType, SimpleNamespace
from uuid import uuid4

from elasticsearch import Elasticsearch, NotFoundError


async def validate():
    index_name = f"graphrag-pit-smoke-{uuid4().hex}"
    client = Elasticsearch(
        f"http://{os.environ.get('ES_HOST', 'es01')}:{os.environ.get('ES_PORT', '9200')}",
        basic_auth=(os.environ.get("ES_USER", "elastic"), os.environ["ELASTIC_PASSWORD"]),
        request_timeout=120,
    )
    # Import the real paginator without connecting unrelated application stores.
    settings = ModuleType("common.settings")
    misc = ModuleType("common.misc_utils")
    misc.thread_pool_exec = asyncio.to_thread
    sys.modules["common.settings"] = settings
    sys.modules["common.misc_utils"] = misc
    pagination = importlib.import_module("rag.graphrag.utils_pagination")
    loop = asyncio.get_running_loop()
    renewed = asyncio.Event()

    class TracedClient:
        def __init__(self):
            self.latest_id = None
            self.renewals = 0
            self.closes = 0

        def open_point_in_time(self, **kwargs):
            assert kwargs == {"index": index_name, "keep_alive": "10m"}
            response = client.open_point_in_time(**kwargs)
            self.latest_id = response["id"]
            return response

        def search(self, *, body, **kwargs):
            assert body["pit"] == {"id": self.latest_id, "keep_alive": "10m"}
            assert kwargs == {"allow_partial_search_results": False}
            response = client.search(body=body, **kwargs)
            self.latest_id = response.get("pit_id", self.latest_id)
            if body["size"] == 0:
                assert body["query"] == {"match_none": {}}
                self.renewals += 1
                loop.call_soon_threadsafe(renewed.set)
            return response

        def close_point_in_time(self, *, id):
            assert id == self.latest_id
            response = client.close_point_in_time(id=id)
            assert response["succeeded"] and response["num_freed"] == 2
            self.closes += 1
            return response

    traced = TracedClient()
    settings.docStoreConn = SimpleNamespace(es=traced)
    # Exercise the actual background timer quickly; production remains 60s.
    pagination._PIT_RENEW_INTERVAL = 0.05
    created = False
    try:
        version = client.info()["version"]["number"]
        print(f"Creating temporary index: {index_name}", flush=True)
        # Creation can succeed server-side even if the response times out.
        created = True
        client.indices.create(index=index_name, settings={"number_of_shards": 2, "number_of_replicas": 0}, mappings={
            "properties": {
                "kb_id": {"type": "keyword"}, "knowledge_graph_kwd": {"type": "keyword"},
                "entity_kwd": {"type": "keyword"}, "compile_kwd": {"type": "keyword"},
                "rank_flt": {"type": "float"},
                "q_2_vec": {"type": "dense_vector", "dims": 2, "index": False},
            },
        })
        expected = {str(i) for i in range(12)}
        results = []
        for sort_field in ("entity_kwd", "_shard_doc"):
            operations = []
            for i in range(14):
                row = {"kb_id": "kb" if i != 13 else "other", "knowledge_graph_kwd": "entity",
                       "entity_kwd": f"duplicate-{i % 2}", "rank_flt": 0.0, "q_2_vec": [1.0, 0.0]}
                if i == 12:
                    row["compile_kwd"] = "artifact"
                operations.extend([{"index": {"_index": index_name, "_id": str(i)}}, row])
            assert not client.bulk(operations=operations, refresh=True)["errors"]
            renewed.clear()
            rows = []
            async with aclosing(pagination.search_all_by_search_after(
                {"knowledge_graph_kwd": ["entity"]}, index_name, "kb", ["entity_kwd", "rank_flt"],
                sort_field, page_size=3,
            )) as pages:
                async for page in pages:
                    if not rows:
                        updates = []
                        for cid in sorted(expected):
                            updates.extend([{"update": {"_index": index_name, "_id": cid}}, {"doc": {"rank_flt": 0.75}}])
                        updates.extend([{"index": {"_index": index_name, "_id": "new"}},
                                        {"kb_id": "kb", "knowledge_graph_kwd": "entity", "entity_kwd": "new"}])
                        result = await asyncio.to_thread(client.bulk, operations=updates, refresh=True)
                        assert not result["errors"]
                        await asyncio.wait_for(renewed.wait(), timeout=10)
                    rows.extend(page)
            assert len(rows) == len(expected) and {row["id"] for row in rows} == expected
            assert all(row["rank_flt"] == 0.0 and "q_2_vec" not in row for row in rows)
            stored = client.get(index=index_name, id="0")["_source"]
            assert stored["rank_flt"] == 0.75 and stored["q_2_vec"] == [1.0, 0.0]
            try:
                client.search(body={"pit": {"id": traced.latest_id, "keep_alive": "10m"}, "size": 0})
            except NotFoundError:
                pass
            else:
                raise AssertionError("Closed PIT still accepts searches")
            assert client.indices.stats(index=index_name, metric="search")["_all"]["total"]["search"]["open_contexts"] == 0
            results.append({"sort": sort_field, "rows": len(rows)})
            client.delete(index=index_name, id="new", refresh=True)
        print(json.dumps({"version": version, "scans": results, "renewals": traced.renewals,
                          "closed_pits": traced.closes, "open_contexts": 0}))
    finally:
        try:
            if created:
                client.indices.delete(index=index_name)
                assert not client.indices.exists(index=index_name)
                print("Disposable test index removed")
        finally:
            client.close()


if __name__ == "__main__":
    asyncio.run(validate())
