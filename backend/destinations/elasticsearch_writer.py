from elasticsearch import Elasticsearch, helpers
from datetime import datetime
import json


def push_elasticsearch(dest, source, rows):
    """
    Push rows to Elasticsearch.
    Expects json format only.
    Mapping:
    - endpoint -> host
    - port -> port
    - username -> username
    - password -> password
    - index_name -> database_name
    """
    if not rows:
        return 0

    endpoint = dest["host"]
    port = dest.get("port")
    user = dest.get("username")
    pwd = dest.get("password")
    index_name = dest["database_name"].lower() # ES indices must be lowercase

    # Build connection
    if user and pwd:
        # Assuming https if no scheme provided, or follow host exactly
        es = Elasticsearch(
            [endpoint],
            basic_auth=(user, pwd),
            verify_certs=False # Typically needed for internal dev endpoints
        )
    else:
        es = Elasticsearch([endpoint])

    now = datetime.utcnow().isoformat()
    
    actions = []
    for r in rows:
        row = dict(r)
        if "fetched_at" not in row:
            row["fetched_at"] = now
        
        actions.append({
            "_index": index_name,
            "_source": row
        })

    success, failed = helpers.bulk(es, actions)
    
    print(f"[DEST] Pushed {success} rows to Elasticsearch (index: {index_name}). Failed: {len(failed) if isinstance(failed, list) else failed}", flush=True)

    return success
