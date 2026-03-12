import pymongo
from datetime import datetime
import json


def push_mongodb(dest, source, rows):
    """
    Push rows to MongoDB.
    Expects json format only.
    Mapping:
    - cluster_uri -> host
    - database -> database_name
    """
    if not rows:
        return 0

    uri = dest["host"]
    db_name = dest["database_name"]
    collection_name = f"{source}_data"

    client = pymongo.MongoClient(uri)
    db = client[db_name]
    collection = db[collection_name]

    # MongoDB expects list of dicts. 
    # Ensure all rows have fetched_at if not present.
    now = datetime.utcnow().isoformat()
    
    formatted_rows = []
    for r in rows:
        row = dict(r)
        if "fetched_at" not in row:
            row["fetched_at"] = now
        formatted_rows.append(row)

    result = collection.insert_many(formatted_rows)
    count = len(result.inserted_ids)

    client.close()

    print(f"[DEST] Pushed {count} rows to MongoDB ({db_name}.{collection_name})")

    return count
