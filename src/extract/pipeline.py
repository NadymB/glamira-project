def build_pipeline(start_id, end_id):
    return [
        {
            "$match": {
                "time_stamp": {"$gte": start_id, "$lt": end_id},
                "product_id": {"$ne": None, "$ne": ""}
            }
        },
        {
            "$sort": {
                "product_id": 1,
                "_id": -1
            }
        },
        {
            "$group": {
                "_id": "$product_id",
                "last_doc_id": {"$max": "$_id"}
            }
        }
    ]