def build_pipeline(start_id, end_id, col_values):
    return [
        {
            "$match": {
                "_id": {"$gte": start_id, "$lt": end_id},
                "collection": {
                    "$in": col_values
                }
            }
        },
        {
            "$project": {
                "_id": 1,
                "product_id": "$viewing_product_id",
                "url": "$referrer_url"
            }
        },
        {
            "$match": {
                "product_id": {"$ne": None},
                "url": {"$ne": None}
            }
        },
        {
            "$group": {
                "_id": "$product_id",
                "url": {"$first": "$url"},
                "last_doc_id": {"$max": "$_id"}
            }
        }
    ]