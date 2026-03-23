import IP2Location
from src.utils.mongo_client import get_collection, get_db

db = get_db()
collection = get_collection()

ipdb = IP2Location.IP2Location("IP-COUNTRY-REGION-CITY.BIN")

pipeline = [
    {"$group": {"_id": "$ip"}}
]

cursor = collection.aggregate(pipeline, allowDiskUse=True)

results = []

for doc in cursor:
    ip = doc["_id"]

    try:
        record = ipdb.get_all(ip)

        data = {
            "ip": ip,
            "country": record.country_long,
            "region": record.region,
            "city": record.city
        }

        results.append(data)

        if len(results) >= 1000:
            db.ip_locations.insert_many(results)
            results = []

    except:
        pass

if results:
    db.ip_locations.insert_many(results)

print("Finished processing IP locations")
