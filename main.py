import json
import logging
import os
import urllib.request

import redis
from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2

load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

GTFSRT_URL = os.getenv("GTFSRT_URL")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

# Clé Redis où le JSON des trip updates est stocké
REDIS_KEY_PREFIX = "trip:"
# TTL : légèrement supérieur à la périodicité du CronJob (ex: 45s pour un job toutes les 30s)
REDIS_TTL_S = 45


def fetch_and_push():
    log.info(f"Source GTFS-RT : {GTFSRT_URL}")

    try:
        req = urllib.request.Request(GTFSRT_URL, headers={"User-Agent": "gtfsrt-redis/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read()
    except Exception as e:
        log.error(f"Échec du téléchargement GTFS-RT : {e}")
        raise

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(raw)

    log.info(f"Timestamp flux : {feed.header.timestamp} | Entités : {len(feed.entity)}")

    trip_updates = {}
    skipped = 0

    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue

        tu = entity.trip_update
        trip_id = tu.trip.trip_id

        if not trip_id:
            skipped += 1
            continue

        vehicle_id = tu.vehicle.id if tu.vehicle.id else None

        delays = {}
        for stu in tu.stop_time_update:
            stop_id = stu.stop_id
            if not stop_id:
                continue

            delay = None
            if stu.HasField("arrival"):
                delay = stu.arrival.delay
            elif stu.HasField("departure"):
                delay = stu.departure.delay

            delays[stop_id] = delay

        trip_updates[trip_id] = {
            "vehicle": vehicle_id,
            "delays": delays,
        }

    log.info(f"TripUpdates valides : {len(trip_updates)} | Ignorées : {skipped}")

    r = redis.from_url(REDIS_URL)
    pipe = r.pipeline()
    for trip_id, data in trip_updates.items():
        pipe.set(f"{REDIS_KEY_PREFIX}{trip_id}", json.dumps(data, separators=(',', ':')), ex=REDIS_TTL_S)
    pipe.execute()

    log.info(f"✅ {len(trip_updates)} clés publiées dans Redis (préfixe '{REDIS_KEY_PREFIX}', TTL {REDIS_TTL_S}s)")


if __name__ == "__main__":
    fetch_and_push()
