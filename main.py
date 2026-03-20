import json
import logging
import os
import time
import urllib.request

import psycopg
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
TRIPS_DATABASE_URL = os.getenv("TRIPS_DATABASE_URL")

# Clé Redis où le JSON des trip updates est stocké
REDIS_KEY_PREFIX = "trip:"
# TTL recommandé par la spec GTFS-RT : 90s (job toutes les 60s)
REDIS_TTL_S = 90


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

        vehicle_id = tu.vehicle.id or None
        route_id = None

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
            "route": route_id,
            "delays": delays,
        }

    log.info(f"TripUpdates valides : {len(trip_updates)} | Ignorées : {skipped}")

    r = redis.from_url(REDIS_URL)

    # TTL cache pour trip->line : 5 minutes
    CACHE_TTL_LINE_S = 300

    # 1) Résolution trip->line manquants : d'abord cache Redis
    missing = [tid for tid, d in trip_updates.items() if not d.get('route')]
    if missing:
        cache_keys = [f"trip_line:{tid}" for tid in missing]
        cached_vals = r.mget(cache_keys)

        to_lookup = []
        for tid, val in zip(missing, cached_vals):
            if val:
                try:
                    line = val.decode() if isinstance(val, bytes) else str(val)
                except Exception:
                    line = str(val)
                trip_updates[tid]['route'] = line
            else:
                to_lookup.append(tid)

        log.info(f"Résolution routes : {len(missing)} manquants → {len(missing)-len(to_lookup)} depuis cache Redis, {len(to_lookup)} à chercher en DB")
        # 2) Si encore manquants, interroger Postgres (si configuré et driver dispo)
        if to_lookup and TRIPS_DATABASE_URL:
            try:
                with psycopg.connect(TRIPS_DATABASE_URL) as conn:
                    # Paramètre : liste/array
                    rows = conn.execute("SELECT trip_id, route_id FROM trips WHERE trip_id = ANY(%s)", (to_lookup,)).fetchall()
                    for trip_id, route_id in rows:
                        if route_id:
                            trip_updates[trip_id]['route'] = route_id
                            # cache
                            r.set(f"trip_line:{trip_id}", route_id, ex=CACHE_TTL_LINE_S)
            except Exception as e:
                log.error(f"Erreur lors du lookup Postgres trip->route: {e}")

    # 3) Publier TripUpdates et construire mapping route->vehicles
    pipe = r.pipeline()
    for trip_id, data in trip_updates.items():
        pipe.set(f"{REDIS_KEY_PREFIX}{trip_id}", json.dumps(data, separators=(',', ':'), ensure_ascii=False), ex=REDIS_TTL_S)

    route_map = {}
    for data in trip_updates.values():
        route = data.get('route')
        vehicle = data.get('vehicle')
        if route and vehicle:
            route_map.setdefault(route, set()).add(vehicle)

    # Publier attributions:<route> -> JSON array of vehicle ids (TTL 5min)
    for route, vehicles in route_map.items():
        vehicles_list = sorted(list(vehicles))
        pipe.set(f"attributions:{route}", json.dumps(vehicles_list, separators=(',', ':'), ensure_ascii=False), ex=CACHE_TTL_LINE_S)

    pipe.execute()

    log.info(f"✅ {len(trip_updates)} clés publiées dans Redis (préfixe '{REDIS_KEY_PREFIX}', TTL {REDIS_TTL_S}s)")
    log.info(f"✅ {len(route_map)} attributions publiées (préfixe 'attributions:')")


if __name__ == "__main__":
    while True:
        try:
            fetch_and_push()
        except Exception as e:
            log.error(f"Erreur lors de l'exécution : {e}")
        
        time.sleep(59)
