import json
import logging
import os
import time
import urllib.request
from datetime import datetime, timedelta

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


def parse_start_datetime(start_date: str | None, start_time: str | None) -> datetime | None:
    """Convertit start_date (YYYYMMDD) + start_time (HH:MM:SS, peut dépasser 24h) en datetime."""
    if not start_date or not start_time:
        return None
    try:
        base = datetime.strptime(start_date, "%Y%m%d")
        h, m, s = (int(x) for x in start_time.split(":"))
        return base + timedelta(hours=h, minutes=m, seconds=s)
    except Exception:
        return None


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
        start_time = tu.trip.start_time or None   # "HH:MM:SS"
        start_date = tu.trip.start_date or None   # "YYYYMMDD"

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
            "headsign": None,
            "start_time": start_time,
            "start_date": start_date,
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
                    decoded = val.decode() if isinstance(val, bytes) else str(val)
                    cached = json.loads(decoded)
                    trip_updates[tid]['route'] = cached.get('route')
                    trip_updates[tid]['headsign'] = cached.get('headsign')
                except Exception:
                    # ancienne entrée cache plain string (route_id seulement)
                    trip_updates[tid]['route'] = decoded
                    to_lookup.append(tid)
            else:
                to_lookup.append(tid)

        log.info(f"Résolution routes : {len(missing)} manquants → {len(missing)-len(to_lookup)} depuis cache Redis, {len(to_lookup)} à chercher en DB")
        # 2) Si encore manquants, interroger Postgres (si configuré et driver dispo)
        if to_lookup and TRIPS_DATABASE_URL:
            try:
                with psycopg.connect(TRIPS_DATABASE_URL) as conn:
                    rows = conn.execute(
                        "SELECT trip_id, route_id, trip_headsign FROM trips WHERE trip_id = ANY(%s)",
                        (to_lookup,)
                    ).fetchall()
                    for trip_id, route_id, trip_headsign in rows:
                        if route_id:
                            trip_updates[trip_id]['route'] = route_id
                            trip_updates[trip_id]['headsign'] = trip_headsign or None
                            r.set(f"trip_line:{trip_id}", json.dumps({"route": route_id, "headsign": trip_headsign}, separators=(',', ':')), ex=CACHE_TTL_LINE_S)
            except Exception as e:
                log.error(f"Erreur lors du lookup Postgres trip->route: {e}")

    # 3) Publier TripUpdates et construire mappings route->vehicles et vehicle->info
    pipe = r.pipeline()
    for trip_id, data in trip_updates.items():
        pipe.set(f"{REDIS_KEY_PREFIX}{trip_id}", json.dumps(data, separators=(',', ':'), ensure_ascii=False), ex=REDIS_TTL_S)

    route_map = {}
    # vehicle_best : pour chaque véhicule, on garde le trip dont le start_time
    # est le plus récent parmi ceux déjà démarrés (ou le plus proche dans le futur
    # si tous les trips sont encore à venir).
    now = datetime.now()
    vehicle_best: dict[str, tuple[datetime | None, dict]] = {}
    for tid, data in trip_updates.items():
        route = data.get('route')
        vehicle = data.get('vehicle')
        headsign = data.get('headsign')
        if not (route and vehicle):
            continue
        route_map.setdefault(route, set()).add(vehicle)
        # Retard au prochain arrêt : premier élément du dict delays (insertion-order)
        next_delay = next(iter(data.get('delays', {}).values()), None)
        info = {"route": route, "headsign": headsign, "delay": next_delay, "trip_id": tid}
        start_dt = parse_start_datetime(data.get('start_date'), data.get('start_time'))
        if vehicle not in vehicle_best:
            vehicle_best[vehicle] = (start_dt, info)
        else:
            prev_dt, _ = vehicle_best[vehicle]
            if start_dt is None:
                pass  # pas de start_time : on garde l'existant
            elif prev_dt is None:
                vehicle_best[vehicle] = (start_dt, info)
            else:
                prev_past = prev_dt <= now
                curr_past = start_dt <= now
                if prev_past and curr_past:
                    # Les deux déjà démarrés : prendre le plus récent
                    if start_dt > prev_dt:
                        vehicle_best[vehicle] = (start_dt, info)
                elif curr_past and not prev_past:
                    # Nouveau déjà démarré, ancien dans le futur : prendre le nouveau
                    vehicle_best[vehicle] = (start_dt, info)
                elif not prev_past and not curr_past:
                    # Les deux dans le futur : prendre le plus proche
                    if start_dt < prev_dt:
                        vehicle_best[vehicle] = (start_dt, info)
                # else : ancien déjà démarré, nouveau dans le futur → garder l'ancien
    vehicle_info = {v: info for v, (_, info) in vehicle_best.items()}

    # Publier attributions:<route> -> JSON array of vehicle ids (tous sens, TTL 5min)
    for route, vehicles in route_map.items():
        vehicles_list = sorted(list(vehicles))
        pipe.set(f"attributions:{route}", json.dumps(vehicles_list, separators=(',', ':'), ensure_ascii=False), ex=CACHE_TTL_LINE_S)

    # Publier attributions:<route>:<headsign> depuis vehicle_info (headsign correct par véhicule)
    route_headsign_map: dict[tuple[str, str], set[str]] = {}
    for vehicle, info in vehicle_info.items():
        route = info['route']
        headsign = info['headsign']
        if headsign:
            route_headsign_map.setdefault((route, headsign), set()).add(vehicle)

    for (route, headsign), vehicles in route_headsign_map.items():
        vehicles_list = sorted(list(vehicles))
        pipe.set(f"attributions:{route}:{headsign}", json.dumps(vehicles_list, separators=(',', ':'), ensure_ascii=False), ex=CACHE_TTL_LINE_S)

    # Publier vehicle:<vehicle_id> -> {route, headsign, delay, trip_id} (TTL 90s)
    for vehicle, info in vehicle_info.items():
        pipe.set(f"vehicle:{vehicle}", json.dumps(info, separators=(',', ':'), ensure_ascii=False), ex=REDIS_TTL_S)

    pipe.execute()

    log.info(f"✅ {len(trip_updates)} clés publiées dans Redis (préfixe '{REDIS_KEY_PREFIX}', TTL {REDIS_TTL_S}s)")
    log.info(f"✅ {len(route_map)} attributions par ligne | {len(route_headsign_map)} par direction | {len(vehicle_info)} vehicle:")


if __name__ == "__main__":
    while True:
        try:
            fetch_and_push()
        except Exception as e:
            log.error(f"Erreur lors de l'exécution : {e}")
        
        time.sleep(59)
