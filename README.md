# gtfsrt-redis

Micro-service Python qui récupère le flux **GTFS-RT** (TripUpdates) et le publie dans Redis.

Conçu pour être exécuté par un **CronJob Kubernetes** (ex : toutes les 30 secondes).

## Variables d'environnement

| Variable | Description |
|---|---|
| `GTFSRT_URL` | URL du flux GTFS-RT protobuf |
| `REDIS_URL` | URL de connexion Redis (défaut : `redis://localhost:6379`) |

## Clé Redis produite

```
gtfsrt:trip_updates   →  JSON  { "<trip_id>": { "vehicle": "422", "delays": { "<stop_id>": <delay_sec> } } }
```

TTL : **45 secondes** (à ajuster selon la périodicité du CronJob).

## Lancement local

```bash
cp .env.example .env
# remplir GTFSRT_URL et REDIS_URL dans .env
pip install -r requirements.txt
python main.py
```
