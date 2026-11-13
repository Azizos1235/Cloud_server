import logging

from app.config import settings

log = logging.getLogger("ingest")


class Cols:
    """One collection per class of the diagram."""

    def __init__(self, db):
        self.admins = db["admins"]
        self.eleveurs = db["eleveurs"]
        self.superusers = db["superusers"]
        self.invites = db["invites"]
        self.subscription_requests = db["subscription_requests"]
        self.profiles = db[settings.mongo_profiles_collection]
        self.realtime = db[settings.mongo_realtime_collection]
        self.daily = db[settings.mongo_daily_collection]
        self.alerts = db[settings.mongo_alerts_collection]
        self.dispatch = db[settings.mongo_dispatch_collection]


async def ensure_index(cols: Cols):
    specs = [
        (cols.admins,   [("email", 1)], {"unique": True}),
        (cols.eleveurs, [("email", 1)], {"unique": True}),
        (cols.superusers, [("email", 1)], {"unique": True}),
        (cols.invites, [("code_hash", 1)], {"unique": True}),
        (cols.invites, [("admin_id", 1)], {}),
        (cols.subscription_requests, [("status", 1), ("created_at", -1)], {}),
        (cols.subscription_requests, [("email", 1)], {}),
        (cols.daily,    [("cow_id", 1), ("date", -1)], {"unique": True}),       # one summary per cow per day
        (cols.realtime, [("cow_id", 1), ("timestamp", -1)], {}),
        (cols.dispatch, [("cow_id", 1), ("date", 1)], {"unique": True}),
        (cols.profiles, [("admin_id", 1)], {}),
        (cols.profiles, [("eleveur_id", 1)], {}),
        (cols.profiles, [("nfc_uid", 1)], {"unique": True, "sparse": True}),
        (cols.alerts,   [("eleveur_id", 1), ("status", 1), ("created_at", -1)], {}),
        (cols.alerts,   [("admin_id", 1), ("created_at", -1)], {}),
        (cols.alerts,   [("dedupe_key", 1)], {"unique": True, "sparse": True}),  # no duplicate system alerts
    ]
    if settings.realtime_ttl_days > 0:
        specs.append((cols.realtime, [("timestamp", 1)],
                      {"expireAfterSeconds": settings.realtime_ttl_days * 86400}))
    for col, keys, options in specs:
        try:
            await col.create_index(keys, **options)
        except Exception:
            log.exception("Could not create index %s on %s (will continue)", keys, col.name)
