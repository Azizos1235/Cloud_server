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


async def _ensure_pending_request_email_index(col):
    expected_partial_filter = {"status": "pending"}
    expected_index_exists = False
    async for index in col.list_indexes():
        keys = index.get("key", {})
        if list(keys.items()) != [("email", 1)]:
            continue
        if (index.get("unique")
                and index.get("partialFilterExpression") == expected_partial_filter):
            expected_index_exists = True
        else:
            await col.drop_index(index["name"])

    if not expected_index_exists:
        await col.create_index(
            [("email", 1)],
            unique=True,
            partialFilterExpression=expected_partial_filter,
            name="pending_request_email_unique",
        )


async def ensure_index(cols: Cols):
    async for index in cols.eleveurs.list_indexes():
        keys = index.get("key", {})
        if index.get("unique") and list(keys.items()) == [("email", 1)]:
            await cols.eleveurs.drop_index(index["name"])
    await _ensure_pending_request_email_index(cols.subscription_requests)

    specs = [
        (cols.admins,   [("email", 1)], {"unique": True}),
        (cols.superusers, [("email", 1)], {"unique": True}),
        (cols.invites, [("code_hash", 1)], {"unique": True}),
        (cols.invites, [("admin_id", 1)], {}),
        (cols.subscription_requests, [("status", 1), ("created_at", -1)], {}),
        (cols.daily,    [("cow_id", 1), ("date", -1)], {"unique": True}),       # one summary per cow per day
        (cols.realtime, [("cow_id", 1), ("timestamp", -1)], {}),
        (cols.dispatch, [("cow_id", 1), ("date", 1)], {"unique": True}),
        (cols.profiles, [("admin_id", 1)], {}),
        (cols.profiles, [("nfc_uid", 1)], {"unique": True, "sparse": True}),
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


async def adopt_orphans(cols: Cols, default_admin_id):
    result = await cols.profiles.update_many(
        {"admin_id": {"$exists": False}},
        {"$set": {"admin_id": default_admin_id}},
    )
    log.info("Adopted %d orphan cow profile(s)", result.modified_count)

    cursor = cols.alerts.find({
        "admin_id": {"$exists": False},
        "cow_id": {"$exists": True},
    })
    async for alert in cursor:
        cow = await cols.profiles.find_one(
            {"_id": alert["cow_id"], "admin_id": {"$exists": True}},
            {"admin_id": 1},
        )
        if cow and cow.get("admin_id") is not None:
            await cols.alerts.update_one(
                {"_id": alert["_id"], "admin_id": {"$exists": False}},
                {"$set": {"admin_id": cow["admin_id"]}},
            )
