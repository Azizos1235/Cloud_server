import argparse
import asyncio
import sys
from pathlib import Path

from pymongo import AsyncMongoClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


async def migrate(dry_run: bool) -> int:
    from app.config import settings

    client = AsyncMongoClient(settings.mongo_uri, tz_aware=True)
    try:
        alerts = client[settings.mongo_db][settings.mongo_alerts_collection]
        legacy_filter = {
            "$or": [
                {"source": "admin"},
                {"eleveur_id": {"$exists": True, "$ne": None}},
                {"source": "system", "type": "appetite"},
            ]
        }
        if dry_run:
            return await alerts.count_documents(legacy_filter)
        result = await alerts.delete_many(legacy_filter)
        return result.deleted_count
    finally:
        await client.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Delete admin-authored or eleveur-targeted legacy alerts."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Count matching alerts without deleting them.",
    )
    args = parser.parse_args()
    count = asyncio.run(migrate(args.dry_run))
    action = "would delete" if args.dry_run else "deleted"
    print(f"{count} legacy alert(s) {action}.")


if __name__ == "__main__":
    main()
