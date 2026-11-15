"""Migrate legacy RFID-valued cow_id fields in daily_summaries."""

import argparse
import asyncio
from pathlib import Path

from pymongo import AsyncMongoClient
from pydantic_settings import BaseSettings, SettingsConfigDict


class MigrationSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=Path(__file__).resolve().parents[1] / ".env",
        extra="ignore",
    )

    mongo_uri: str
    mongo_db: str = "farm"
    mongo_daily_collection: str = "daily_summaries"
    mongo_profiles_collection: str = "cow_profiles"


settings = MigrationSettings()


async def migrate(dry_run: bool) -> int:
    client = AsyncMongoClient(settings.mongo_uri)
    try:
        db = client[settings.mongo_db]
        summaries = db[settings.mongo_daily_collection]
        profiles = db[settings.mongo_profiles_collection]
        changed = 0

        async for document in summaries.find({
            "rfid": {"$exists": False},
            "cow_id": {"$type": "string"},
        }):
            rfid = document["cow_id"].strip().upper()
            if not rfid:
                continue
            profile = await profiles.find_one({"_id": rfid}, {"cow_id": 1})
            cow_id = profile.get("cow_id") if profile else None
            if not isinstance(cow_id, str) or not cow_id.strip():
                continue
            cow_id = cow_id.strip().upper()
            if cow_id == rfid:
                continue

            if dry_run:
                changed += 1
                continue

            result = await summaries.update_one(
                {
                    "_id": document["_id"],
                    "cow_id": document["cow_id"],
                    "rfid": {"$exists": False},
                },
                {"$set": {"cow_id": cow_id, "rfid": rfid}},
            )
            changed += result.modified_count

        return changed
    finally:
        await client.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Only count eligible documents")
    args = parser.parse_args()
    changed = asyncio.run(migrate(args.dry_run))
    if args.dry_run:
        print(f"Would change {changed} daily_summaries document(s)")
    else:
        print(f"Changed {changed} daily_summaries document(s)")


if __name__ == "__main__":
    main()
