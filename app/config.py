import logging

from pydantic_settings import BaseSettings, SettingsConfigDict

logging.basicConfig(level=logging.INFO)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    amqp_url: str
    mongo_uri: str
    mongo_db: str = "farm"
    jwt_secret: str                                        # same secret as the auth backend

    mongo_realtime_collection: str = "realtime_readings"   # LectureTempsReel, append only
    mongo_daily_collection: str = "daily_summaries"        # ResumeJournalier, one per cow per day
    mongo_profiles_collection: str = "cow_profiles"        # Vache
    mongo_alerts_collection: str = "alerts"                # Alerte
    mongo_dispatch_collection: str = "feed_dispatch"       # one dispense per cow per day

    daily_queue: str = "cow.daily"
    realtime_queue: str = "cow.realtime"
    dispatch_queue: str = ""                               # empty = disabled

    pi_ai_url: str = "http://127.0.0.1:8001"
    pi_proxy: str = ""
    pi_ai_timeout: float = 30.0
    realtime_ttl_days: int = 90                            # 0 = keep forever


settings = Settings()

BREED_TYPE = {"Holstein": "dairy", "Jersey": "dairy", "Angus": "beef",
              "Limousin": "beef", "Charolais": "beef", "Simmental": "dual"}
READING_FIELDS = ("visits", "food_consumed_g", "feed_offered_g", "eating_minutes", "no_food_visits")
LOW_APPETITE = {"low", "poor", "faible"}   # appetite_class values that raise a system alert
