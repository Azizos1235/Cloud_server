import logging

from pydantic_settings import BaseSettings, SettingsConfigDict

logging.basicConfig(level=logging.INFO)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    amqp_url: str
    mongo_uri: str
    mongo_db: str = "farm"
    jwt_secret: str                                        # same secret as the auth backend
    invite_ttl_hours: int = 24
    jwt_expire_hours: int = 168
    eleveur_jwt_expire_hours: int = 8760
    superuser_email: str = ""
    superuser_password: str = ""
    subscription_days_per_month: int = 30
    appetite_alert_threshold: float = 65
    default_admin_email: str = ""
    default_admin_password: str = ""
    default_admin_name: str = "Default Admin"
    default_admin_months: int = 12

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

    # --- end-of-day appetite job ---
    eod_enabled: bool = True
    eod_hour: int = 23                                     # local time of the run
    eod_minute: int = 55
    timezone: str = "Africa/Tunis"
    appetite_use_ai: bool = True                           # ask the Pi first, rule below as fallback
    appetite_low_ratio: float = 0.5                        # eaten / offered below this = "low"
    appetite_high_ratio: float = 0.9                       # eaten / offered above this = "high"


settings = Settings()

BREED_TYPE = {"Holstein": "dairy", "Jersey": "dairy", "Angus": "beef",
              "Limousin": "beef", "Charolais": "beef", "Simmental": "dual"}
READING_FIELDS = ("visits", "food_consumed_g", "feed_offered_g", "eating_minutes", "no_food_visits")
LOW_APPETITE = {"low", "poor", "faible"}   # appetite_class values that raise a system alert
