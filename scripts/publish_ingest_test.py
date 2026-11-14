import json
import os
import sys
from datetime import datetime, timezone

import pika


def main() -> None:
    amqp_url = os.environ.get("AMQP_URL")
    if not amqp_url:
        raise SystemExit("Set AMQP_URL to the RabbitMQ connection string.")

    now = datetime.now(timezone.utc)
    cow_id = os.environ.get("INGEST_TEST_COW_ID", "INGEST-TEST-001")
    common = {
        "cow_id": cow_id,
        "date": now.date().isoformat(),
        "age_months": 24.0,
        "food_consumed_g": 830,
        "feed_offered_g": 8208.5,
        "visits": 1,
        "eating_minutes": 8.0,
        "avg_meal_duration_min": 8,
        "no_food_visits": 0,
        "first_visit_hour": 6,
        "last_visit_hour": 6,
    }
    realtime = {
        **common,
        "received_at": now.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
    }
    daily = {
        **common,
        "appetite": {
            "category": "normal",
            "percentages": {
                "normal": 82.5,
                "decreasing": 12.0,
                "increasing": 5.5,
            },
            "alert": False,
        },
    }

    connection = None
    try:
        connection = pika.BlockingConnection(pika.URLParameters(amqp_url))
        channel = connection.channel()
        properties = pika.BasicProperties(
            content_type="application/json",
            delivery_mode=2,
        )
        for queue_name, payload in (
            ("cow.realtime", realtime),
            ("cow.daily", daily),
        ):
            channel.basic_publish(
                exchange="",
                routing_key=queue_name,
                body=json.dumps(payload),
                properties=properties,
                mandatory=True,
            )
            print("Published one test message to " + queue_name)
    except Exception:
        print("Publish failed; check RabbitMQ connectivity and queue configuration.")
        sys.exit(1)
    finally:
        if connection is not None and connection.is_open:
            connection.close()


if __name__ == "__main__":
    main()
