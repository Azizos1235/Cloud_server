import os
import secrets
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx

os.environ.setdefault("JWT_SECRET", secrets.token_urlsafe(32))

from app.config import settings
from app.jobs.appetite import classify
from app.services.ai import predict


class _FakeHttpClient:
    def __init__(self):
        self.get = AsyncMock(
            return_value=SimpleNamespace(
                status_code=200,
                json=lambda: {"appetite_class": "high"},
            )
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return None


class AppetiteFallbackTimeoutTests(unittest.IsolatedAsyncioTestCase):
    async def test_ai_timeout_is_capped_at_five_seconds(self):
        client = _FakeHttpClient()
        with (
            patch.object(settings, "pi_ai_timeout", 30.0),
            patch.object(settings, "pi_ai_url", "http://ai.invalid"),
            patch("app.services.ai.httpx.AsyncClient", return_value=client) as factory,
        ):
            result = await predict({"cow_id": "COW001"})

        self.assertEqual(result["appetite_class"], "high")
        self.assertLessEqual(factory.call_args.kwargs["timeout"], 5.0)

    async def test_ai_timeout_falls_back_to_rule_classification(self):
        document = {
            "cow_id": "COW001",
            "total_food_consumed_g": 300,
            "total_feed_offered_g": 1000,
        }
        with (
            patch.object(settings, "appetite_use_ai", True),
            patch(
                "app.jobs.appetite.predict",
                new=AsyncMock(side_effect=httpx.ReadTimeout("AI timed out")),
            ),
        ):
            result = await classify(document)

        self.assertEqual(result, ("low", "rule", {"low": 30.0}))


if __name__ == "__main__":
    unittest.main()
