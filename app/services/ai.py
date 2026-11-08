import httpx

from app.config import settings


async def predict(record: dict) -> dict:
    """Ask the classifier service on the Pi (it reads the cow's history itself)."""
    if not settings.pi_ai_url:
        raise NotImplementedError("PI_AI_URL is not set on the server")
    url = f'{settings.pi_ai_url.rstrip("/")}/predict/{record["cow_id"]}'
    async with httpx.AsyncClient(timeout=settings.pi_ai_timeout,
                                 proxy=settings.pi_proxy or None) as client:
        r = await client.get(url)
    if r.status_code != 200:
        raise RuntimeError(f"AI service HTTP {r.status_code}: {r.text[:200]}")
    return r.json()
