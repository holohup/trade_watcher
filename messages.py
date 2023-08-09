import settings
import redis.asyncio as redis

messages_url = settings.get('MESSAGES_URL')
r = redis.from_url(messages_url)


async def save_message(message: str) -> None:
    async with r as client:
        await client.lpush('messages', message)
