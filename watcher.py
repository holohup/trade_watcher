import asyncio
import os
from dotenv import load_dotenv
import redis.asyncio as redis
from pickle import dumps
from tinkoff.invest import AsyncClient, OrderTrades
from tinkoff.invest.exceptions import AioRequestError


load_dotenv()


async def save_tcs_trade(trades: OrderTrades):
    print(trades)
    async with redis.from_url(os.getenv('REDIS_URL')) as client:
        r = await client.lpush('tcs_trades', dumps(trades))


async def subscribe_to_tcs():
    async with AsyncClient(os.getenv('TCS_RO_TOKEN')) as client:
        while True:
            try:
                async for trades in client.orders_stream.trades_stream(
                    accounts=[os.getenv('TCS_ACCOUNT_ID')]
                ):
                    if trades.ping is not None:
                        last_ping_time = trades.ping.time
                    if trades.order_trades is not None:
                        await save_tcs_trade(trades.order_trades)
            except AioRequestError as e:
                print(f'AuoRequestError, {e.details=}, {e.metadata=}, {e.code=}')
            except Exception as e:
                raise e


if __name__ == "__main__":
    asyncio.run(subscribe_to_tcs())
