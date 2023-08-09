import asyncio
import logging
from pickle import dumps

import redis.asyncio as redis
from tinkoff.invest import AsyncClient, OrderTrades
from tinkoff.invest.exceptions import AioRequestError

import settings


logging.basicConfig(
    format="%(asctime)s %(levelname)s:%(message)s", level=logging.DEBUG
)
trades_url = settings.get('TRADES_URL')


async def save_tcs_trade(trades: OrderTrades):
    logging.info(f'received new trades: {trades}')
    async with redis.from_url(trades_url) as client:
        r = await client.lpush('tcs_trades', dumps(trades))
    if not r:
        logging.error(f'could not save trades: {r}, {trades}')


async def watch_tcs_orders():
    async with AsyncClient(settings.get('TCS_RO_TOKEN')) as client:
        while True:
            try:
                async for trades in client.orders_stream.trades_stream(
                    accounts=[settings.get('TCS_ACCOUNT_ID')]
                ):
                    if trades.ping is not None:
                        last_ping_time = trades.ping.time
                        logging.debug(f'received ping: {last_ping_time}')
                    if trades.order_trades is not None:
                        await save_tcs_trade(trades.order_trades)
            except AioRequestError as e:
                if e.details == 'Stream removed':
                    logging.error('Stream removed error.')
                else:
                    logging.error(
                        f'AuoRequestError, {e.details=}, {e.metadata=}, {e.code=}. {e.args=} {e=}'
                    )
            except Exception as e:
                logging.error(f'Unpredicted error: {e}')
                raise e


if __name__ == "__main__":
    try:
        asyncio.run(watch_tcs_orders())
    except (KeyboardInterrupt, SystemExit):
        pass
