import asyncio
import logging
from datetime import datetime, timezone
from pickle import dumps

import redis.asyncio as redis
from tinkoff.invest import AsyncClient, OrderTrades
from tinkoff.invest.exceptions import AioRequestError

import parsers
import settings
from messages import save_message

PAUSE_BETWEEN_STREAM_CHECKS = 60
SECONDS_BETWEEN_PINGS = 121

logging.basicConfig(
    format="%(asctime)s %(levelname)s:%(message)s", level=logging.DEBUG
)
trades_url = settings.get('TRADES_URL')
last_ping_time = datetime.now(timezone.utc)


async def save_tcs_trades(trades: OrderTrades):
    logging.info(f'received new trades: {trades}')
    async with redis.from_url(trades_url) as client:
        r = await client.lpush('tcs_trades', dumps(trades))
    if not r:
        logging.error(f'could not save trades: {r}, {trades}')


async def watch_tcs_orders():
    global last_ping_time
    async with AsyncClient(settings.get('TCS_RO_TOKEN')) as client:
        logging.info('TCS Stream started')
        while True:
            try:
                async for trades in client.orders_stream.trades_stream(
                    accounts=[settings.get('TCS_ACCOUNT_ID')]
                ):
                    if trades.ping is not None:
                        last_ping_time = trades.ping.time
                        logging.debug(f'received ping: {last_ping_time}')
                    if trades.order_trades is not None:
                        await save_message(parsers.tcs(trades.order_trades))
                        # await save_tcs_trades(trades.order_trades)
            except AioRequestError as e:
                if e.details == 'Stream removed':
                    logging.error('Stream removed error.')
                else:
                    logging.error(
                        f'AuoRequestError, {e.details=}, '
                        f'{e.metadata=}, {e.code=}. {e.args=} {e=}'
                    )
            except Exception as e:
                logging.error(f'Unpredicted error: {e}')
                raise e


def create_stream_task():
    loop = asyncio.get_event_loop()
    return loop.create_task(watch_tcs_orders())


async def manage_streams():
    logging.info('Starting TCS stream')
    stream_task = create_stream_task()
    await asyncio.sleep(SECONDS_BETWEEN_PINGS)
    while True:
        await asyncio.sleep(PAUSE_BETWEEN_STREAM_CHECKS)
        current_time = datetime.now(timezone.utc)
        if (current_time - last_ping_time).seconds >= SECONDS_BETWEEN_PINGS:
            logging.error(
                'Ping time missed, restarting stream listening. '
                f'current time: {current_time.strftime("%H:%M:%S")} '
                f'last ping: {last_ping_time.strftime("%H:%M:%S")}'
            )
            stream_task.cancel()
            while not stream_task.cancelled():
                await asyncio.sleep(1)
            stream_task = create_stream_task()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    manager_task = loop.create_task(manage_streams())
    try:
        loop.run_forever()
    except (KeyboardInterrupt, SystemExit):
        logging.debug('Endind the watcher.')
        if manager_task:
            manager_task.cancel()
            loop.run_until_complete(
                asyncio.gather(manager_task, return_exceptions=True)
            )
        loop.close()
