from repo import TCSAssetRepo
from tinkoff.invest import OrderTrades, OrderDirection
from tinkoff.invest.utils import quotation_to_decimal


def tcs(trades: OrderTrades) -> str:
    repo = TCSAssetRepo()
    ticker = repo[trades.instrument_uid].ticker
    quantity = 0
    total_price = 0
    for trade in trades.trades:
        quantity += trade.quantity
        total_price += quotation_to_decimal(trade.price) * trade.quantity
    if quantity == 0:
        return f'Empty order report received: {trades}'
    exec_price = float(total_price / quantity)
    direction = (
        'Sold'
        if trades.direction == OrderDirection.ORDER_DIRECTION_SELL
        else 'Bought'
    )
    details = f'{direction} {quantity} {ticker} for {exec_price}'
    return f'TCS order filled: {details}'
