import json
import time
from jsonschema import validate
from ledger import Balance

from ledger import Amount

from sqlalchemy_models import get_schemas
from trade_manager import ses, em, wm
from kraken_manager import Kraken
from trade_manager.plugin import sync_ticker, get_ticker, sync_balances, get_balances, sync_trades, sync_credits, \
    sync_debits, create_order

kraken = Kraken(session=ses)
SCHEMAS = get_schemas()

"""
def test_ticker():
    sync_ticker('kraken', 'BTC_USD')
    i = 0
    ticker = None
    while ticker is None and i < 10:
        ticker = get_ticker('kraken', 'BTC_USD')
        i += 1
        if ticker is None:
            time.sleep(1)
    tick = json.loads(ticker)
    assert validate(tick, SCHEMAS['Ticker']) is None


def test_balance():
    sync_balances('kraken')
    i = 0
    total = None
    while total is None and i < 10:
        try:
            total, available = get_balances('kraken', session=kraken.session)
        except TypeError:
            i += 1
            time.sleep(1)
    assert isinstance(total, Balance)
    assert isinstance(available, Balance)
    for amount in total:
        assert amount >= available.commodity_amount(amount.commodity)


def test_order_lifecycle():
    sync_ticker('kraken', 'BTC_USD')
    i = 0
    ticker = None
    while ticker is None and i < 10:
        ticker = get_ticker('kraken', 'BTC_USD')
        i += 1
        if ticker is None:
            time.sleep(1)
    assert ticker is not None
    ticker = em.Ticker(**ticker)
    create_order('kraken', Amount("0.01 BTC"), ticker.high * 1.1, 'ask', 'BTC_USD')


def test_sync_trades():
    begin = 1374549600
    end = time.time()
    # ses.query(em.Trade)\
    #   .filter(em.Trade.time >= datetime.datetime.strptime('2012-10-29 10:07:00', '%Y-%m-%d %H:%M:%S'))\
    #   .filter(em.Trade.exchange == 'kraken').delete()
    trade = kraken.session.query(em.Trade).filter(em.Trade.exchange == 'kraken').order_by(em.Trade.time.desc()).first()
    if trade is not None:
        kraken.session.delete(trade)
        begin = 'last'
    count = kraken.session.query(em.Trade).count()
    sync_trades('kraken')
    newcount = kraken.session.query(em.Trade).count()
    i = 0
    while newcount <= count and i < 30:
        i += 1
        newcount = kraken.session.query(em.Trade).count()
        if newcount <= count:
            time.sleep(0.1)
    assert newcount > count


def test_save_credits():
    begin = 1374549600
    end = time.time()
    #kraken.session.query(wm.Credit).filter(wm.Credit.reference == 'kraken').delete()
    credit = kraken.session.query(wm.Credit) \
        .filter(wm.Credit.reference == 'kraken') \
        .order_by(wm.Credit.time.desc()) \
        .first()
    if credit is not None:
        kraken.session.delete(credit)
        begin = 'last'
    count = kraken.session.query(wm.Credit).count()
    sync_credits('kraken')
    newcount = kraken.session.query(wm.Credit).count()
    i = 0
    while newcount <= count and i < 30:
        i += 1
        newcount = kraken.session.query(wm.Credit).count()
        if newcount <= count:
            time.sleep(0.1)
    assert newcount > count


def test_save_debits():
    begin = 1374549600
    end = time.time()
    #kraken.session.query(wm.Debit).filter(wm.Debit.reference == 'kraken').delete()
    debit = kraken.session.query(wm.Debit) \
        .filter(wm.Debit.reference == 'kraken') \
        .order_by(wm.Debit.time.desc()) \
        .first()
    if debit is not None:
        kraken.session.delete(debit)
        begin = 'last'
    count = kraken.session.query(wm.Debit).count()
    sync_debits('kraken')
    newcount = kraken.session.query(wm.Debit).count()
    while newcount <= count and i < 30:
        i += 1
        newcount = kraken.session.query(wm.Debit).count()
        if newcount <= count:
            time.sleep(0.1)
    assert newcount > count
"""
