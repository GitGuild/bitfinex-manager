import time

from trade_manager import ses, em, wm
from bitfinex_manager import Bitfinex

bitfinex = Bitfinex()


def test_save_trades():
    begin = 1354549600
    end = time.time()
    #bitfinex.session.query(em.Trade).filter(em.Trade.exchange == 'bitfinex').filter(em.Trade.time >= datetime.datetime.strptime('2016-05-29 10:07:00', '%Y-%m-%d %H:%M:%S')).delete()
    #bitfinex.session.query(em.Trade).filter(em.Trade.exchange == 'bitfinex').delete()
    #bitfinex.session.commit()
    trade = bitfinex.session.query(em.Trade).filter(em.Trade.exchange == 'bitfinex').order_by(em.Trade.time.desc()).first()
    if trade is not None:
        bitfinex.session.delete(trade)
        begin = 'last'
    count = bitfinex.session.query(em.Trade).filter(em.Trade.exchange == 'bitfinex').count()
    bitfinex.save_trades(begin=begin, end=end)
    newcount = bitfinex.session.query(em.Trade).filter(em.Trade.exchange == 'bitfinex').count()
    assert newcount > count


def test_save_credits():
    begin = 1354549600
    end = time.time()
    #bitfinex.session.query(wm.Credit).filter(wm.Credit.reference=='bitfinex').delete()
    credit = bitfinex.session.query(wm.Credit)\
        .filter(wm.Credit.reference=='bitfinex')\
        .order_by(wm.Credit.time.desc())\
        .first()
    if credit is not None:
        bitfinex.session.delete(credit)
        begin = 'last'
    count = bitfinex.session.query(wm.Credit).count()
    bitfinex.save_credits(begin=begin, end=end)
    newcount = bitfinex.session.query(wm.Credit).count()
    assert newcount > count


def test_save_debits():
    begin = 1354549600
    end = time.time()
    #bitfinex.session.query(wm.Debit).filter(wm.Debit.reference=='bitfinex').delete()
    debit = bitfinex.session.query(wm.Debit)\
        .filter(wm.Debit.reference=='bitfinex')\
        .order_by(wm.Debit.time.desc())\
        .first()
    if debit is not None:
        bitfinex.session.delete(debit)
        begin = 'last'
    count = bitfinex.session.query(wm.Debit).count()
    bitfinex.save_debits(begin=begin, end=end)
    newcount = bitfinex.session.query(wm.Debit).count()
    assert newcount > count

if __name__ == "__main__":
    test_save_trades()
    test_save_credits()
    test_save_debits()
