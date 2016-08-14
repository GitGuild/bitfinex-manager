"""
Plugin for managing a Bitfinex account.
This module can be imported by trade_manager and used like a plugin.
"""
import datetime
import hmac
import json
import requests
import time
from base64 import b64encode
from hashlib import sha384
from ledger import Amount, Balance
from requests.exceptions import Timeout, ConnectionError
from sqlalchemy import update
from sqlalchemy_models import jsonify2
from trade_manager import em, wm
from trade_manager.plugin import ExchangePluginBase, get_order_by_order_id, submit_order, get_orders

NAME = 'bitfinex'

BASE_URL = "https://api.bitfinex.com"
REQ_TIMEOUT = 10  # seconds


class Bitfinex(ExchangePluginBase):
    NAME = 'bitfinex'
    _user = None

    def bitfinex_encode(self, msg):
        msg['nonce'] = str(int(time.time() * 1e6))
        msg = b64encode(json.dumps(msg))
        signature = hmac.new(self.secret, msg, sha384).hexdigest()
        return {
            'X-BFX-APIKEY': self.key,
            'X-BFX-PAYLOAD': msg,
            'X-BFX-SIGNATURE': signature
        }

    def bitfinex_request(self, endpoint, params=None):
        if "/v1/" not in endpoint:
            endpoint = "/v1/%s" % endpoint
        params = params or {}
        params['request'] = endpoint
        headers = self.bitfinex_encode(params)
        response = None
        try:
            response = requests.post(url=BASE_URL + params['request'],
                                     headers=headers,
                                     timeout=REQ_TIMEOUT)
            if "Nonce is too small." in response:
                response = None
        except (ConnectionError, Timeout) as e:
            self.logger.exception(
                '%s %s while sending %r to bitfinex %s, response %s' % (type(e), e, params, endpoint, response))
            return
        return response

    @classmethod
    def format_market(cls, market):
        """
        The default market symbol is an uppercase string consisting of the base commodity
        on the left and the quote commodity on the right, separated by an underscore.

        If the data provided by the exchange does not match the default
        implementation, then this method must be re-implemented.

        Bitfinex uses the following.

        formatted : unformatted
        'BTC_USD': 'btcusd'
        'LTC_USD': 'ltcusd'
        'LTC_BTC': 'ltcbtc'
        'ETH_USD': 'ethusd'
        'ETH_BTC': 'ethbtc'
        'DASH_USD': 'drkusd'

        :return: a market formated according to what bitcoin_exchanges expects.
        """
        base = market[:3].upper().replace('DRK', 'DASH')
        quote = market[3:].upper().replace('DRK', 'DASH')
        return base + '_' + quote

    @classmethod
    def unformat_market(cls, market):
        """
        Reverse format a market to the format recognized by the exchange.
        If the data provided by the exchange does not match the default
        implementation, then this method must be re-implemented.

        :return: a market formated according to what bitfinex expects.
        """
        try:
            base, quote = market.lower().split("_")
        except ValueError:
            base = market[:3].lower().replace('dash', 'drk')
            quote = market[3:].lower().replace('dash', 'drk')
        return "%s%s" % (base, quote)

    @classmethod
    def format_commodity(cls, c):
        """
        The default commodity symbol is an uppercase string of 3 or 4 letters.

        If the data provided by the exchange does not match the default
        implementation, then this method must be re-implemented.
        """
        return c.upper().replace('DRK', 'DASH')

    @classmethod
    def unformat_commodity(cls, c):
        """
        Reverse format a commodity to the format recognized by the exchange.
        If the data provided by the exchange does not match the default
        implementation, then this method must be re-implemented.
        """
        return c.lower().replace('dash', 'drk')

    @classmethod
    def sync_book(cls, market=None):
        exch_pair = cls.unformat_market(market)
        return requests.get('%s/v1/book/%s' % (BASE_URL, exch_pair), timeout=REQ_TIMEOUT).json()

    def sync_ticker(self, market='BTC_USD'):
        exch_pair = self.unformat_market(market)
        try:
            rawtick = requests.get(BASE_URL + '/v1/pubticker/%s' % exch_pair, timeout=REQ_TIMEOUT).json()
        except (ConnectionError, Timeout, ValueError) as e:
            self.logger.exception(e)
            return

        tick = em.Ticker(float(rawtick['bid']),
                         float(rawtick['ask']),
                         float(rawtick['high']),
                         float(rawtick['low']),
                         float(rawtick['volume']),
                         float(rawtick['last_price']),
                         market, 'bitfinex')

        self.logger.debug("bitfinex %s tick %s" % (market, tick))
        jtick = jsonify2(tick, 'Ticker')
        self.logger.debug("bitfinex %s json ticker %s" % (market, jtick))
        self.red.set('bitfinex_%s_ticker' % market, jtick)
        return tick

    def sync_balances(self):
        try:
            data = self.bitfinex_request('balances').json()
        except ValueError as e:
            self.logger.exception('%s %s while sending to bitfinex get_balance' % (type(e), str(e)))
        if 'message' in data:
            self.logger.exception('%s while sending to bitfinex get_balance' % data['message'])
        self.logger.debug('balances data %s' % data)
        self.logger.debug('self.active_currencies %s' % self.active_currencies)
        total = Balance()
        available = Balance()
        for bal in data:
            comm = self.format_commodity(bal['currency'])
            total = total + Amount("%s %s" % (bal['amount'], comm))
            available = available + Amount("%s %s" % (bal['available'], comm))
        self.logger.debug("total balance: %s" % total)
        self.logger.debug("available balance: %s" % available)
        bals = {}
        for amount in total:
            comm = str(amount.commodity)
            bals[comm] = self.session.query(wm.Balance).filter(wm.Balance.user_id == self.manager_user.id) \
                .filter(wm.Balance.currency == comm).one_or_none()
            if not bals[comm]:
                bals[comm] = wm.Balance(amount, available.commodity_amount(amount.commodity), comm, "",
                                        self.manager_user.id)
                self.session.add(bals[comm])
            else:
                bals[comm].load_commodities()
                bals[comm].total = amount
                bals[comm].available = available.commodity_amount(amount.commodity)
        try:
            self.session.commit()
        except Exception as e:
            self.logger.exception(e)
            self.session.rollback()
            self.session.flush()

    def sync_orders(self):
        oorders = self.get_open_orders()
        dboorders = get_orders(exchange='bitfinex', state='open', session=self.session)
        for dbo in dboorders:
            if dbo not in oorders:
                dbo.state = 'closed'
        self.session.commit()

    def cancel_order(self, oid=None, order_id=None, order=None):
        if order is None and oid is not None:
            order = self.session.query(em.LimitOrder).filter(em.LimitOrder.id == oid).first()
        elif order is None and order_id is not None:
            order = self.session.query(em.LimitOrder).filter(em.LimitOrder.order_id == order_id).first()
        elif order is None:
            return
        params = {'order_id': int(order.order_id.split("|")[1])}
        resp = self.bitfinex_request('order/cancel', params).json()
        if resp and 'id' in resp and resp['id'] == params['order_id']:
            order.state = 'closed'
            order.order_id = order.order_id.replace('tmp', 'bitfinex')
            try:
                self.session.commit()
            except Exception as e:
                self.logger.exception(e)
                self.session.rollback()
                self.session.flush()

    def cancel_orders(self, market=None, side=None, oid=None, order_id=None):
        if market is None and side is None and oid is None and order_id is None:
            resp = self.bitfinex_request('order/cancel/all')
            if "orders successfully cancelled" in resp.text:
                # self.session.query(em.LimitOrder).filter(em.LimitOrder.exchange == 'bitfinex').update()
                update(em.LimitOrder).where(em.LimitOrder.exchange == 'bitfinex').values(state='closed')
        elif oid is not None or order_id is not None:
            order = self.session.query(em.LimitOrder)
            if oid is not None:
                order = order.filter(em.LimitOrder.id == oid).first()
            elif order_id is not None:
                order_id = order_id if "|" not in order_id else "bitfinex|%s" % order_id.split("|")[1]
                order = get_order_by_order_id(order_id, 'bitfinex', session=self.session)
            self.cancel_order(order=order)
        else:
            orders = self.get_open_orders(market=market)
            for o in orders:
                if market is not None and market != o.market:
                    continue
                if side is not None and side != o.side:
                    continue
                self.cancel_order(order=o)

    def create_order(self, oid, expire=None):
        order = self.session.query(em.LimitOrder).filter(em.LimitOrder.id == oid).first()
        if not order:
            self.logger.warning("unable to find order %s" % oid)
            if expire is not None and expire < time.time():
                submit_order('bitfinex', oid, expire=expire)  # back of the line!
            return
        market = self.unformat_market(order.market)
        amount = "{:0.5f}".format(order.amount.to_double()) if isinstance(order.amount, Amount) else float(order.amount)
        price = "{:0.5f}".format(order.price.to_double()) if isinstance(order.price, Amount) else float(order.price)
        side = 'buy' if order.side == 'bid' else 'sell'
        exch_pair = self.unformat_market(market)
        params = {
            'side': side,
            'symbol': exch_pair,
            'amount': amount,
            'price': price,
            'exchange': 'all',
            'type': 'exchange limit'
        }
        try:
            resp = self.bitfinex_request('order/new', params).json()
        except ValueError as e:
            self.logger.exception(e)
        if 'is_live' not in resp or not resp['is_live']:
            self.logger.warning("create order failed w/ %s" % resp)
            # Do nothing. The order can stay locally "pending" and be retried, if desired.
        else:
            order.order_id = 'bitfinex|%s' % resp['order_id']
            order.state = 'open'
            self.logger.debug("submitted order %s" % order)
            try:
                self.session.commit()
            except Exception as e:
                self.logger.exception(e)
                self.session.rollback()
                self.session.flush()
            return order

    def get_open_orders(self, market=None):
        try:
            rawos = self.bitfinex_request('orders').json()
        except ValueError as e:
            self.logger.exception(e)
        orders = []
        for o in rawos:
            if market is None or o['symbol'] == self.unformat_market(market):
                side = 'ask' if o['side'] == 'sell' else 'bid'
                # orders.append(em.LimitOrder(Amount("%s %s" % (o['price'], self.quote_commodity(market))),
                #                        Amount("%s %s" % (o['remaining_amount'], self.base_commodity(market))), side,
                #                        self.NAME, str(o['id'])))
                pair = self.format_market(o['symbol'])
                base = self.base_commodity(pair)
                amount = Amount("%s %s" % (o['remaining_amount'], base))
                exec_amount = Amount("%s %s" % (o['executed_amount'], base))
                quote = self.quote_commodity(pair)
                lo = None
                try:
                    lo = get_order_by_order_id(str(o['id']), 'bitfinex', session=self.session)
                except Exception as e:
                    self.logger.exception(e)
                if lo is None:
                    lo = em.LimitOrder(Amount("%s %s" % (o['price'], quote)), amount, pair, side,
                                       self.NAME, str(o['id']), exec_amount=exec_amount, state='open')
                    self.session.add(lo)
                else:
                    lo.state = 'open'
                orders.append(lo)
        try:
            self.session.commit()
        except Exception as e:
            self.logger.exception(e)
            self.session.rollback()
            self.session.flush()
        return orders

    def get_trades_history(self, begin=None, end=None, market='BTC_USD', limit=500):
        exch_pair = self.unformat_market(market)
        params = {'symbol': exch_pair, 'limit_trades': limit}
        if begin is not None:
            params['timestamp'] = str(begin)
        if end is not None:
            params['until'] = str(end)
        try:
            return self.bitfinex_request('mytrades', params).json()
        except ValueError as e:
            self.logger.exception(e)

    def get_dw_history(self, currency, begin=None, end=None):
        params = {'currency': currency.replace("DASH", "DRK")}
        if begin is not None:
            params['since'] = str(begin)
        if end is not None:
            params['until'] = str(end)
        try:
            return self.bitfinex_request('history/movements', params).json()
        except ValueError as e:
            self.logger.exception(e)

    def sync_trades(self, market=None, rescan=False):
        for market in json.loads(self.cfg.get('bitfinex', 'live_pairs')) + ["DRK_BTC", "DRK_USD"]:
            market = market.replace('DRK', 'DASH')
            allknown = False
            end = time.time()
            while not allknown:
                trades = self.get_trades_history(end=end, market=market)
                if len(trades) == 0 or 'amount' not in trades[0]:
                    break
                allknown = True
                for row in trades:
                    found = self.session.query(em.Trade) \
                        .filter(em.Trade.trade_id == 'bitfinex|%s' % row['tid']) \
                        .count()
                    if found != 0:
                        print "; %s already known" % row['tid']
                        continue
                    allknown = False
                    if float(row['timestamp']) < end:
                        end = float(row['timestamp'])
                    dtime = datetime.datetime.fromtimestamp(float(row['timestamp']))
                    price = float(row['price'])
                    amount = abs(float(row['amount']))
                    fee = abs(float(row['fee_amount']))
                    fee_side = 'base' if row['fee_currency'].replace('DRK', 'DASH') == market.split("_")[0] else 'quote'
                    side = row['type'].lower()
                    self.session.add(em.Trade(row['tid'], 'bitfinex', market, side,
                                              amount, price, fee, fee_side, dtime))
        self.session.commit()

    def sync_credits(self, rescan=False):
        for cur in self.active_currencies.union(set(["DRK"])):
            allknown = False
            end = time.time()
            while not allknown:
                history = self.get_dw_history(cur, end=end)
                if len(history) == 0:
                    break
                allknown = True
                for row in history:
                    if row['status'] != 'COMPLETED':
                        continue
                    rtype = row['type'].lower()
                    found = 0
                    if rtype == "withdrawal":
                        found = self.session.query(wm.Debit) \
                            .filter(wm.Debit.ref_id == 'bitfinex|%s' % row['id']) \
                            .count()
                    elif rtype == "deposit":
                        found = self.session.query(wm.Credit) \
                            .filter(wm.Credit.ref_id == 'bitfinex|%s' % row['id']) \
                            .count()
                    if found != 0:
                        print "; %s already known" % row['id']
                        continue
                    allknown = False
                    if float(row['timestamp']) < end:
                        end = float(row['timestamp'])
                    dtime = datetime.datetime.fromtimestamp(float(row['timestamp']))
                    asset = self.format_commodity(row['currency'])
                    amount = Amount("%s %s" % (row['amount'], asset))
                    if row['status'] == 'COMPLETED':
                        status = 'complete'
                    elif row['status'] == 'CANCELED':
                        status = 'canceled'
                    else:
                        status = 'unconfirmed'
                    if rtype == "withdrawal":
                        self.session.add(
                            wm.Debit(amount, 0, row['address'], asset, "bitfinex", status, "bitfinex",
                                     "bitfinex|%s" % row['id'],
                                     self.manager_user.id, dtime))
                    elif rtype == "deposit":
                        self.session.add(wm.Credit(amount, row['address'], asset, "bitfinex", status, "bitfinex",
                                                   "bitfinex|%s" % row['id'],
                                                   self.manager_user.id, dtime))
                self.session.commit()

    sync_debits = sync_credits


def main():
    bitfinex = Bitfinex()
    bitfinex.run()


if __name__ == "__main__":
    main()
