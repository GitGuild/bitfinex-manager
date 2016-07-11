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
from trade_manager import CFG, em, wm, ExchangeError, make_ledger
from trade_manager.plugin import InternalExchangePlugin

NAME = 'bitfinex'
KEY = CFG.get('bitfinex', 'key')

BASE_URL = 'https://api.bitfinex.com'
REQ_TIMEOUT = 10  # seconds


def unadjust_currency(c):
    if len(c) > 3 and c[0] in "XZ":
        c = c[1:]
    if c == "XBT":
        c = "BTC"
    return c


class Bitfinex(InternalExchangePlugin):
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
        params = params or {}
        params['request'] = endpoint
        response = None
        while response is None:
            try:
                response = requests.post(url=BASE_URL + params['request'],
                                         headers=self.bitfinex_encode(params),
                                         timeout=REQ_TIMEOUT)
                if "Nonce is too small." in response:
                    response = None
            except (ConnectionError, Timeout) as e:
                raise ExchangeError('bitfinex', '%s %s while sending to bitfinex %r' % (type(e), str(e), params))
        return response

    @classmethod
    def format_pair(cls, pair):
        """
        formatted : unformatted
        'BTC_USD': 'btcusd'
        'LTC_USD': 'ltcusd'
        'LTC_BTC': 'ltcbtc'
        'ETH_USD': 'ethusd'
        'ETH_BTC': 'ethbtc'
        """
        if pair[0] == 'l':
            base = 'LTC'
            if pair[2:] == 'btc':
                quote = 'BTC'
            else:
                quote = 'USD'
        elif pair[0] == 'e':
            base = 'ETH'
            if pair[2:] == 'btc':
                quote = 'BTC'
            else:
                quote = 'USD'
        elif pair[0] == 'b':
            base = 'BTC'
            quote = 'USD'
        return base + '_' + quote

    @classmethod
    def unformat_pair(cls, pair):
        try:
            base, quote = pair.lower().split("_")
        except ValueError:
            base = pair[:3].lower()
            quote = pair[4:].lower()
        base = base.replace('dash', 'drk')
        return "%s%s" % (base, quote)

    def base_currency(self, pair):
        bcurr = pair[:3]
        return bcurr

    def quote_currency(self, pair):
        qcurr = pair[4:]
        return qcurr

    def cancel_order(self, order_id, pair=None):
        params = {'order_id': int(order_id)}
        try:
            resp = self.bitfinex_request('/v1/order/cancel', params).json()
        except ValueError as e:
            raise ExchangeError('bitfinex', '%s %s while sending to bitfinex %r' % (type(e), str(e), params))
        if resp and 'id' in resp and resp['id'] == params['order_id']:
            return True
        elif 'message' in resp and resp['message'] == 'Order could not be cancelled.':
            return True
        else:
            return False

    def cancel_orders(self, pair=None, **kwargs):
        resp = self.bitfinex_request('/v1/order/cancel/all')
        # example respose:
        # {"result":"4 orders successfully cancelled"}
        if "orders successfully cancelled" in resp.text:
            return True
        else:
            return False

    def create_order(self, amount, price, otype, pair, typ='exchange limit', bfxexch='all'):
        if CFG.get('bitfinex', 'BLOCK_ORDERS'):
            return "order blocked"
        if otype == 'bid':
            otype = 'buy'
        elif otype == 'ask':
            otype = 'sell'
        else:
            raise Exception('unknown side %r' % otype)
        exch_pair = self.unformat_pair(pair)
        params = {
            'side': otype,
            'symbol': exch_pair,
            'amount': "{:0.4f}".format(amount),
            'price': "{:0.4f}".format(price),
            'exchange': bfxexch,
            'type': typ
        }
        try:
            order = self.bitfinex_request('/v1/order/new', params).json()
        except ValueError as e:
            raise ExchangeError('bitfinex', '%s %s while sending to bitfinex %r' % (type(e), str(e), params))

        if 'is_live' in order and order['is_live']:
            return str(order['order_id'])
        raise ExchangeError('bitfinex', 'unable to create order %r response was %r' % (params, order))

    @classmethod
    def format_book_item(cls, item):
        return super(Bitfinex, cls).format_book_item((item['price'], item['amount']))

    @classmethod
    def unformat_book_item(cls, item):
        return {'price': str(item[0]), 'amount': str(item[1])}

    def get_balance(self, btype='total'):
        try:
            data = self.bitfinex_request('/v1/balances').json()
        except ValueError as e:
            raise ExchangeError('bitfinex', '%s %s while sending to bitfinex get_balance' % (type(e), str(e)))
        if 'message' in data:
            raise ExchangeError(exchange='bitfinex', message=data['message'])
        relevant = filter(lambda x: x['currency'].upper() in self.active_currencies, data)

        if btype == 'total':
            total = Balance(*map(lambda x: Amount("%s %s" % (x['amount'], x['currency'].upper())), relevant))
            return total
        elif btype == 'available':
            available = Balance(*map(lambda x: Amount("%s %s" % (x['available'], x['currency'].upper())), relevant))
            return available
        else:
            total = Balance(*map(lambda x: Amount("%s %s" % (x['amount'], x['currency'].upper())), relevant))
            available = Balance(*map(lambda x: Amount("%s %s" % (x['available'], x['currency'].upper())), relevant))
            return total, available

    def get_open_orders(self, pair):
        exch_pair = self.unformat_pair(pair)
        try:
            rawos = self.bitfinex_request('/v1/orders').json()
        except ValueError as e:
            raise ExchangeError('bitfinex', '%s %s while sending to bitfinex get_open_orders' % (type(e), str(e)))
        orders = []
        for o in rawos:
            if o['symbol'] == exch_pair:
                side = 'ask' if o['side'] == 'sell' else 'bid'
                orders.append(em.Order(Amount("%s %s" % (o['price'], self.quote_currency(pair))),
                                       Amount("%s %s" % (o['remaining_amount'], self.base_currency(pair))), side,
                                       self.NAME, str(o['id'])))
            else:
                pass
        return orders

    @classmethod
    def get_order_book(cls, pair=None, **kwargs):
        exch_pair = cls.unformat_pair(pair)
        try:
            return requests.get('%s/v1/book/%s' % (BASE_URL, exch_pair),
                                timeout=REQ_TIMEOUT).json()
        except ValueError as e:
            raise ExchangeError('bitfinex', '%s %s while sending to bitfinex get_order_book' % (type(e), str(e)))

    @classmethod
    def get_ticker(cls, pair=None):
        exch_pair = cls.unformat_pair(pair)
        try:
            rawtick = requests.get(BASE_URL + '/v1/pubticker/%s' % exch_pair, timeout=REQ_TIMEOUT).json()
        except (ConnectionError, Timeout, ValueError) as e:
            raise ExchangeError('bitfinex', '%s %s while sending get_ticker to bitfinex' % (type(e), str(e)))

        return create_ticker(bid=rawtick['bid'], ask=rawtick['ask'], high=rawtick['high'], low=rawtick['low'],
                             volume=rawtick['volume'], last=rawtick['last_price'], timestamp=rawtick['timestamp'],
                             currency=cls.quote_currency(pair), vcurrency=cls.base_currency(pair))

    def get_trades_history(self, begin=None, end=None, pair='BTC_USD', limit=50):
        exch_pair = self.unformat_pair(pair)
        params = {'symbol': exch_pair.replace("/_", ""), 'limit_trades': limit, 'reverse': 1}
        if begin == 'last':
            last = self.session.query(em.Trade)\
                        .filter(em.Trade.exchange=='bitfinex') \
                        .filter(em.Trade.market == pair) \
                        .order_by(em.Trade.time.desc())\
                        .first()
            if last:
                params['timestamp'] = str(time.mktime(last.time.timetuple()))
        elif begin is not None:
            params['timestamp'] = str(begin)
        if end is not None:
            params['until'] = str(end)
        try:
            return self.bitfinex_request('/v1/mytrades', params).json()
        except ValueError as e:
            raise ExchangeError('bitfinex', '%s %s while sending to bitfinex get_trades_history' % (type(e), str(e)))

    def get_dw_history(self, currency, begin, end):
        params = {'currency': currency.replace("DASH", "DRK")}
        if begin == 'last':
            last = self.session.query(em.Trade)\
                        .filter(em.Trade.exchange=='bitfinex')\
                        .order_by(em.Trade.time.desc())\
                        .first()
            if last:
                params['since'] = str(time.mktime(last.time.timetuple()))
        elif begin is not None:
            params['since'] = str(begin)
        if end is not None:
            params['until'] = str(end)
        try:
            return self.bitfinex_request('/v1/history/movements', params).json()
        except ValueError as e:
            raise ExchangeError('bitfinex', '%s %s while sending to bitfinex get_dw_history' % (type(e), str(e)))

    def get_active_positions(self):
        try:
            return self.bitfinex_request('/v1/positions').json()
        except ValueError as e:
            raise ExchangeError('bitfinex', '%s %s while sending to bitfinex get_active_positions' % (type(e), str(e)))

    def get_order_status(self, order_id):
        params = {'order_id': int(order_id)}
        try:
            return self.bitfinex_request('/v1/order/status', params).json()
        except ValueError as e:
            raise ExchangeError('bitfinex', '%s %s while sending to bitfinex get_order_status for %s' % (
                type(e), str(e), str(order_id)))

    def get_deposit_address(self):
        """
        # TODO implement for multicurrency

        request parmas
        Key         Type      Description
        method	    [string]  Method of deposit (accepted:
                                  'bitcoin', 'litecoin', 'ethereum'.)
        wallet_name [string]  Your wallet needs to already exist.
                              Wallet to deposit in (accepted:
                                  'trading', 'exchange', 'deposit')
        renew	    [integer] (optional) Default is 0.
                              If set to 1, will return a new unused deposit address

        response
        result	    [string]   'success' or 'error
        method	    [string]
        currency    [string]
        address	    [string]	The deposit address (or error message if result = 'error')
        """
        try:
            result = self.bitfinex_request('/v1/deposit/new', {'currency': 'BTC', 'method': 'bitcoin',
                                                               'wallet_name': 'exchange'}).json()
            if result['result'] == 'success' and 'address' in result:
                return str(result['address'])
            else:
                raise ExchangeError('bitfinex', result)
        except ValueError as e:
            raise ExchangeError('bitfinex', '%s %s while sending get_deposit_address' % (type(e), str(e)))

    def account_info(self):
        try:
            data = self.bitfinex_request('/v1/account_infos').json()
        except ValueError as e:
            raise ExchangeError('bitfinex', '%s %s while sending to bitfinex get_open_orders' % (type(e), str(e)))
        return data

    def save_trades(self, begin='last', end=None, pair=None):
        for market in json.loads(CFG.get('bitfinex', 'live_pairs')) + ["DRK_BTC", "DRK_USD"]:
            tmpbegin = begin
            market = market.replace('DRK', 'DASH')
            allknown = False
            while not allknown:
                trades = self.get_trades_history(begin=tmpbegin, end=end, pair=market)
                if len(trades) == 0 or 'amount' not in trades[0]:
                    break
                allknown = True
                for row in trades:
                    found = self.session.query(em.Trade)\
                            .filter(em.Trade.trade_id=='bitfinex|%s' % row['tid'])\
                            .count()
                    if found != 0:
                        print "; %s already known" % row['tid']
                        continue
                    allknown = False
                    if float(row['timestamp']) > tmpbegin:
                        tmpbegin = float(row['timestamp'])
                    dtime = datetime.datetime.fromtimestamp(float(row['timestamp']))
                    price = float(row['price'])
                    amount = abs(float(row['amount']))
                    fee = abs(float(row['fee_amount']))
                    fee_side = 'base' if row['fee_currency'].replace('DRK', 'DASH') == market.split("_")[0] else 'quote'
                    side = row['type'].lower()
                    self.session.add(em.Trade(row['tid'], 'bitfinex', market, side,
                            amount, price, fee, fee_side, dtime))
        self.session.commit()

    def save_credits(self, begin='last', end=None):
        for cur in self.active_currencies.union(set(["DRK"])):
            tmpbegin = begin
            allknown = False
            while not allknown:
                history = self.get_dw_history(cur, begin=tmpbegin, end=end)
                if len(history) == 0:
                    break
                allknown = True
                for row in history:
                    if row['status'] != 'COMPLETED':
                        continue
                    rtype = row['type'].lower()
                    found = 0
                    if rtype == "withdrawal":
                        found = self.session.query(wm.Debit)\
                                .filter(wm.Debit.ref_id=='bitfinex|%s' % row['id'])\
                                .count()
                    elif rtype == "deposit":
                        found = self.session.query(wm.Credit)\
                                .filter(wm.Credit.ref_id=='bitfinex|%s' % row['id'])\
                                .count()
                    if found != 0:
                        print "; %s already known" % row['id']
                        continue
                    allknown = False
                    if float(row['timestamp']) > tmpbegin:
                        tmpbegin = float(row['timestamp'])
                    dtime = datetime.datetime.fromtimestamp(float(row['timestamp']))
                    asset = unadjust_currency(row['currency']).replace("DRK", "DASH")
                    amount = Amount("%s %s" % (row['amount'], asset))
                    if row['status'] == 'COMPLETED':
                        status = 'complete'
                    elif row['status'] == 'CANCELED':
                        status = 'canceled'
                    else:
                        status = 'unconfirmed'
                    if rtype == "withdrawal":
                        self.session.add(
                            wm.Debit(amount, 0, row['description'], asset, "bitfinex", status, "bitfinex", "bitfinex|%s" % row['id'],
                                     self.get_manager_user().id, dtime))
                    elif rtype == "deposit":
                        self.session.add(wm.Credit(amount, row['description'], asset, "bitfinex", status, "bitfinex", "bitfinex|%s" % row['id'],
                                         self.get_manager_user().id, dtime))
                self.session.commit()

    save_debits = save_credits


if __name__ == "__main__":
    bitfinex = Bitfinex()
    bitfinex.save_trades()
    bitfinex.save_credits()
    bitfinex.save_debits()
    ledger = make_ledger(exchange='bitfinex')
    print ledger
