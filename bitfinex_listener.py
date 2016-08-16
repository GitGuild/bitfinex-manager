import json
import datetime
import isodate
import websocket
import thread
import time

from alchemyjsonschema.dictify import datetime_rfc3339
from tapp_config import setup_redis, get_config, setup_logging
from trade_manager.plugin import get_active_markets
# from sqlalchemy_models import wallet as wm
from bitfinex_manager import bitfinex_sign, Bitfinex

red = setup_redis()

channels = {}
bitfinex = Bitfinex()
logger = setup_logging('bitfinex_listener', bitfinex.cfg)
bitfinex.setup_connections()
bitfinex.setup_logger()  # will be actually use the logger above


def on_message(ws, message):
    if '"hb"' in message:
        return
    mess = json.loads(message)
    if isinstance(mess, dict) and "event" in mess:
        if mess["event"] == "subscribed":
            if mess["channel"] == "ticker":
                market = ("%s_%s" % (mess["pair"][:3], mess["pair"][3:])).replace("DRK", "DASH")
                channels[str(mess["chanId"])] = {"channel": mess["channel"], "market": market}
                logger.info("subscribed to %s channel %s" % (mess["channel"], mess["chanId"]))
        elif mess["event"] == "auth":
            if mess["status"] == "FAIL":
                logger.exception("ERROR: auth failed")
            else:
                channels[str(mess["chanId"])] = {"channel": "account", "userId": mess["userId"]}
                logger.info("subscribed to account channel %s" % mess["chanId"])
    elif isinstance(mess, list) and len(mess) > 0:
        mchan = str(mess[0])
        if mchan in channels:
            if channels[mchan]["channel"] == "ticker":
                bid = mess[1]
                # bid_size = mess[2]
                ask = mess[3]
                # ask_size = mess[4]
                # daily_change = mess[5]
                # daily_change_perc = mess[6]
                last = mess[7]
                volume = mess[8]
                high = mess[9]
                low = mess[10]
                jtick = {'bid': bid, 'ask': ask, 'last': last, 'high': high, 'low': low, 'volume': volume,
                         'market': channels[mchan]['market'], 'exchange': 'bitfinex',
                         'time': datetime_rfc3339(datetime.datetime.utcnow())}
                red.set('bitfinex_%s_ticker' % channels[mchan]["market"], json.dumps(jtick))
                logger.debug("set bitfinex %s ticker %s" % (channels[mchan]['market'], jtick))
            elif channels[mchan]["channel"] == "account":
                subchan = mess[1]
                logger.info("subchan %s" % subchan)
                changed = False
                if subchan == "ws":  # wallet update
                    for wallet in mess[2]:
                        wname = wallet[0]
                        wcomm = wallet[1]
                        wbal = wallet[2]
                        # w_interest_unsettled = wallet[3]
                        # available = '?'
                        if wname == 'exchange':
                            changed = True
                            bitfinex.update_balance(wcomm, wbal, None, "")
                elif subchan == "ts":  # trade update
                    for trade in mess[2]:
                        logger.debug("trade details {0}".format(trade))
                        tid = str(trade[0])
                        tpair = "%s_%s" % (trade[1][:3], trade[1][3:])
                        ttime = datetime.datetime.fromtimestamp(float(trade[2]))
                        # tord_id = str(trade[3])
                        tamtexec = float(trade[4])
                        tside = 'buy' if tamtexec > 0 else 'sell'
                        # tpriceexec = trade[5]
                        # ttype = trade[6]
                        tprice = float(trade[7])
                        # if trade[8] is None:
                        #     logger.warning("found odd trade %s" % trade)
                        #     continue
                        tfee = abs(float(trade[8])) if trade[8] is not None else 0
                        tfeecomm = trade[9] if trade[9] is not None else "quote"
                        fee_side = "base" if tpair.find(tfeecomm) == 0 else "quote"
                        trade = bitfinex.add_trade(market=tpair, tid=tid, trade_side=tside, price=tprice,
                                                   amount=abs(tamtexec), fee=abs(tfee), fee_side=fee_side, dtime=ttime)
                        if trade is not None:
                            changed = True
                elif "os" in subchan:  # order update
                    # logger.debug("order update %s" % message)
                    for order in mess[2]:
                        logger.debug("order details %s" % mess[2])
                        oid = str(order[0])
                        opair = ("%s_%s" % (order[1][:3], order[1][3:])).replace("DRK", "DASH")
                        oamount = order[2]
                        oside = 'ask' if oamount < 0 else 'bid'
                        oamount_origin = order[3]
                        exec_amount = oamount_origin - oamount
                        # otype = order[4]
                        ostatus = order[5]
                        ostate = 'closed' if ostatus == 'CANCELED' or \
                                 'EXECUTED' in ostatus and oamount == 0 else 'open'
                        oprice = order[6]
                        # oprice_avg = order[7]
                        ocreated = isodate.parse_datetime(order[8])
                        # onotify = order[9]
                        # ohidden = order[10]
                        # ooco = order[11]
                        order = bitfinex.add_order(oprice, abs(oamount_origin), opair, oside, order_id=oid,
                                                   create_time=ocreated, exec_amount=abs(exec_amount),
                                                   state=ostate)
                        if order is not None:
                            changed = True
                if changed:
                    try:
                        logger.info("commit %s" % bitfinex.session.commit())
                    except Exception as e:
                        logger.exception(e)
                        bitfinex.session.rollback()
                        bitfinex.session.flush()


def on_error(ws, error):
    logger.exception(error)


def on_close(ws):
    logger.info("Bitfinex listener closed")


def on_open(ws):
    def run(*args):
        # subscribe to tickers
        markets = get_active_markets('bitfinex')
        for market in markets:
            ws.send(json.dumps({"event": "subscribe", "channel": "ticker", "pair": market.replace("_", "")}))
        # subscribe to balances
        payload = "AUTH"+str(time.time())
        headers = bitfinex_sign(key=bitfinex.key, secret=bitfinex.secret, msg=payload)
        ws.send(json.dumps({"event": "auth", "apiKey": bitfinex.key, "authSig": headers['X-BFX-SIGNATURE'],
                            "authPayload": payload}))
        while True:
            time.sleep(0.1)
        # time.sleep(1)
        # ws.close()
        # print "thread terminating..."
    thread.start_new_thread(run, ())


def main():
    ws = websocket.WebSocketApp("wss://api2.bitfinex.com:3000/ws",
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.on_open = on_open
    ws.run_forever()


if __name__ == "__main__":
    main()
