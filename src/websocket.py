from stock_brokers.bypass.bypass import Bypass
from stock_brokers.zerodha.zerodha import Zerodha
from kiteconnect import KiteTicker
import pendulum
import polars as pl
import logging


class Wsocket:
    def __init__(self, api, sym_tkn=None):
        self.ticks = []
        # List of DataFrame
        self.df = []
        self.dftime = pendulum.now()

        if isinstance(api, Bypass):
            self.kws = api.kite.kws()
        elif isinstance(api, Zerodha):
            self.kws = KiteTicker(api_key=api.kite.api_key, access_token=api.kite.access_token)

        if isinstance(sym_tkn, list):
            self.sym_tkn = sym_tkn
        else:
            self.sym_tkn = []
        # Assign the callbacks.
        self.kws.on_ticks = self.on_ticks
        self.kws.on_connect = self.on_connect
        self.kws.on_close = self.on_close
        self.kws.on_error = self.on_error
        self.kws.on_reconnect = self.on_reconnect
        self.kws.on_noreconnect = self.on_noreconnect

        # Infinite loop on the main thread. Nothing after this will run.
        # You have to use the pre-defined callbacks to manage subscriptions.
        self.kws.connect(threaded=True)

    def on_ticks(self, ws, ticks):
        # Callback to receive ticks.
        if ticks:
            time = pendulum.now()
            data = []
            # print(ticks)
            for t in ticks:
                dohlc = t.get('ohlc')
                open = high = low = close = None
                if dohlc:
                    open = dohlc.get('open')
                    high = dohlc.get('high')
                    low = dohlc.get('low')
                    close = dohlc.get('close')
                dd = dict(time=time, ltp=t.get('last_price'), instrument_token=t.get('instrument_token'), qty=t.get('last_traded_quantity'), volume=t.get('volume_traded'), dopen=open, dhigh=high, dlow=low, dclose=close)
                if not dd: continue
                data.append(dd)
            df = pl.DataFrame(data)
            df = df.with_columns(pl.col('time').dt.convert_time_zone('Asia/Kolkata'))
            self._save_tick_data(df=df, time=time)
            self.last_tick = pendulum.now()
        else:
            print(f'Not found a tick, {ticks}.')

    def _save_tick_data(self, df, time: pendulum.DateTime):
        if len(self.df) >= 1 and (self.dftime.minute == time.minute):
            odf = self.df[-1]
            self.df[-1] = pl.concat([odf, df])
        else:
            self.df.append(df)
            self.dftime = pendulum.now()
        
        # Old Code.
        # if len(self.df) >= 1 and (time.second % 59 != 0):
        #     odf = self.df[-1]
        #     self.df[-1] = pl.concat([odf, df])
        # else:
        #     self.df.append(df)

    def on_connect(self, ws, response):
        # Callback on successful connect.
        # Subscribe to a list of instrument_tokens.
        if self.sym_tkn:
            ws.set_mode(ws.MODE_LTP, self.sym_tkn)
        print('Websocket Connected successfully...')

    def on_close(self, ws, code, reason):
        # On connection close stop the main loop
        print('Websocket Connection Closed...')
        
    def on_error(self, ws, code, reason):
        # Callback when connection closed with error.
        logging.info("Connection error: {code} - {reason}".format(code=code, reason=reason))

    # Callback when all reconnect failed (exhausted max retries)
    def on_reconnect(self, ws, attempts_count):
        # Callback when reconnect is on progress
        logging.info("Reconnecting: {}".format(attempts_count))

    # websocket connection status.
    def is_connected(self):
        return self.kws.is_connected()

    def on_noreconnect(self, ws):
        logging.info("Reconnect failed.")

    def subscribe(self, tokens: list[int], mode='ltp'):
        if isinstance(tokens, int):
            tokens = [tokens]
        if mode.lower() not in ('ltp', 'quote', 'full'):
            mode = 'ltp'
        self.sym_tkn.extend(tokens)
        self.kws.set_mode(mode, tokens)

    def unsubscribe(self, tokens: list[int]):
        if isinstance(tokens, int):
            tokens = [tokens]
        if self.kws.unsubscribe(tokens):
            for t in tokens:
                if t in self.sym_tkn: 
                    self.sym_tkn.remove(t)
            return True

    def unsubsribe_all(self):
        if not isinstance(self.sym_tkn, list): return
        # Unsubscribe All tokens.
        if self.kws.unsubscribe(self.sym_tkn):
            self.sym_tkn = []
            return True
        
    def is_connected(self):
        return self.kws.is_connected()
        
    def close_connection(self):
        self.kws.close()



