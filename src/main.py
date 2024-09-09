from constants import logging, O_CNFG, O_SETG, O_FUTL
from stock_brokers.bypass.bypass import Bypass
from stock_brokers.zerodha.zerodha import Zerodha
from websocket import Wsocket
import polars as pl
import pendulum as pdlm
import time
# from datetime import datetime, timedelta



def generate_ohlc(df: pl.DataFrame, interval='1m', output_file='ohlc.csv'):
    """
    To generate ohlc data from tick data and save it in csv file.
    """
    # Calculate OHLC.
    ohlc = (
        df.group_by([pl.col('time').dt.truncate(interval), 'instrument_token'])
        .agg([
            pl.col('ltp').first().alias('open'),
            pl.col('ltp').max().alias('high'),
            pl.col('ltp').min().alias('low'),
            pl.col('ltp').last().alias('close'),
            pl.col('qty').sum().alias('qty'),
            pl.col('dopen').last().alias('dopen'),
            pl.col('dhigh').last().alias('dhigh'),
            pl.col('dlow').last().alias('dlow'),
            pl.col('dclose').last().alias('dclose'),
        ])
    )

    ohlc = ohlc.sort(['time', 'instrument_token'])

    # Save to CSV.
    ohlc.write_csv(output_file)

    # Remove used tick data.
    # last_time = df['time'].max()
    # cutoff_time = last_time - timedelta(minutes=1)
    # df = df.filter(pl.col('time') > cutoff_time)
    return df


def broker(userid, password, totp, api_key=None, api_secret=None, broker='zerodha', sec_dir='./'):
    try:
        if broker == 'bypass':
            tokpath = sec_dir + userid + ".txt"
            bypass = Bypass(userid=userid, password=password, totp=totp, tokpath=tokpath)
            if bypass.authenticate():
                enctoken = bypass.kite.enctoken
                if enctoken:
                    with open(tokpath, "w") as tw:
                        tw.write(enctoken)
                    return bypass
                print('Either Token is expired or Invalid Details entered.')
        elif broker == 'zerodha':
            kite = Zerodha(userid=userid, password=password, totp=totp, api_key=api_key, secret=api_secret)
            if kite.authenticate():
                return kite
            print('Either Token is expired or Invalid Details entered.')
    except Exception as e:
        print(f"unable to create bypass object  {e}")


def main():
    cond = dict(userid=O_CNFG['broker'].get('userid'), password=O_CNFG['broker'].get('password'), totp=O_CNFG['broker'].get('totp'), api_key=O_CNFG['broker'].get('api_key'), api_secret=O_CNFG['broker'].get('api_secret'), broker=O_CNFG['broker'].get('name'))
    kite = broker(**cond)
    if not kite:
        print(f'Broker not found: {kite}, exiting...')
        exit(0)

    # Instrument download
    data = kite.kite.instruments('NSE')
    if not data:
        print('No symbol data found, exiting...')
        exit(0)

    df = pl.DataFrame(data)
    # Columns...
    # ['instrument_token', 'exchange_token', 'tradingsymbol', 'name', 'last_price', 'expiry', 'strike', 'tick_size', 'lot_size', 'instrument_type', 'segment', 'exchange']
    df = df.filter(pl.col('segment') == "NSE")
    tokens = df['instrument_token'][:1500].to_list()

    # Start websockets
    ws = Wsocket(kite)
    while not ws.is_connected():
        time.sleep(1)
    # Subscribing first 1500 tokens.
    ws.subscribe(tokens, 'quote')
    print(f'{len(tokens)} Token subscribed...')

    ttime =  pdlm.now().add(minutes=1, seconds=5)
    print(f'[{time.ctime()}] OHLC Computation Engine Started...')
    print(f'[{time.ctime()}] Waiting for data...')
    ptime = pdlm.now()
    gdf = None
    
    while True:
        if len(ws.df) == 0:
            time.sleep(5)
            continue
        if pdlm.now() > ptime and len(ws.df) > 1:
            ptime = ptime.add(seconds=30)
            if gdf is None and len(ws.df) > 1:
                gdf: pl.DataFrame = ws.df[0]
                ws.df.pop(0)
            elif len(ws.df) >= 2:
                gdf = pl.concat([gdf, ws.df[0]])
                ws.df.pop(0)
            else: continue
            print(f'[{time.ctime()}] Saving Tick data...')
            gdf = gdf.sort(['time', 'instrument_token'])
            gdf.write_csv('tick.csv')

        if pdlm.now() > ttime and gdf is not None:
            print(time.ctime(), gdf.count())
            print(f'[{time.ctime()}] calculating ohlc...')
            stt = time.perf_counter()
            generate_ohlc(gdf)
            spt = time.perf_counter()
            print(f'Time Taken: {spt-stt:5f} secs.')
            print(f'[{time.ctime()}] Ohlc calculation done...')
            print(f'[{time.ctime()}] Waiting for data...')
            ttime = ttime.add(minutes=1, seconds=2)

        time.sleep(2)


if __name__ == "__main__":
    main()
