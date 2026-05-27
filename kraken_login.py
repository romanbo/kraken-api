#!/usr/bin/env python3

# Kraken Rest API + WebSocket streaming + historical data download
#
# Usage: ./krakenapi.py endpoint [parameters] [-pretty] [--stream] [--history]
# Example: ./krakenapi.py Time
# Example: ./krakenapi.py OHLC pair=xbtusd interval=1440
# Example: ./krakenapi.py Balance
# Example: ./krakenapi.py TradeBalance asset=xdg
# Example: ./krakenapi.py OpenPositions
# Example: ./krakenapi.py AddOrder pair=xxbtzusd type=buy ordertype=market volume=0.003 leverage=5
# Example: ./krakenapi.py Ticker pair=BTC/USD,ETH/USD
# Example: ./krakenapi.py Ticker pair=BTC/USD,ETH/USD --stream
# Example: ./krakenapi.py Ticker pair=BTC/USD,ETH/USD --stream -pretty
# Example: ./krakenapi.py OHLC pair=xbtusd interval=1440 --history
# Example: ./krakenapi.py OHLC pair=xbtusd interval=60 since=2024-01-01 --history > ohlc.csv
# Example: ./krakenapi.py Trades pair=xbtusd --history
# Example: ./krakenapi.py Trades pair=xbtusd since=2024-01-01 --history > trades.csv

import sys
import time
import base64
import hashlib
import hmac
import urllib.request
import json
import socket
import ssl
import struct
import os
import datetime

api_public = {"Time", "Assets", "AssetPairs", "Ticker", "OHLC", "Depth", "Trades", "Spread", "SystemStatus"}
api_private = {"Balance", "BalanceEx", "TradeBalance", "OpenOrders", "ClosedOrders", "QueryOrders", "TradesHistory", "QueryTrades", "OpenPositions", "Ledgers", "QueryLedgers", "TradeVolume", "AddExport", "ExportStatus", "RetrieveExport", "RemoveExport", "GetWebSocketsToken", "CreateSubaccount", "AccountTransfer"}
api_trading = {"AddOrder", "AddOrderBatch", "EditOrder", "CancelOrder", "CancelOrderBatch", "CancelAll", "CancelAllOrdersAfter"}
api_funding = {"DepositMethods", "DepositAddresses", "DepositStatus", "WithdrawInfo", "Withdraw", "WithdrawStatus", "WithdrawCancel", "WalletTransfer"}
api_staking = {"Earn/Strategies", "Earn/Allocations", "Earn/Allocate", "Earn/Deallocate", "Earn/AllocateStatus", "Earn/DeallocateStatus", "Staking/Assets", "Staking/Balance", "Stake", "Unstake", "Staking/Pending", "Staking/Transactions"}

api_domain = "https://api.kraken.com"
WS_HOST = "ws.kraken.com"
WS_PATH = "/v2"

api_data = ""
output_format = 0
stream_mode = False
history_mode = False

if len(sys.argv) < 2:
    api_method = "Time"
elif len(sys.argv) == 2:
    api_method = sys.argv[1]
else:
    api_method = sys.argv[1]
    for count in range(2, len(sys.argv)):
        if sys.argv[count] == '-pretty':
            output_format = 1
            continue
        if sys.argv[count] == '--stream':
            stream_mode = True
            continue
        if sys.argv[count] == '--history':
            history_mode = True
            continue
        if count == 2:
            api_data = sys.argv[count]
        else:
            api_data = api_data + "&" + sys.argv[count]


def parse_params(data_str):
    params = {}
    for item in data_str.split('&'):
        if '=' in item:
            k, v = item.split('=', 1)
            params[k] = v
    return params


def parse_since(s):
    try:
        return int(s)
    except ValueError:
        pass
    for fmt in ('%Y-%m-%d', '%Y-%m-%dT%H:%M:%S', '%Y/%m/%d'):
        try:
            return int(datetime.datetime.strptime(s, fmt).timestamp())
        except ValueError:
            continue
    print(f"Cannot parse date '{s}' — use YYYY-MM-DD or a Unix timestamp", file=sys.stderr)
    sys.exit(1)


def _public_get(endpoint, params):
    qs = '&'.join(f"{k}={v}" for k, v in params.items())
    req = urllib.request.Request(f"{api_domain}/0/public/{endpoint}?{qs}")
    req.add_header("User-Agent", "Kraken REST API")
    return json.loads(urllib.request.urlopen(req).read().decode())


# --- Historical data download ---

def download_ohlc_history(pair, interval=1440, since=None):
    print("time,open,high,low,close,vwap,volume,count")
    total = 0
    while True:
        params = {'pair': pair, 'interval': str(interval)}
        if since is not None:
            params['since'] = str(since)
        try:
            data = _public_get('OHLC', params)
        except Exception as e:
            print(f"\nRequest failed: {e}", file=sys.stderr)
            sys.exit(1)
        if data.get('error'):
            print(f"\nAPI error: {data['error']}", file=sys.stderr)
            sys.exit(1)
        result = data['result']
        last = result.get('last')
        candles = next((v for k, v in result.items() if k != 'last'), [])
        if not candles:
            break
        for c in candles:
            print(','.join(str(x) for x in c))
        total += len(candles)
        print(f"  {total} candles...", file=sys.stderr, end='\r')
        if last is None or last == since:
            break
        since = last
        time.sleep(0.5)
    print(f"\nDone. {total} candles total.", file=sys.stderr)


def download_trades_history(pair, since=None):
    print("price,volume,time,side,type,misc,trade_id")
    total = 0
    # Trades endpoint uses nanosecond timestamps; convert user-provided seconds
    since_ns = str(since * 1_000_000_000) if since is not None else None
    while True:
        params = {'pair': pair}
        if since_ns is not None:
            params['since'] = since_ns
        try:
            data = _public_get('Trades', params)
        except Exception as e:
            print(f"\nRequest failed: {e}", file=sys.stderr)
            sys.exit(1)
        if data.get('error'):
            print(f"\nAPI error: {data['error']}", file=sys.stderr)
            sys.exit(1)
        result = data['result']
        last = result.get('last')
        trades = next((v for k, v in result.items() if k != 'last'), [])
        if not trades:
            break
        for t in trades:
            print(','.join(str(x) for x in t))
        total += len(trades)
        print(f"  {total} trades...", file=sys.stderr, end='\r')
        if last is None or last == since_ns:
            break
        since_ns = last
        time.sleep(0.5)
    print(f"\nDone. {total} trades total.", file=sys.stderr)


# --- Ticker formatting ---

def format_ticker_rest(result):
    """Print a formatted table from a REST /0/public/Ticker response result dict."""
    col_w = 14
    header = f"{'Pair':<12} {'Last':>{col_w}} {'Bid':>{col_w}} {'Ask':>{col_w}} {'24h Low':>{col_w}} {'24h High':>{col_w}} {'24h Vol':>{col_w}} {'24h VWAP':>{col_w}}"
    print(header)
    print('-' * len(header))
    for pair, d in result.items():
        last   = d['c'][0]
        bid    = d['b'][0]
        ask    = d['a'][0]
        low    = d['l'][1]
        high   = d['h'][1]
        vol    = d['v'][1]
        vwap   = d['p'][1]
        print(f"{pair:<12} {float(last):>{col_w},.2f} {float(bid):>{col_w},.2f} {float(ask):>{col_w},.2f} "
              f"{float(low):>{col_w},.2f} {float(high):>{col_w},.2f} {float(vol):>{col_w},.4f} {float(vwap):>{col_w},.2f}")


def format_ticker_ws(msg_str):
    """Pretty-print a WebSocket v2 ticker message; returns True if it was a ticker message."""
    try:
        msg = json.loads(msg_str)
    except ValueError:
        return False
    if not isinstance(msg, dict) or msg.get('channel') != 'ticker':
        return False
    msg_type = msg.get('type', '')
    for d in msg.get('data', []):
        symbol  = d.get('symbol', '?')
        last    = d.get('last', 0)
        bid     = d.get('bid', 0)
        ask     = d.get('ask', 0)
        low     = d.get('low', 0)
        high    = d.get('high', 0)
        vol     = d.get('volume', 0)
        chg_pct = d.get('change_pct', 0)
        sign    = '+' if chg_pct >= 0 else ''
        ts      = time.strftime('%H:%M:%S')
        print(f"[{ts}] {symbol:<10} last={last:>12,.2f}  bid={bid:>12,.2f}  ask={ask:>12,.2f}"
              f"  lo={low:>12,.2f}  hi={high:>12,.2f}  vol={vol:>12,.4f}  chg={sign}{chg_pct:.2f}%",
              flush=True)
    return True


# --- Minimal stdlib WebSocket client ---

def _recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("WebSocket connection closed")
        buf += chunk
    return buf


def ws_connect(host, path):
    raw = socket.create_connection((host, 443))
    ctx = ssl.create_default_context()
    sock = ctx.wrap_socket(raw, server_hostname=host)
    key = base64.b64encode(os.urandom(16)).decode()
    sock.send((
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {host}\r\n"
        f"Upgrade: websocket\r\n"
        f"Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\n"
        f"Sec-WebSocket-Version: 13\r\n"
        f"\r\n"
    ).encode())
    resp = b""
    while b"\r\n\r\n" not in resp:
        resp += sock.recv(4096)
    if b"101" not in resp:
        raise ConnectionError(f"Handshake failed: {resp[:200]}")
    return sock


def ws_send(sock, text):
    payload = text.encode()
    mask = os.urandom(4)
    masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    n = len(payload)
    if n <= 125:
        header = struct.pack("!BB", 0x81, 0x80 | n)
    elif n <= 65535:
        header = struct.pack("!BBH", 0x81, 0x80 | 126, n)
    else:
        header = struct.pack("!BBQ", 0x81, 0x80 | 127, n)
    sock.send(header + mask + masked)


def ws_recv(sock):
    """Read one complete WebSocket message; returns str or None on close."""
    payload = b""
    opcode = None
    while True:
        hdr = _recv_exact(sock, 2)
        fin    = hdr[0] & 0x80
        op     = hdr[0] & 0x0F
        masked = hdr[1] & 0x80
        n      = hdr[1] & 0x7F
        if n == 126:
            n = struct.unpack("!H", _recv_exact(sock, 2))[0]
        elif n == 127:
            n = struct.unpack("!Q", _recv_exact(sock, 8))[0]
        mask_key = _recv_exact(sock, 4) if masked else None
        data = _recv_exact(sock, n)
        if mask_key:
            data = bytes(b ^ mask_key[i % 4] for i, b in enumerate(data))
        if op == 0x08:   # close
            return None
        if op == 0x09:   # ping → pong
            sock.send(struct.pack("!BB", 0x8A, 0x00))
            continue
        if op == 0x0A:   # pong
            continue
        if op in (0x01, 0x02):
            opcode = op
            payload = data
        else:            # continuation (0x00)
            payload += data
        if fin:
            return payload.decode() if opcode == 0x01 else payload


# --- Streaming quotes ---

def stream_quotes(symbols, pretty=False):
    print(f"Streaming {', '.join(symbols)} — press Ctrl+C to stop", flush=True)
    while True:
        try:
            sock = ws_connect(WS_HOST, WS_PATH)
            ws_send(sock, json.dumps({
                "method": "subscribe",
                "params": {"channel": "ticker", "symbol": symbols},
            }))
            while True:
                msg = ws_recv(sock)
                if msg is None:
                    print("Connection closed by server, reconnecting...", flush=True)
                    break
                if pretty:
                    if not format_ticker_ws(msg):
                        parsed_msg = json.loads(msg)
                        if parsed_msg.get('channel') == 'heartbeat':
                            continue
                        print(json.dumps(parsed_msg, indent=4), flush=True)
                else:
                    print(msg, flush=True)
        except KeyboardInterrupt:
            print("\nStopped.", flush=True)
            return
        except (ConnectionError, OSError) as e:
            print(f"Connection error ({e}), reconnecting in 5s...", flush=True)
            time.sleep(5)


if stream_mode:
    params = parse_params(api_data)
    raw_pair = params.get('pair', params.get('symbol', ''))
    symbols = [s.strip() for s in raw_pair.split(',') if s.strip()]
    if not symbols:
        print("--stream requires pair=SYMBOL[,SYMBOL] (e.g. pair=BTC/USD,ETH/USD)")
        sys.exit(1)
    stream_quotes(symbols, output_format == 1)
    sys.exit(0)


# --- Historical download ---

if history_mode:
    params = parse_params(api_data)
    pair = params.get('pair', '')
    if not pair:
        print("--history requires pair=SYMBOL (e.g. pair=XBTUSD)", file=sys.stderr)
        sys.exit(1)
    since_raw = params.get('since')
    since = parse_since(since_raw) if since_raw else None
    try:
        if api_method == 'OHLC':
            interval = int(params.get('interval', 1440))
            download_ohlc_history(pair, interval, since)
        elif api_method == 'Trades':
            download_trades_history(pair, since)
        else:
            print(f"--history is only supported for OHLC and Trades endpoints", file=sys.stderr)
            sys.exit(1)
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
    sys.exit(0)


# --- REST API ---

if api_method in api_private or api_method in api_trading or api_method in api_funding or api_method in api_staking:
    api_path = "/0/private/"
    api_nonce = str(int(time.time()*1000))
    try:
        api_key = open("API_Public_Key").read().strip()
        api_secret = base64.b64decode(open("API_Private_Key").read().strip())
    except:
        print("API public key and API private (secret) key must be in plain text files called API_Public_Key and API_Private_Key")
        sys.exit(1)
    api_postdata = api_data + "&nonce=" + api_nonce
    api_postdata = api_postdata.encode('utf-8')
    api_sha256 = hashlib.sha256(api_nonce.encode('utf-8') + api_postdata).digest()
    api_hmacsha512 = hmac.new(api_secret, api_path.encode('utf-8') + api_method.encode('utf-8') + api_sha256, hashlib.sha512)
    api_request = urllib.request.Request(api_domain + api_path + api_method, api_postdata)
    api_request.add_header("API-Key", api_key)
    api_request.add_header("API-Sign", base64.b64encode(api_hmacsha512.digest()))
    api_request.add_header("User-Agent", "Kraken REST API")
elif api_method in api_public:
    api_path = "/0/public/"
    api_request = urllib.request.Request(api_domain + api_path + api_method + '?' + api_data)
    api_request.add_header("User-Agent", "Kraken REST API")
else:
    print("Usage: %s method [parameters]" % sys.argv[0])
    print("Example: %s OHLC pair=xbtusd interval=1440" % sys.argv[0])
    sys.exit(1)

try:
    api_reply = urllib.request.urlopen(api_request).read()
except Exception as error:
    print("API call failed (%s)" % error)
    sys.exit(1)

try:
    api_reply = api_reply.decode()
except Exception as error:
    if api_method == 'RetrieveExport':
        sys.stdout.buffer.write(api_reply)
        sys.exit(0)
    print("API response invalid (%s)" % error)
    sys.exit(1)

parsed = json.loads(api_reply)
has_error = bool(parsed.get('error'))

if api_method == 'Ticker' and not has_error and output_format == 1:
    format_ticker_rest(parsed['result'])
elif output_format == 1:
    print(json.dumps(parsed, indent=4))
else:
    print(api_reply)

sys.exit(1 if has_error else 0)
