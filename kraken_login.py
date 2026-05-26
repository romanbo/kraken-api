#!/usr/bin/env python3

# Kraken Rest API + WebSocket streaming
#
# Usage: ./krakenapi.py endpoint [parameters] [-pretty] [--stream]
# Example: ./krakenapi.py Time
# Example: ./krakenapi.py OHLC pair=xbtusd interval=1440
# Example: ./krakenapi.py Balance
# Example: ./krakenapi.py TradeBalance asset=xdg
# Example: ./krakenapi.py OpenPositions
# Example: ./krakenapi.py AddOrder pair=xxbtzusd type=buy ordertype=market volume=0.003 leverage=5
# Example: ./krakenapi.py Ticker pair=BTC/USD,ETH/USD --stream

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
    sock = ws_connect(WS_HOST, WS_PATH)
    ws_send(sock, json.dumps({
        "method": "subscribe",
        "params": {"channel": "ticker", "symbol": symbols},
    }))
    while True:
        msg = ws_recv(sock)
        if msg is None:
            break
        if pretty:
            print(json.dumps(json.loads(msg), indent=4), flush=True)
        else:
            print(msg, flush=True)


if stream_mode:
    params = parse_params(api_data)
    raw_pair = params.get('pair', params.get('symbol', ''))
    symbols = [s.strip() for s in raw_pair.split(',') if s.strip()]
    if not symbols:
        print("--stream requires pair=SYMBOL[,SYMBOL] (e.g. pair=BTC/USD,ETH/USD)")
        sys.exit(1)
    try:
        stream_quotes(symbols, output_format == 1)
    except KeyboardInterrupt:
        pass
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

if '"error":[]' in api_reply:
    print(api_reply if output_format == 0 else json.dumps(json.loads(api_reply), indent=4))
    sys.exit(0)
else:
    print(api_reply if output_format == 0 else json.dumps(json.loads(api_reply), indent=4))
    sys.exit(1)
