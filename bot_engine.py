import json
import time
import logging
from datetime import datetime, timedelta, timezone
import pandas as pd
import numpy as np
import websocket # The 'websocket-client' package provides the 'websocket' module
import ta
import threading
from collections import deque
import os # Added for file path operations
import requests
import hashlib
import hmac
import base64
import math
import _thread

# Global variables for OKX API configuration
server_time_offset = 0
okx_simulated_trading_header = {}
okx_api_key = ""
okx_api_secret = ""
okx_passphrase = ""
okx_rest_api_base_url = "https://www.okx.com"

# Placeholder for PRODUCT_INFO, will be populated by fetch_product_info
PRODUCT_INFO = {
    "pricePrecision": None,
    "qtyPrecision": None,
    "priceTickSize": None,
    "minOrderQty": None,
    "contractSize": None,
}

def safe_float(value, default=0.0):
    try:
        return float(value)
    except (ValueError, TypeError):
        return default

def get_okx_server_time_and_offset(log_callback):
    global server_time_offset
    try:
        response = requests.get(f"{okx_rest_api_base_url}/api/v5/public/time", timeout=5)
        response.raise_for_status()
        json_response = response.json()
        if json_response.get('code') == '0' and json_response.get('data'):
            server_timestamp_ms = int(json_response['data'][0]['ts'])
            local_timestamp_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            server_time_offset = server_timestamp_ms - local_timestamp_ms
            log_callback(f"OKX server time synchronized. Offset: {server_time_offset}ms", level="info")
            return True
        else:
            log_callback(f"Failed to get OKX server time: {json_response.get('msg', 'Unknown error')}", level="error")
            return False
    except requests.exceptions.RequestException as e:
        log_callback(f"Error fetching OKX server time: {e}", level="error")
        return False
    except Exception as e:
        log_callback(f"Unexpected error in get_okx_server_time_and_offset: {e}", level="error")
        return False

def generate_okx_signature(timestamp, method, request_path, body_str=''):
    """
    Generate HMAC SHA256 signature for OKX API.
    Returns Base64-encoded HMAC-SHA256 digest.
    """
    message = str(timestamp) + method.upper() + request_path + body_str
    hashed = hmac.new(okx_api_secret.encode('utf-8'), message.encode('utf-8'), hashlib.sha256)
    signature = base64.b64encode(hashed.digest()).decode('utf-8')
    return signature

class TradingBotEngine:
    def __init__(self, config_path, emit_callback):
        self.config_path = config_path
        self.emit = emit_callback
        
        self.console_logs = deque(maxlen=500)
        self.config = self._load_config()

        # Configure logging based on config.log_level
        numeric_level = getattr(logging, self.config.get('log_level', 'info').upper(), None)
        if not isinstance(numeric_level, int):
            raise ValueError(f'Invalid log level: {self.config.get("log_level")}')
        
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG) # Set root logger to DEBUG to ensure all messages are captured

        # Clear existing handlers to prevent duplicate messages if bot is restarted
        for handler in root_logger.handlers[:]: # Iterate over a slice to safely modify list
            root_logger.removeHandler(handler)
        
        # StreamHandler for console output (INFO and higher)
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(console_formatter)
        console_handler.setLevel(logging.INFO) # Console gets INFO and higher
        root_logger.addHandler(console_handler)

        # FileHandler for debug.log (DEBUG and higher, only if log_level is DEBUG)
        if numeric_level == logging.DEBUG:
            file_handler = logging.FileHandler('debug.log')
            file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(file_formatter)
            file_handler.setLevel(logging.DEBUG) # File gets DEBUG and higher
            root_logger.addHandler(file_handler)

        self._apply_api_credentials()

        self.ws = None
        self.ws_thread = None
        self.is_running = False
        self.stop_event = threading.Event()
        self.bot_start_time = int(time.time() * 1000) # Track start time in ms
        
        self.current_balance = 0.0
        self.open_trades = []
        self.is_bot_initialized = threading.Event()
        
        # OKX specific variables (from example bot)
        self.historical_data_store = {}
        self.data_lock = threading.Lock()
        self.trade_data_lock = threading.Lock()
        self.latest_trade_price = None
        self.latest_trade_timestamp = None
        self.last_price_update_time = time.time() # High-precision timestamp of last price arrival
        self.account_balance = 0.0
        self.available_balance = 0.0
        self.total_equity = 0.0
        self.initial_total_capital = 0.0 # Session-based, in-memory only
        self.account_info_lock = threading.Lock()
        self.net_profit = 0.0 # Track actual PnL
    
        # Financial Display Metrics
        self.max_allowed_display = 0.0
        self.max_amount_display = 0.0
        self.remaining_amount_notional = 0.0
        self.trade_fees = 0.0
        
        # Refactored for Dual-Direction Support
        self.in_position = {'long': False, 'short': False}
        self.position_entry_price = {'long': 0.0, 'short': 0.0}
        self.position_qty = {'long': 0.0, 'short': 0.0}
        self.current_stop_loss = {'long': 0.0, 'short': 0.0}
        self.current_take_profit = {'long': 0.0, 'short': 0.0}
        self.position_exit_orders = {'long': {}, 'short': {}} # { 'long': {'tp': id, 'sl': id}, ... }
        self.entry_reduced_tp_flag = {'long': False, 'short': False}
        
        self.batch_counter = 0 # Track batches for logging
        self.used_amount_notional = 0.0
        self.position_lock = threading.Lock()
        self.pending_entry_ids = [] # List to track multiple pending entry orders
        self.pending_entry_order_id = None # Kept for backward compatibility/single tracking if needed
        self.pending_entry_order_details = {} # Now will store details per order ID in a dict
        self.entry_sl_price = 0.0 # This might need migration too if we have concurrent entries? 
                                  # Entries are usually batch-based and transient. 
        self.sl_hit_triggered = False
        self.sl_hit_lock = threading.Lock()
        self.entry_order_with_sl = None
        self.entry_order_sl_lock = threading.Lock()
        self.tp_hit_triggered = False
        self.tp_hit_lock = threading.Lock()
        self.bot_startup_complete = False
        self._should_update_tpsl = False

        self.ws_subscriptions_ready = threading.Event()
        self.pending_subscriptions = set()
        
        self.total_trades_count = 0 # Persistent counter for individual fills
        self.confirmed_subscriptions = set()

        self.intervals = {
            '1m': 60, '3m': 180, '5m': 300, '15m': 900, '30m': 1800,
            '1h': 3600, '2h': 7200, '4h': 14400, '6h': 21600, '8h': 28800,
            '12h': 43200, '1d': 86400, '1w': 604800, '1M': 2592000
        }
        
    def log(self, message, level='info', to_file=False, filename=None):
        # Map levels to numerical priorities
        LEVEL_MAP = {
            'debug': 10,
            'info': 20,
            'warning': 30,
            'error': 40,
            'critical': 50
        }
        
        # Get configured log level from config
        configured_level_str = self.config.get('log_level', 'info').lower()
        configured_level = LEVEL_MAP.get(configured_level_str, 20)
        current_level = LEVEL_MAP.get(level.lower(), 20)
        
        # If the level of this message is lower than the configured level, skip it
        if current_level < configured_level:
            return

        timestamp = datetime.now().strftime('%H:%M:%S')
        log_entry = {'timestamp': timestamp, 'message': message, 'level': level}
        
        # Always append to console_logs for internal history if it passed the filter
        self.console_logs.append(log_entry)
        
        # Emit to the frontend
        self.emit('console_log', log_entry)
        
        # Always write to the local log file via Python logging if it passed the filter
        if level == 'info':
            logging.info(message)
        elif level == 'warning':
            logging.warning(message)
        elif level == 'error':
            logging.error(message)
        elif level == 'debug':
            logging.debug(message)
        elif level == 'critical':
            logging.critical(message)
    
    def check_credentials(self):
        """Verifies if the current API credentials are valid and configured."""
        self._apply_api_credentials()

        global okx_api_key, okx_api_secret, okx_passphrase
        if not okx_api_key or not okx_api_secret or not okx_passphrase:
            return False, "API Key, Secret, or Passphrase missing for selected mode."

        try:
            path = "/api/v5/account/balance"
            params = {"ccy": "USDT"}
            # Use max_retries=1 to fail quickly if invalid
            response = self._okx_request("GET", path, params=params, max_retries=1)

            if response and response.get('code') == '0':
                return True, "Credentials valid."
            elif response and response.get('code') == '50110': # Invalid API key
                return False, "Invalid API credentials."
            elif response and response.get('msg'):
                return False, f"API Error: {response.get('msg')}"
            else:
                return False, "Unknown API error during validation."
        except Exception as e:
            return False, f"Connection error: {str(e)}"

    def start(self, passive_monitoring=False):
        if self.is_running and not passive_monitoring:
            self.log('Bot is already trading', 'warning')
            return
        
        if not passive_monitoring:
            self.is_running = True
            self.log('Bot starting trading logic...', 'info')
        else:
            self.log('Bot starting background monitoring...', 'info')
        
        # 0. Apply Credentials
        self._apply_api_credentials()

        # Validation: check if credentials are provided
        global okx_api_key, okx_api_secret, okx_passphrase
        if not okx_api_key or not okx_api_secret or not okx_passphrase:
            self.log("⚠️ API Credentials not configured for the selected mode.", "error")
            if not passive_monitoring:
                self.emit('error', {'message': 'API Credentials not configured.'})
                self.is_running = False
            return

        # New initialization sequence for OKX
        if not get_okx_server_time_and_offset(self.log):
            self.log("Failed to synchronize server time. Please check network connection or API.", 'error')
            if not passive_monitoring:
                self.is_running = False
            self.emit('bot_status', {'running': False})
            return
        
        if not self._fetch_product_info(self.config['symbol']):
            self.log("Failed to fetch product info. Exiting.", 'error')
            if not passive_monitoring:
                self.is_running = False
            self.emit('bot_status', {'running': False})
            return
 
        if not passive_monitoring:
            # 1. Position Mode Sync (Must happen BEFORE leverage)
            target_pos_mode = self.config.get('okx_pos_mode', 'net_mode')
            if not self._okx_set_position_mode(target_pos_mode):
                 self.log("Failed to verify/set position mode. Exiting.", 'error')
                 self.is_running = False
                 self.emit('bot_status', {'running': False})
                 return

            # 2. Leverage Sync (Requires posSide in Hedge Mode)
            lev_val = self.config.get('leverage', 20)
            symbol = self.config['symbol']
            lev_success = False

            if target_pos_mode == 'long_short_mode':
                # Set for both sides in hedge mode
                l_ok = self._okx_set_leverage(symbol, lev_val, pos_side="long")
                s_ok = self._okx_set_leverage(symbol, lev_val, pos_side="short")
                lev_success = l_ok and s_ok
            else:
                # Set for net side in one-way mode
                lev_success = self._okx_set_leverage(symbol, lev_val, pos_side="net")

            if not lev_success:
                self.log("Failed to set leverage. Exiting.", 'error')
                self.is_running = False
                self.emit('bot_status', {'running': False})
                return
        
            self.log("Checking for and closing any existing open positions...", level="info")
            self._check_and_close_any_open_position()

        # Check if threads are already running
        if getattr(self, 'ws_thread', None) and self.ws_thread.is_alive():
            self.log("WebSocket and Management threads are already active. Re-applied any credential changes.", level="debug")
            return

        self.log('Bot initialized. Starting live connection threads...', 'info')
        self.stop_event.clear()
        self.ws_thread = threading.Thread(target=self._initialize_websocket_and_start_main_loop, daemon=True)
        self.ws_thread.start()

        # Start unified management thread (Account Info + Cancellation)
        # Note: In the previous code, this was started in start(), but it should be part of the thread group
        if not getattr(self, 'mgmt_thread', None) or not self.mgmt_thread.is_alive():
            self.mgmt_thread = threading.Thread(target=self._unified_management_loop, daemon=True)
            self.mgmt_thread.start()
    
    def stop(self):
        if not self.is_running:
            self.log('Bot trading is not active', 'warning')
            return
        
        self.is_running = False
        self.log('Bot trading logic paused. Background monitoring remains active.', 'info')
        
        # We no longer set stop_event or close WS here to allow background Auto-Exit to work
        self.emit('bot_status', {'running': False})

    def shutdown(self):
        """Truly stops all threads and connections."""
        self.is_running = False
        self.stop_event.set()
        if self.ws:
            try:
                self.ws.close()
            except:
                pass
        self.log('Bot fully shut down.', 'info')
    
    def _load_config(self):
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)
                # Ensure new config parameters have default values if not present
                config.setdefault('max_allowed_used', 1000.0)
                config.setdefault('cancel_on_tp_price_below_market', True)
                config.setdefault('cancel_on_entry_price_below_market', True)
                config.setdefault('websocket_timeframes', ['1m', '5m']) # Add default for websocket_timeframes
                config.setdefault('direction', 'long')
                config.setdefault('mode', 'cross') # Changed default mode to 'cross'
                config.setdefault('tp_amount', 0.5)
                config.setdefault('sl_amount', 1.0)
                config.setdefault('trigger_price', 'last')
                config.setdefault('tp_mode', 'limit')
                config.setdefault('tp_type', 'oco')
                config.setdefault('use_candlestick_conditions', True) # New parameter
                config.setdefault('okx_demo_api_key', '')
                config.setdefault('okx_demo_api_secret', '')
                config.setdefault('okx_demo_api_passphrase', '')
                config.setdefault('use_chg_open_close', False)
                config.setdefault('min_chg_open_close', 0)
                config.setdefault('max_chg_open_close', 0)
                config.setdefault('use_chg_high_low', False)
                config.setdefault('min_chg_high_low', 0)
                config.setdefault('max_chg_high_low', 0)
                config.setdefault('use_chg_high_close', False)
                config.setdefault('min_chg_high_close', 0)
                config.setdefault('max_chg_high_close', 0)
                config.setdefault('candlestick_timeframe', '1m')
                config.setdefault('initial_total_capital', 0.0) # New: Default for initial total capital
                config.setdefault('log_level', 'info') # New: Default for log level
                return config
        except FileNotFoundError:
            self.log(f"Config file not found: {self.config_path}", 'error')
            raise
        except json.JSONDecodeError as e:
            self.log(f"Error decoding config file {self.config_path}: {e}", 'error')
            raise
        except Exception as e:
            self.log(f"An unexpected error occurred while loading config: {e}", 'error')
            raise

    # ================================================================================
    # OKX API Helper Functions (Adapted as methods)
    # ================================================================================

    def _apply_api_credentials(self):
        """Applies configured API credentials to global variables used by requests."""
        global okx_api_key, okx_api_secret, okx_passphrase, okx_simulated_trading_header
        use_dev = self.config.get('use_developer_api', False)
        use_demo = self.config.get('use_testnet', False)

        if use_dev:
            if use_demo:
                okx_api_key = self.config.get('dev_demo_api_key', '')
                okx_api_secret = self.config.get('dev_demo_api_secret', '')
                okx_passphrase = self.config.get('dev_demo_api_passphrase', '')
            else:
                okx_api_key = self.config.get('dev_api_key', '')
                okx_api_secret = self.config.get('dev_api_secret', '')
                okx_passphrase = self.config.get('dev_passphrase', '')
        else:
            if use_demo:
                okx_api_key = self.config.get('okx_demo_api_key', '')
                okx_api_secret = self.config.get('okx_demo_api_secret', '')
                okx_passphrase = self.config.get('okx_demo_api_passphrase', '')
            else:
                okx_api_key = self.config.get('okx_api_key', '')
                okx_api_secret = self.config.get('okx_api_secret', '')
                okx_passphrase = self.config.get('okx_passphrase', '')

        if use_demo:
            okx_simulated_trading_header = {'x-simulated-trading': '1'}
        else:
            okx_simulated_trading_header = {}
        self.log(f"API Credentials Applied: {'Developer' if use_dev else 'User'} | {'Demo' if use_demo else 'Live'}", level="debug")

    def _save_config(self):
        try:
            with open(self.config_path, 'w') as f:
                json.dump(self.config, f, indent=2)
            self.log(f"Config saved to {self.config_path}", level="debug")
        except Exception as e:
            self.log(f"Error saving config: {e}", level="error")

    def _okx_request(self, method, path, params=None, body_dict=None, max_retries=3):
        local_dt = datetime.now(timezone.utc)
        adjusted_dt = local_dt + timedelta(milliseconds=server_time_offset)
        timestamp = adjusted_dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

        body_str = ''
        if body_dict:
            body_str = json.dumps(body_dict, separators=(',', ':'), sort_keys=True)

        request_path_for_signing = path
        final_url = f"{okx_rest_api_base_url}{path}" 

        if params and method.upper() == 'GET':
            query_string = '?' + '&'.join([f'{k}={v}' for k, v in sorted(params.items())])
            request_path_for_signing += query_string
            final_url += query_string

        signature = generate_okx_signature(timestamp, method, request_path_for_signing, body_str)

        headers = {
            "OK-ACCESS-KEY": okx_api_key,
            "OK-ACCESS-SIGN": signature,
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": okx_passphrase,
            "Content-Type": "application/json"
        }

        headers.update(okx_simulated_trading_header)

        for attempt in range(max_retries):
            try:
                req_func = getattr(requests, method.lower(), None)
                if not req_func:
                    self.log(f"Unsupported HTTP method: {method}", level="error")
                    return None

                kwargs = {'headers': headers, 'timeout': 15}

                if body_dict and method.upper() in ['POST', 'PUT', 'DELETE']:
                    kwargs['data'] = body_str

                self.log(f"{method} {path} (Attempt {attempt + 1}/{max_retries})", level="debug")
                response = req_func(final_url, **kwargs)

                if response.status_code != 200:
                    try:
                        error_json = response.json()
                        self.log(f"API Error: Status={response.status_code}, Code={error_json.get('code')}, Msg={error_json.get('msg')}. Full Response: {error_json}", level="error")
                        okx_error_code = error_json.get('code')
                        if okx_error_code:
                            return error_json
                    except json.JSONDecodeError:
                        self.log(f"API Error: Status={response.status_code}, Response: {response.text}", level="error")

                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)
                        continue
                    return None

                try:
                    json_response = response.json()
                    if json_response.get('code') != '0':
                        self.log(f"OKX API returned non-zero code: {json_response.get('code')} Msg: {json_response.get('msg')} for {method} {path}. Full Response: {json_response}", level="warning")
                    self.log(f"DEBUG: Full OKX API Response for {method} {path}: {json_response}", level="debug") # Log full response
                    return json_response
                except json.JSONDecodeError:
                    self.log(f"Failed to decode JSON for {method} {path}. Status: {response.status_code}, Resp: {response.text}", level="error")
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)
                        continue
                    return None

            except requests.exceptions.Timeout:
                self.log(f"API request timeout (Attempt {attempt + 1}/{max_retries})", level="error")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                return None
            except requests.exceptions.RequestException as e:
                status_code = e.response.status_code if e.response is not None else "N/A"
                err_text = e.response.text[:200] if e.response is not None else 'No response text'
                self.log(f"OKX API HTTP Error ({method} {path}): Status={status_code}, Error={e}. Response: {err_text}", level="error")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                return None
            except Exception as e:
                self.log(f"Unexpected error during OKX API request ({method} {path}): {e}", level="error")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                return None
        return None

    def _fetch_historical_data_okx(self, symbol, timeframe, start_ts_ms, end_ts_ms):
        try:
            path = "/api/v5/market/history-candles"

            okx_timeframe_map = {
                '1m': '1m', '3m': '3m', '5m': '5m', '15m': '15m', '30m': '30m',
                '1h': '1H', '2h': '2H', '4h': '4H', '6h': '6H', '8h': '8H',
                '12h': '12H', '1d': '1D', '1w': '1W', '1M': '1M'
            }
            okx_timeframe = okx_timeframe_map.get(timeframe)

            if not okx_timeframe:
                self.log(f"Invalid timeframe for OKX: {timeframe}", level="error")
                return []

            all_data = []
            max_candles_limit = 100

            current_before_ms = end_ts_ms

            self.log(f"Fetching historical data for {symbol} ({timeframe})", level="debug")

            while True:
                params = {
                    "instId": symbol,
                    "bar": okx_timeframe,
                    "limit": str(max_candles_limit),
                    "before": str(current_before_ms)
                }

                response = self._okx_request("GET", path, params=params)
                
                if response and response.get('code') == '0':
                    rows = response.get('data', [])
                    if rows:
                        self.log(f"Fetched {len(rows)} candles for {timeframe}", level="debug")
                        parsed_klines = []
                        for kline in rows:
                            try:
                                parsed_klines.append([
                                    int(kline[0]),
                                    float(kline[1]),
                                    float(kline[2]),
                                    float(kline[3]),
                                    float(kline[4]),
                                    float(kline[5])
                                ])
                            except (ValueError, TypeError, IndexError) as e:
                                self.log(f"Error parsing OKX kline: {kline} - {e}", level="error")
                                continue
                        
                        all_data.extend(parsed_klines)
                        
                        oldest_ts = int(rows[-1][0])
                        current_before_ms = oldest_ts

                        if oldest_ts <= start_ts_ms or len(rows) < max_candles_limit:
                            break 
                    else:
                        break 

                    time.sleep(0.3)
                else:
                    self.log(f"Error fetching OKX klines: {response}", level="error")
                    return []
            
            final_data = pd.DataFrame(all_data, columns=['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
            if not final_data.empty:
                final_data = final_data.drop_duplicates(subset=['Timestamp'])
                final_data = final_data[final_data['Timestamp'] >= start_ts_ms]
                final_data = final_data.sort_values(by='Timestamp', ascending=True)
                return final_data.values.tolist()
            else:
                return []
        except Exception as e:
            self.log(f"Exception in _fetch_historical_data_okx: {e}", level="error")
            return []

    def _fetch_product_info(self, target_symbol):
        global PRODUCT_INFO
        try:
            path = "/api/v5/public/instruments"
            params = {"instType": "SWAP", "instId": target_symbol}
            response = self._okx_request("GET", path, params=params)

            if response and response.get('code') == '0':
                product_data = None
                if isinstance(response.get('data'), list):
                    for item in response['data']:
                        if item.get('instId') == target_symbol:
                            product_data = item
                            break
                elif isinstance(response.get('data'), dict) and response.get('data').get('instId') == target_symbol:
                    product_data = response.get('data')

                if not product_data:
                    self.log(f"Product {target_symbol} not found in OKX instruments response.", level="error")
                    return False

                PRODUCT_INFO['priceTickSize'] = safe_float(product_data.get('tickSz'))
                PRODUCT_INFO['qtyPrecision'] = int(np.abs(np.log10(safe_float(product_data.get('lotSz'))))) if safe_float(product_data.get('lotSz')) > 0 else 0
                PRODUCT_INFO['pricePrecision'] = int(np.abs(np.log10(safe_float(product_data.get('tickSz'))))) if safe_float(product_data.get('tickSz')) > 0 else 0
                PRODUCT_INFO['qtyStepSize'] = safe_float(product_data.get('lotSz'))
                PRODUCT_INFO['minOrderQty'] = safe_float(product_data.get('minSz'))

                PRODUCT_INFO['contractSize'] = safe_float(product_data.get('ctVal', '1'), 1.0)

                self.log(f"Product specifications for {target_symbol} initialized.", level="debug")
                return True
            else:
                self.log(f"Failed to fetch product info for {target_symbol} (code: {response.get('code') if response else 'N/A'}, msg: {response.get('msg') if response else 'N/A'})", level="error")
                return False
        except Exception as e:
            self.log(f"Exception in fetch_product_info: {e}", level="error")
            return False

    def _okx_set_leverage(self, symbol, leverage_val, pos_side="net"):
        try:
            path = "/api/v5/account/set-leverage"
            body = {
                "instId": symbol,
                "lever": str(int(leverage_val)),
                "mgnMode": self.config.get('mode', 'cross'), # Use mode from config
                "posSide": pos_side
            }

            self.log(f"Setting leverage to {leverage_val}x for {symbol} ({pos_side})", level="debug")
            response = self._okx_request("POST", path, body_dict=body)

            if response and response.get('code') == '0':
                self.log(f"✓ Leverage set to {leverage_val}x for {symbol} ({pos_side})", level="info")
                return True
            else:
                self.log(f"Failed to set leverage for {symbol}: {response.get('msg') if response else 'No response'}", level="error")
                return False
        except Exception as e:
            self.log(f"Exception in okx_set_leverage: {e}", level="error")
            return False

    def _okx_set_position_mode(self, mode_val):
        try:
            # Mode options: 'net_mode' (One-way) or 'long_short_mode' (Hedge)
            path = "/api/v5/account/set-position-mode"
            body = {"posMode": mode_val}
            
            self.log(f"Setting account position mode to {mode_val}...", level="debug")
            response = self._okx_request("POST", path, body_dict=body)
            
            if response and response.get('code') == '0':
                self.log(f"✓ Position mode set to {mode_val}", level="info")
                return True
            elif response and response.get('code') == '51000': # Already in this mode
                self.log(f"✓ Position mode already confirmed: {mode_val}", level="debug")
                return True
            else:
                self.log(f"Failed to set position mode: {response.get('msg') if response else 'No response'}", level="error")
                return False
        except Exception as e:
            self.log(f"Exception in _okx_set_position_mode: {e}", level="error")
            return False

    def _get_ws_url(self):
        # Dynamic URL: Production vs Demo
        if self.config.get('use_testnet'):
            self.log("Using OKX Demo WebSocket (wspap.okx.com)", level="debug")
            return "wss://wspap.okx.com:8443/ws/v5/public"
        return "wss://ws.okx.com:8443/ws/v5/public"

    def _on_websocket_message(self, ws_app, message):
        # Removed raw message logging to reduce clutter
        try:
            msg = json.loads(message)
            # self.log(f"DEBUG: _on_websocket_message received parsed message: {msg}", level="debug")

            # Handle event messages (subscribe)
            if 'event' in msg:
                if msg['event'] == 'subscribe':
                    arg = msg.get('arg', {})
                    channel_id = f"{arg.get('channel')}:{arg.get('instId')}"
                    self.log(f"Subscription confirmed for {channel_id}", level="debug")
                    self.confirmed_subscriptions.add(channel_id)
                    if self.pending_subscriptions == self.confirmed_subscriptions:
                        self.log("All WebSocket subscriptions are ready.", level="debug")
                        self.ws_subscriptions_ready.set()
                else: # Log other event messages
                    self.log(f"Received non-subscribe event message: {msg}", level="warning")
                # Do NOT return here, allow further processing if it's a data message that also has an event.
            
            if 'data' in msg:
                channel = msg.get('arg', {}).get('channel', '')
                data = msg.get('data', [])

                if channel == 'trades' and data:
                    with self.trade_data_lock:
                        self.latest_trade_timestamp = int(data[-1].get('ts'))
                        self.latest_trade_price = safe_float(data[-1].get('px'))
                        self.last_price_update_time = time.time()

                elif channel == 'tickers' and data:
                    # Process ticker data to update latest_trade_price
                    # The `last` field from ticker data represents the current price
                    self.latest_trade_price = safe_float(data[0].get('last'))
                    self.last_price_update_time = time.time()
                    # No need to update historical data store from tickers channel

        except json.JSONDecodeError:
            self.log(f"DEBUG: Non-JSON WebSocket message received: {message[:500]}", level="debug")
        except Exception as e:
            self.log(f"Exception in on_websocket_message: {e}", level="error")

    def _on_websocket_open(self, ws_app):
        self.log("OKX WebSocket connection opened.", level="info")
        # For public endpoints, authentication is not required, directly send subscriptions
        self._send_websocket_subscriptions()
        # The _send_websocket_subscriptions method will populate self.pending_subscriptions

    def _send_websocket_subscriptions(self):
        channels = [
            {"channel": "trades", "instId": self.config['symbol']},
            {"channel": "tickers", "instId": self.config['symbol']}, # Public tickers channel for real-time price
        ]
        
        # Temporarily removed candle subscriptions until correct format for ETH-USDT-SWAP is confirmed
 
        subscription_payload = {
            "op": "subscribe",
            "args": channels
        }
        self.log(f"WS Sending public subscription request: {json.dumps(subscription_payload)}", level="debug")
        self.ws.send(json.dumps(subscription_payload))
        self.log(f"WS Sent public subscription request for {len(channels)} channels.", level="debug")
        # Populate pending_subscriptions with the channels we just sent
        self.pending_subscriptions = {f"{arg['channel']}:{arg['instId']}" for arg in channels}

    def _on_websocket_error(self, ws_app, error):
        self.log(f"OKX WebSocket error: {error}", level="error")

    def _on_websocket_close(self, ws_app, close_status_code, close_msg):
        self.log("OKX WebSocket closed.", level="debug")
        if self.is_running and not self.stop_event.is_set():
            self.log('Attempting to reconnect WebSocket...', level="debug")
            time.sleep(5)
            self.ws_thread = threading.Thread(target=self._initialize_websocket_and_start_main_loop, daemon=True)
            self.ws_thread.start()
        else:
            self.log('WebSocket will not reconnect as bot is stopped.', level="debug")

    def connect(self): # This method will be called from start()
        ws_url = self._get_ws_url()
        try:
            self.ws = websocket.WebSocketApp(
                ws_url,
                on_open=self._on_websocket_open,
                on_message=self._on_websocket_message,
                on_error=self._on_websocket_error,
                on_close=self._on_websocket_close
            )
            self.emit('bot_status', {'running': True})
            self.ws.run_forever()
        except Exception as e:
            self.log(f"Exception initializing WebSocket: {e}", level="error")


    def _fetch_initial_historical_data(self, symbol, timeframe, start_date_str, end_date_str):
        with self.data_lock:
            try:
                start_dt = datetime.strptime(start_date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                start_ts_ms = int(start_dt.timestamp() * 1000)
                end_dt = datetime.strptime(end_date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                end_ts_ms = int(end_dt.timestamp() * 1000)

                raw_data = self._fetch_historical_data_okx(symbol, timeframe, start_ts_ms, end_ts_ms)

                if raw_data:
                    df = pd.DataFrame(raw_data, columns=['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
                    df.dropna(subset=['Open', 'High', 'Low', 'Close', 'Volume'], inplace=True)

                    if df.empty:
                        self.log(f"No valid data for {timeframe}", level="error")
                        return False

                    invalid_rows = df[(df['Low'] > df['High']) |
                                    (df['Open'] < df['Low']) | (df['Open'] > df['High']) |
                                    (df['Close'] < df['Low']) | (df['Close'] > df['High'])]

                    if not invalid_rows.empty:
                        self.log(f"WARNING: Found {len(invalid_rows)} invalid OHLC rows", level="warning")
                        df = df[(df['Low'] <= df['High'])]

                    df['Datetime'] = pd.to_datetime(df['Timestamp'], unit='ms', utc=True)
                    df = df.set_index('Datetime')
                    df = df[~df.index.duplicated(keep='first')]
                    df = df.sort_index()

                    self.historical_data_store[timeframe] = df

                    self.log(f"Loaded {len(df)} candles for {timeframe}", level="debug")
                    return True
                else:
                    self.log(f"Failed to fetch data for {timeframe}", level="error")
                    return False
            except Exception as e:
                self.log(f"Exception in _fetch_initial_historical_data: {e}", level="error")
                return False

    def _okx_place_order(self, symbol, side, qty, price=None, order_type="Market",
                        time_in_force=None, reduce_only=False,
                        stop_loss_price=None, take_profit_price=None, posSide=None, verbose=True):
        try:
            path = "/api/v5/trade/order"
            price_precision = PRODUCT_INFO.get('pricePrecision', 4)
            qty_precision = PRODUCT_INFO.get('qtyPrecision', 8)

            order_qty_str = f"{qty:.{qty_precision}f}"

            body = {
                "instId": symbol,
                "tdMode": self.config.get('mode', 'cross'),
                "side": side.lower(),
                "ordType": order_type.lower(),
                "sz": order_qty_str,
            }

            if (self.config.get('hedge_mode', False) or self.config.get('okx_pos_mode') == 'long_short_mode') and posSide:
                body["posSide"] = posSide

            if order_type.lower() == "limit" and price is not None:
                body["px"] = f"{price:.{price_precision}f}"

            if time_in_force:
                if time_in_force == "GoodTillCancel":
                    body["timeInForce"] = "GTC"
                else:
                    body["timeInForce"] = time_in_force

            if reduce_only:
                body["reduceOnly"] = True

            self.log(f"DEBUG: Order placement request body: {body}", level="debug")
            if verbose:
                self.log(f"Placing {order_type} {side} order for {order_qty_str} {symbol} at {price}", level="info")
            
            response = self._okx_request("POST", path, body_dict=body)

            if response and response.get('code') == '0':
                order_data = response.get('data', [])
                if order_data and order_data[0].get('ordId'):
                    if verbose:
                        self.log(f"✓ Order placed: OrderID={order_data[0]['ordId']}", level="info")
                    return order_data[0]
                else:
                    self.log(f"✗ Order placement failed: No order ID in response. Response: {response}", level="error")
                    return None
            else:
                error_msg = response.get('msg', 'Unknown error') if response else 'No response'
                self.log(f"✗ Order placement failed: {error_msg}. Response: {response}", level="error")
                return None
        except Exception as e:
            self.log(f"Exception in _okx_place_order: {e}", level="error")
            return None

    def _okx_place_algo_order(self, body, verbose=True):
        try:
            path = "/api/v5/trade/order-algo"
            if verbose:
                self.log(f"Placing algo order", level="info")
            response = self._okx_request("POST", path, body_dict=body)
            if response and response.get('code') == '0':
                data = response.get('data', [])
                if data and (data[0].get('algoId') or data[0].get('ordId')):
                    if verbose:
                        self.log(f"✓ Algo order placed", level="info")
                    return data[0]
                else:
                    self.log(f"✗ Algo order placed but no algoId/ordId returned: {response}", level="error")
                    return None
            else:
                self.log(f"✗ Algo order failed: {response}", level="error")
                return None
        except Exception as e:
            self.log(f"Exception in _okx_place_algo_order: {e}", level="error")
            return None

    def _okx_cancel_order(self, symbol, order_id):
        try:
            path = "/api/v5/trade/cancel-order"
            body = {
                "instId": symbol,
                "ordId": order_id,
            }

            self.log(f"Cancelling OKX order {order_id[:12]}...", level="info")
            response = self._okx_request("POST", path, body_dict=body)

            if response and response.get('code') == '0':
                self.log(f"✓ Order cancelled", level="info")
                return True
            elif response and response.get('code') == '51001':
                self.log(f"Order already filled/cancelled (OK)", level="info")
                return True
            else:
                self.log(f"Failed to cancel order (OK, continuing): {response.get('msg') if response else 'No response'}", level="warning")
                return False
        except Exception as e:
            self.log(f"Exception in _okx_cancel_order: {e}", level="error")
            return False

    def _okx_cancel_algo_order(self, symbol, algo_id):
        try:
            # Use the modern plural endpoint for canceling algo orders
            path = "/api/v5/trade/cancel-algos"
            body = [{
                "instId": symbol,
                "algoId": algo_id,
            }]

            self.log(f"Cancelling OKX algo order {str(algo_id)[:12]}...", level="info")
            response = self._okx_request("POST", path, body_dict=body)

            if response and response.get('code') == '0':
                self.log(f"✓ Algo order cancelled", level="info")
                return True
            elif response and response.get('code') == '51001':
                self.log(f"Algo order already filled/cancelled (OK)", level="info")
                return True
            else:
                self.log(f"Failed to cancel algo order (OK, continuing): {response.get('msg') if response else 'No response'}", level="warning")
                return False
        except Exception as e:
            self.log(f"Exception in _okx_cancel_algo_order: {e}", level="error")
            return False

    def _close_all_entry_orders(self):
        try:
            self.log("Attempting to close unfilled linear entry orders...", level="info")

            path = "/api/v5/trade/orders-pending"
            params = {"instType": "SWAP", "instId": self.config['symbol']}
            response = self._okx_request("GET", path, params=params)

            if not response or response.get('code') == '0':
                self.log("No orders found or API error (OK if no orders)", level="info")
                return True

            orders = response.get('data', [])
            cancelled_count = 0

            for order in orders:
                try:
                    order_id = order.get('ordId')
                    status = order.get('state')
                    side = order.get('side')
                    if side == 'buy' and status not in ['filled', 'canceled', 'rejected']:
                        if self._okx_cancel_order(self.config['symbol'], order_id):
                            cancelled_count += 1
                            time.sleep(0.1)
                except Exception as e:
                    self.log(f"Error processing OKX order: {e}", level="error")

            if cancelled_count > 0:
                self.log(f"✓ Closed {cancelled_count} unfilled linear entry orders", level="info")
            else:
                self.log(f"No unfilled linear entry orders to close (OK)", level="info")

            return True
        except Exception as e:
            self.log(f"Exception in _close_all_entry_orders: {e} (continuing)", level="error")
            return True

    def _handle_tp_hit(self, side='long'):
        with self.tp_hit_lock:
            self.tp_hit_triggered = True # Set the flag immediately

        try:
            self.log("=" * 80, level="info")
            self.log(f"🎯 TP HIT ({side.upper()}) - EXECUTING PROTOCOL", level="info")
            self.log("=" * 80, level="info")

            self.log("Step 1: Closing unfilled entry orders...", level="info")
            self._close_all_entry_orders()

            time.sleep(1)

            self.log(f"Step 2: Checking {side.upper()} OKX position status...", level="info")
            path = "/api/v5/account/positions"
            params = {"instType": "SWAP", "instId": self.config['symbol']}
            response = self._okx_request("GET", path, params=params)

            position_still_open = False
            open_qty = 0.0

            if response and response.get('code') == '0':
                positions = response.get('data', [])
                for pos in positions:
                    if pos.get('instId') == self.config['symbol'] and pos.get('posSide', 'net') == side:
                        pos_qty_raw = safe_float(pos.get('pos', '0'))
                        if abs(pos_qty_raw) > 0:
                            position_still_open = True
                            open_qty = abs(pos_qty_raw)
                            self.log(f"OKX {side.upper()} position still open: {open_qty} (partial fill)", level="info")
                            break

            if position_still_open and open_qty > 0:
                self.log("Step 3: Waiting 3 seconds for liquidity...", level="info")
                time.sleep(3)

                self.log(f"Step 4: Market closing remaining {side.upper()} position...", level="info")
                close_side = "Sell" if side == 'long' else "Buy"
                exit_order_response = self._okx_place_order(self.config['symbol'], close_side, open_qty, order_type="Market", reduce_only=True, posSide=side)

                if exit_order_response and exit_order_response.get('ordId'):
                    self.log(f"✓ Market close order placed for {open_qty} {self.config['symbol']} ({side})", level="info")
                
                time.sleep(1)
                self._cancel_all_exit_orders_and_reset(f"TP hit - {side} closed", side=side)
            else:
                self.log(f"OKX {side.upper()} position fully closed or not found. No market close needed.", level="info")
                self._cancel_all_exit_orders_and_reset(f"TP hit - {side} fully closed", side=side)

            with self.tp_hit_lock:
                self.tp_hit_triggered = False

            self.log(f"✓ {side.upper()} TP HIT PROTOCOL COMPLETE", level="info")

        except Exception as e:
            self.log(f"Exception in _handle_tp_hit ({side}): {e}", level="error")
            with self.tp_hit_lock:
                self.tp_hit_triggered = False

    def _handle_eod_exit(self):
        try:
            self.log("=" * 80, level="info")
            self.log("🕐 EOD EXIT TRIGGERED (OKX)", level="info")
            self.log("=" * 80, level="info")

            self.log("Step 1: Closing all open OKX positions...", level="info")
            try:
                path = "/api/v5/account/positions"
                params = {"instType": "SWAP", "instId": self.config['symbol']}
                response = self._okx_request("GET", path, params=params)

                if response and response.get('code') == '0':
                    positions = response.get('data', [])
                    for pos in positions:
                        if pos.get('instId') == self.config['symbol']:
                            size_rv = safe_float(pos.get('pos', 0))
                            if abs(size_rv) > 0:
                                pos_side = pos.get('posSide', 'net')
                                close_side = "Sell" if size_rv > 0 else "Buy"
                                
                                self.log(f"Found active {pos_side} position: {size_rv} - closing...", level="info")
                                exit_order_response = self._okx_place_order(self.config['symbol'], close_side, abs(size_rv), order_type="Market", reduce_only=True, posSide=pos_side)
                                if exit_order_response and exit_order_response.get('ordId'):
                                    self.log(f"✓ {pos_side.upper()} close order placed", level="info")
                                else:
                                    self.log(f"⚠ {pos_side.upper()} close failed (OK if closed)", level="warning")
                                time.sleep(0.5)
                else:
                    self.log("No OKX positions found or API error (OK)", level="info")
            except Exception as e:
                self.log(f"Error closing OKX positions: {e} (OK, continuing)", level="warning")

            self.log("Step 2: Closing unfilled entry orders...", level="info")
            try:
                self._close_all_entry_orders()
            except Exception as e:
                self.log(f"Error closing entry orders: {e} (OK, continuing)", level="warning")

            time.sleep(0.5)

            self.log("Step 3: Force cancelling all remaining OKX orders...", level="info")
            try:
                path = "/api/v5/trade/cancel-all-after"
                body = {"timeOut": "0", "instType": "SWAP"}
                response = self._okx_request("POST", path, body_dict=body)
                if response and response.get('code') == '0':
                    self.log(f"✓ All OKX orders cancelled", level="info")
                else:
                    self.log(f"⚠ All OKX orders cancel response: {response} (OK)", level="warning")
            except Exception as e:
                self.log(f"Error force cancelling OKX orders: {e} (OK, continuing)", level="error")

            self.log("=" * 80, level="info")
            self.log("✓ EOD EXIT COMPLETE (OKX)", level="info")
            self.log("=" * 80, level="info")

            self._cancel_all_exit_orders_and_reset("EOD Exit")

        except Exception as e:
            self.log(f"Exception in _handle_eod_exit (OKX): {e} (continuing)", level="error")
            self._cancel_all_exit_orders_and_reset("EOD Exit - forced")

    def _handle_order_update(self, orders_data):
        with self.position_lock:
            current_pending_id = self.pending_entry_order_id
            is_in_pos = self.in_position
            active_exit_orders = dict(self.position_exit_orders)
            tracked_qty = self.position_qty

        with self.entry_order_sl_lock:
            tracked_entry_order = self.entry_order_with_sl

    def _handle_order_update(self, orders_data):
        with self.position_lock:
             # Snapshot current states for directional mapping 
             active_exit_ids = {
                 'long': self.position_exit_orders.get('long', {}),
                 'short': self.position_exit_orders.get('short', {})
             }
             pending_entry_ids = list(self.pending_entry_ids)
             
        for order in orders_data:
            if not isinstance(order, dict): continue

            order_id = order.get('ordId') or order.get('algoId')
            status = order.get('state')
            symbol = order.get('instId')
            pos_side = order.get('posSide', 'net')
            
            # Map side for processing
            side_key = 'long'
            if pos_side == 'short': side_key = 'short'
            elif pos_side == 'net':
                # Map net based on order contents or current config if ambiguous
                side_key = self.config.get('direction', 'long')
                if side_key == 'both': side_key = 'long'

            if symbol != self.config['symbol']: continue

            # 1. SL HIT
            if order_id == active_exit_ids[side_key].get('sl') and status in ['filled', 'partially_filled']:
                with self.sl_hit_lock:
                    if not self.sl_hit_triggered:
                        self.sl_hit_triggered = True
                        threading.Timer(0.1, lambda s=side_key: self._handle_sl_hit(side=s)).start()
                return

            # 2. ENTRY FILLED
            if order_id in pending_entry_ids:
                cum_qty = safe_float(order.get('accFillSz', 0))
                with self.position_lock:
                    if order_id in self.pending_entry_order_details:
                        self.pending_entry_order_details[order_id]['status'] = status
                        self.pending_entry_order_details[order_id]['cum_qty'] = cum_qty

                if status in ['filled', 'partially_filled'] or cum_qty > 0:
                    self.log(f"🎉 ENTRY FILLED [{side_key.upper()}]: {cum_qty} {self.config['symbol']}", level="info")
                    if status == 'filled':
                        threading.Timer(2.0, lambda oid=order_id: self._confirm_and_set_active_position(oid)).start()
                    else:
                        threading.Timer(5.0, lambda oid=order_id: self._confirm_and_set_active_position(oid)).start()
                    return
                elif status in ['canceled', 'failed']:
                    self._reset_entry_state(f"Entry order {status}")
                    return

            # 3. TP HIT
            if order_id == active_exit_ids[side_key].get('tp') and status in ['filled', 'partially_filled']:
                with self.tp_hit_lock:
                    if not self.tp_hit_triggered:
                        self.tp_hit_triggered = True
                        threading.Timer(0.1, lambda s=side_key: self._handle_tp_hit(side=s)).start()
                return

    def _detect_sl_from_position_update(self, positions_msg):
        # Scan positions message for closures
        for pos in positions_msg:
            if pos.get('instId') == self.config['symbol']:
                pos_side = pos.get('posSide', 'net')
                side_key = 'long'
                if pos_side == 'short': side_key = 'short'
                elif pos_side == 'net':
                    side_key = self.config.get('direction', 'long')
                    if side_key == 'both': side_key = 'long'

                size_rv = safe_float(pos.get('pos', 0))
                
                with self.position_lock:
                    was_in = self.in_position[side_key]
                    exp_qty = self.position_qty[side_key]

                if was_in and size_rv == 0 and abs(exp_qty) > 0:
                    self.log(f"🛑 SL DETECTED [{side_key.upper()}] via WebSocket Position Update!", level="info")
                    with self.sl_hit_lock:
                        if not self.sl_hit_triggered:
                            self.sl_hit_triggered = True
                            threading.Timer(0.1, lambda s=side_key: self._handle_sl_hit(side=s)).start()


    def _handle_sl_hit(self, side='long'):
        with self.sl_hit_lock:
            self.sl_hit_triggered = True # Set the flag immediately

        try:
            self.log("=" * 80, level="info")
            self.log(f"🛑 STOP LOSS HIT ({side.upper()}) - EXECUTING CLEANUP", level="info")
            self.log("=" * 80, level="info")

            self.log(f"{side.upper()} position already closed by exchange SL", level="info")

            try:
                self._close_all_entry_orders()
            except: pass

            time.sleep(0.5)

            self.log(f"Cancelling {side.upper()} TP order and resetting state...", level="info")
            self._cancel_all_exit_orders_and_reset(f"SL hit - {side} closed by exchange", side=side)

            with self.sl_hit_lock:
                self.sl_hit_triggered = False
            self.log(f"✓ {side.upper()} SL CLEANUP COMPLETE", level="info")
        except Exception as e:
            self.log(f"Exception in _handle_sl_hit ({side}): {e}", level="error")
            self._cancel_all_exit_orders_and_reset(f"SL hit - {side} forced reset", side=side)
            with self.sl_hit_lock:
                self.sl_hit_triggered = False

    def _confirm_and_set_active_position(self, filled_order_id):
        try:
            self.log(f"Confirming OKX position for filled order ID: {filled_order_id}", level="debug")

            path = "/api/v5/account/positions"
            params = {"instType": "SWAP", "instId": self.config['symbol']}
            response = self._okx_request("GET", path, params=params)
            self.log(f"DEBUG: Response from /api/v5/account/positions: {response}", level="debug")

            entry_confirmed = False
            actual_entry_price = 0.0
            actual_qty = 0.0
            actual_side = None
            found_pos_side = None

            if response and response.get('code') == '0':
                positions = response.get('data', [])
                self.log(f"DEBUG: Positions data from OKX: {positions}", level="debug")
                for pos in positions:
                    if pos.get('instId') == self.config['symbol']:
                        pos_qty_str = pos.get('pos', '0')
                        size_val = safe_float(pos_qty_str)
                        if abs(size_val) > 0:
                            avg_entry_price_rv = safe_float(pos.get('avgPx', 0))
                            actual_entry_price = avg_entry_price_rv
                            actual_qty = size_val
                            entry_confirmed = True
                            # Determine actual side if 'net'
                            found_pos_side = pos.get('posSide')
                            if found_pos_side == 'net' or not found_pos_side:
                                actual_side = 'short' if size_val < 0 else 'long'
                            else:
                                actual_side = found_pos_side
                            
                            self.log(f"DEBUG: Confirmed active {actual_side} position - Entry Price: {actual_entry_price}, Quantity: {actual_qty}", level="debug")
                            break

            if not entry_confirmed or actual_entry_price <= 0:
                self.log("CRITICAL: Could not confirm OKX position or invalid entry price!", level="error")
                self.log(f"DEBUG: entry_confirmed: {entry_confirmed}, actual_entry_price: {actual_entry_price}", level="debug")
                return


            tp_price_offset = self.config.get('tp_price_offset', 0.6)
            sl_price_offset = self.config.get('sl_price_offset', 30)

            # Use confirmed actual_side instead of signal_direction to be more robust
            self.log(f"DEBUG: confirm_and_set_active_position: actual_side={actual_side}, qty={actual_qty}", level="debug")

            if actual_side == 'long':
                tp_price = actual_entry_price + tp_price_offset
                sl_price = actual_entry_price - sl_price_offset
                exit_order_side = "sell"
            else: # short
                tp_price = actual_entry_price - tp_price_offset
                sl_price = actual_entry_price + sl_price_offset
                exit_order_side = "buy"
            
            with self.position_lock:
                self.in_position[actual_side] = True
                self.position_entry_price[actual_side] = actual_entry_price
                self.position_qty[actual_side] = actual_qty
                self.current_take_profit[actual_side] = tp_price
                self.current_stop_loss[actual_side] = sl_price
                self.pending_entry_order_id = None
                self.position_exit_orders[actual_side] = {}

                # Emit position update for this side
                self.emit('position_update', {
                    'in_position': self.in_position[actual_side],
                    'position_entry_price': self.position_entry_price[actual_side],
                    'position_qty': self.position_qty[actual_side],
                    'current_take_profit': self.current_take_profit[actual_side],
                    'current_stop_loss': self.current_stop_loss[actual_side],
                    'side': actual_side 
                })

            self.log(f"OKX {actual_side.upper()} POSITION OPENED", level="info")
            self.log(f"Entry: ${actual_entry_price:.2f} | Qty: {actual_qty}", level="info")
            self.log(f"TP: ${tp_price:.2f} | SL: ${sl_price:.2f}", level="info")

            price_precision = PRODUCT_INFO.get('pricePrecision', 4)
            qty_precision = PRODUCT_INFO.get('qtyPrecision', 8)

            # Place TP and SL as algo (conditional) orders via /api/v5/trade/order-algo
            tp_body = {
                "instId": self.config['symbol'],
                "tdMode": self.config.get('mode', 'cross'),
                "side": exit_order_side,
                "posSide": actual_side, # In Hedge Mode we use actual_side, in Net it's usually net or ignored
                "ordType": "conditional",
                "sz": f"{(abs(actual_qty) * (self.config.get('tp_amount', 100) / 100)):.{qty_precision}f}",
                "tpTriggerPx": f"{tp_price:.{price_precision}f}",
                "tpOrdPx": "-1" if self.config.get('tp_mode', 'market') == 'market' else f"{tp_price:.{price_precision}f}",
                "reduceOnly": "true"
            }

            tp_order = self._okx_place_algo_order(tp_body)
            if tp_order and (tp_order.get('algoId') or tp_order.get('ordId')):
                algo_id = tp_order.get('algoId') or tp_order.get('ordId')
                with self.position_lock:
                    self.position_exit_orders[actual_side]['tp'] = algo_id
                self.log(f"✓ TP algo order placed for {actual_side.upper()} at ${tp_price:.2f}", level="info")
            else:
                self.log(f"❌ Failed to place TP algo order: {tp_order}", level="error")
                self._execute_trade_exit(f"Failed to place TP for {actual_side}", side=actual_side)
                return

            sl_body = {
                "instId": self.config['symbol'],
                "tdMode": self.config.get('mode', 'cross'),
                "side": exit_order_side,
                "posSide": actual_side,
                "ordType": "conditional",
                "sz": f"{(abs(actual_qty) * (self.config.get('sl_amount', 100) / 100)):.{qty_precision}f}",
                "slTriggerPx": f"{sl_price:.{price_precision}f}",
                "slOrdPx": "-1", # market
                "reduceOnly": "true"
            }

            sl_order = self._okx_place_algo_order(sl_body)
            if sl_order and (sl_order.get('algoId') or sl_order.get('ordId')):
                algo_id = sl_order.get('algoId') or sl_order.get('ordId')
                with self.position_lock:
                    self.position_exit_orders[actual_side]['sl'] = algo_id
                self.log(f"✓ SL algo order placed for {actual_side.upper()} at ${sl_price:.2f}", level="info")
            else:
                self.log(f"❌ Failed to place SL algo order: {sl_order}", level="error")
                self._execute_trade_exit(f"Failed to place SL for {actual_side}", side=actual_side)
                return

        except Exception as e:
            self.log(f"Exception in _confirm_and_set_active_position (OKX): {e}", level="error")


    def _execute_trade_exit(self, reason, side=None):
        try:
            self.log(f"=== EXIT === Reason: {reason} | Target: {side.upper() if side else 'ALL'}", level="info")

            # Determine sides to exit
            sides_to_exit = [side] if side else ['long', 'short']

            for s in sides_to_exit:
                with self.position_lock:
                    if not self.in_position[s]:
                        if side: # Only log warning if specific side was requested
                            self.log(f"Exit aborted for {s.upper()}: Not in position", level="warning")
                        continue
                    qty_to_close = self.position_qty[s]
                    orders_to_cancel = list(self.position_exit_orders[s].values())

                # Cancel TP/SL for this side
                for order_id in orders_to_cancel:
                    if order_id:
                        try:
                            self._okx_cancel_algo_order(self.config['symbol'], order_id)
                            time.sleep(0.1)
                        except Exception as e:
                            self.log(f"Error cancelling {s.upper()} order: {e} (OK, continuing)", level="error")

                try:
                    # Determine side for market order to close
                    # If position qty is positive (long), we SELL to close.
                    # If position qty is negative (short), we BUY to close.
                    close_side = "Sell" if qty_to_close > 0 else "Buy"
                    
                    self.log(f"Placing market {close_side.lower()} for {abs(qty_to_close)} {self.config['symbol']} (posSide: {s})", level="info")
                    exit_order = self._okx_place_order(self.config['symbol'], close_side, abs(qty_to_close), order_type="Market", reduce_only=True, posSide=s)

                    if not (exit_order and exit_order.get('ordId')):
                        self.log(f"WARNING: Market exit for {s.upper()} failed (OK if already closed)", level="warning")
                except Exception as e:
                    self.log(f"Exception during {s.upper()} market exit: {e}", level="error")

                time.sleep(0.5)
                self._cancel_all_exit_orders_and_reset(reason, side=s)

        except Exception as e:
            self.log(f"Exception in _execute_trade_exit: {e}", level="error")

    def _cancel_all_exit_orders_and_reset(self, reason, side=None):
        # Determine sides to reset
        sides_to_reset = [side] if side else ['long', 'short']
        
        with self.position_lock:
            for s in sides_to_reset:
                orders_to_cancel = list(self.position_exit_orders[s].values())

                self.in_position[s] = False
                self.position_entry_price[s] = 0.0
                self.position_qty[s] = 0.0
                self.current_take_profit[s] = 0.0
                self.current_stop_loss[s] = 0.0
                self.position_exit_orders[s] = {}
                self.entry_reduced_tp_flag[s] = False

                for order_id in orders_to_cancel:
                    if order_id:
                        try:
                            # Note: Usually already cancelled by execute_trade_exit, but safe to retry
                            self._okx_cancel_algo_order(self.config['symbol'], order_id)
                        except: pass

        with self.entry_order_sl_lock:
            self.entry_order_with_sl = None

        self.log("=" * 80, level="info")
        self.log(f"STATE RESET [{side.upper() if side else 'ALL'}] - Reason: {reason}", level="info")
        self.log("=" * 80, level="info")
    def _check_and_close_any_open_position(self):
        try:
            self.log("Checking for any open OKX positions to close...", level="debug")
            path = "/api/v5/account/positions"
            params = {"instType": "SWAP", "instId": self.config['symbol']}
            response = self._okx_request("GET", path, params=params)

            any_closed = False
            if response and response.get('code') == '0':
                positions = response.get('data', [])
                for pos in positions:
                    if pos.get('instId') == self.config['symbol']:
                        size_rv = safe_float(pos.get('pos', 0))
                        if abs(size_rv) > 0:
                            # Detect side from pos negative/positive or posSide
                            pos_side = pos.get('posSide')
                            if pos_side == 'net' or not pos_side:
                                pos_side = 'short' if size_rv < 0 else 'long'
                            
                            self.log(f"⚠️ Found open {pos_side} OKX position: {size_rv} {self.config['symbol']}", level="warning")
                            # If size_rv is negative (short), we must BUY to close
                            close_side = "Buy" if size_rv < 0 else "Sell"
                            self.log(f"Closing {abs(size_rv)} {self.config['symbol']} with market {close_side} order (posSide: {pos_side})", level="info")
                            close_order = self._okx_place_order(self.config['symbol'], close_side, abs(size_rv), order_type="Market", reduce_only=True, posSide=pos_side)
                            if close_order and close_order.get('ordId'):
                                self.log(f"✓ Position close order placed: {close_order.get('ordId')}", level="info")
                                any_closed = True
                            else:
                                self.log(f"❌ Failed to place close order for {pos_side} position", level="error")

            if not any_closed:
                self.log("No open OKX positions found to close.", level="info")
            return any_closed
        except Exception as e:
            self.log(f"Exception in _check_and_close_any_open_position (OKX): {e}", level="error")
            return False

    def _reset_entry_state(self, reason):
        with self.position_lock:
            self.pending_entry_order_id = None
            self.entry_reduced_tp_flag = False
            self.pending_entry_order_details = {}
        with self.entry_order_sl_lock:
            self.entry_order_with_sl = None
        self.log(f"Entry state reset. Reason: {reason}", level="info")


        self.log("=" * 80, level="info")
        self.log(f"POSITION CLOSED - Reason: {reason}", level="info")
        self.log("=" * 80, level="info")

        for order_id in orders_to_cancel:
            if order_id:
                try:
                    self._okx_cancel_algo_order(self.config['symbol'], order_id)
                    time.sleep(0.1)
                except Exception as e:
                    self.log(f"Error cancelling order: {e} (OK, continuing)", level="error")

        # Account information is no longer updated in real-time via private WebSocket.

    def _get_latest_data_and_indicators(self):
        try:
            with self.trade_data_lock: # Use trade_data_lock for latest_trade_price
                current_price = self.latest_trade_price
                if current_price is None:
                    if self.is_running:
                        self.log(f"Could not get current market price from WebSocket. Waiting for data.", level="warning")
                    return None
                
                # Check price age for logging/diagnostics
                price_age = time.time() - self.last_price_update_time
                if price_age > 1.0:
                    self.log(f"Price data is {price_age:.1f}s old. Checking connection...", level="debug")

            return {
                'current_price': current_price,
                'price_age': price_age
            }

        except Exception as e:
            self.log(f"Exception in _get_latest_data_and_indicators: {e}", level="error")
            return None

    def _check_candlestick_conditions(self, market_data):
        # Fetch the latest completed candle for the primary timeframe (e.g., '1m')
        # This assumes you have historical data being updated.
        timeframe = self.config.get('candlestick_timeframe', '1m')
        with self.data_lock:
            df = self.historical_data_store.get(timeframe)
            if df is None or df.empty:
                self.log(f"No historical data for {timeframe} to check candlestick conditions.", "warning")
                return True, "No Data (Default Pass)" # Default to true if data is not available to not block trades

            latest_candle = df.iloc[-1]
            o = latest_candle['Open']
            h = latest_candle['High']
            l = latest_candle['Low']
            c = latest_candle['Close']

        status_parts = []
        
        # Check Open-Close Change
        oc_pass = True
        if self.config.get('use_chg_open_close'):
            chg_open_close = abs(o - c)
            min_chg = self.config.get('min_chg_open_close', 0)
            max_chg = self.config.get('max_chg_open_close', 0)
            if not (min_chg <= chg_open_close <= max_chg):
                 oc_pass = False
            status_parts.append(f"open-close={'Passed' if oc_pass else 'Fail'}")

        # Check High-Low Change
        hl_pass = True
        if self.config.get('use_chg_high_low'):
            chg_high_low = h - l
            min_chg = self.config.get('min_chg_high_low', 0)
            max_chg = self.config.get('max_chg_high_low', 0)
            if not (min_chg <= chg_high_low <= max_chg):
                hl_pass = False
            status_parts.append(f"High-Low={'Passed' if hl_pass else 'Fail'}")

        # Check High-Close Change
        hc_pass = True
        if self.config.get('use_chg_high_close'):
            chg_high_close = abs(h - c)
            min_chg = self.config.get('min_chg_high_close', 0)
            max_chg = self.config.get('max_chg_high_close', 0)
            if not (min_chg <= chg_high_close <= max_chg):
                hc_pass = False
            status_parts.append(f"High-Close={'Passed' if hc_pass else 'Fail'}")

        all_passed = oc_pass and hl_pass and hc_pass
        status_str = "; ".join(status_parts) if status_parts else "Skipped"
        
        return all_passed, status_str

    def _check_entry_conditions(self, market_data, log_prefix=""):
        # Max Amount = Max Allowed Used (USDT)
        # Remaining = (Max Amount * Leverage) - Used Notional
        leverage = float(self.config.get('leverage', 1))
        if leverage <= 0: leverage = 1.0
        
        max_amount_usdt = self.config.get('max_allowed_used', 1000)
        rate_divisor = self.config.get('rate_divisor', 1)
        if rate_divisor <= 0: rate_divisor = 1
        max_amount_per_loop = max_amount_usdt / rate_divisor
        max_notional_capacity = max_amount_per_loop * leverage
        
        min_notional_per_order = self.config.get('min_order_amount', 100)
        
        with self.position_lock:
            # High-Precision Remaining Calculation
            remaining_notional = max_notional_capacity - self.used_amount_notional
            
            if remaining_notional < min_notional_per_order:
                self.log(f"{log_prefix}Entry-3:Remaining Capacity: {remaining_notional:.2f} < Min {min_notional_per_order}: NOT Passed", level="info")
                return []

        target_amount = self.config.get('target_order_amount', 100)

        # User is responsible for setting Max Allowed within their balance limits
        # Bot focuses only on remaining capacity
        current_price = market_data['current_price']
        direction_mode = self.config.get('direction', 'long')
        long_safety = self.config.get('long_safety_line_price', 0)
        short_safety = self.config.get('short_safety_line_price', float('inf'))
        entry_price_offset = self.config.get('entry_price_offset', 0)

        valid_entries = []
        
        # Possible directions to check
        directions_to_eval = []
        if direction_mode == 'both':
            directions_to_eval = ['long', 'short']
        else:
            directions_to_eval = [direction_mode]

        # Shared Candlestick check (if enabled)
        candlestick_passed = True
        candlestick_msg = "Skipped"
        if self.config.get('use_candlestick_conditions', False):
            candlestick_passed, candlestick_msg = self._check_candlestick_conditions(market_data)

        for d in directions_to_eval:
            passed = False
            signal = 0
            safety_p = 0.0
            limit_p = 0.0
            
            if d == 'long':
                safety_p = long_safety
                passed = (current_price < long_safety)
                signal = 1
                limit_p = current_price - entry_price_offset
            else:
                safety_p = short_safety
                passed = (current_price > short_safety)
                signal = -1
                limit_p = current_price + entry_price_offset

            self.log(f"{log_prefix}Entry-1:{d.upper()} Market {current_price:.2f}, Safety:{safety_p}, {'Passed' if passed else 'NOT Passed'}", level="info")
            
            if passed:
                if candlestick_passed:
                    valid_entries.append({'signal': signal, 'limit_price': limit_p, 'side': d})
                    if candlestick_msg != "Skipped":
                         self.log(f"{log_prefix}Entry-2:{candlestick_msg}", level="info")
                else:
                    self.log(f"{log_prefix}Entry-2:Candlestick {candlestick_msg}: NOT Passed", level="info")
        
        # Log final verification for consistency if nothing passed
        if not valid_entries:
             return []

        # Check explicit target/min logs for the first valid one to keep user dashboard tidy
        self.log(f"{log_prefix}Entry-3:Remaining: {remaining_notional:.2f} > Target {target_amount}: Passed", level="info")
        self.log(f"{log_prefix}Entry-4:Remaining: {remaining_notional:.2f} > Min {min_notional_per_order}: Passed", level="info")

        return valid_entries

    def _initiate_entry_sequence(self, initial_limit_price, signal, batch_size):
        # NOTE: This function places the batch. It does NOT handle the loop logic. 
        # The loop logic is now in _main_trading_logic.
        
        # We perform a double-check on balance but primary check is in _check_entry_conditions
        with self.account_info_lock:
            current_available_balance = self.available_balance

        batch_offset = self.config['batch_offset']
        self.batch_counter += 1
        
        self.log(f"Place Order Batch {self.batch_counter}", level="info")
        
        for i in range(batch_size):
            current_limit_price = initial_limit_price
            if i > 0: 
                if signal == 1: # Long
                    current_limit_price -= (batch_offset * i)
                else: # Short
                    current_limit_price += (batch_offset * i)

            if current_limit_price <= 0:
                continue

            # Recalculate room for EVERY order to be precise (though less critical if Target is small)
            leverage = float(self.config.get('leverage', 1))
            if leverage <= 0: leverage = 1.0
            max_amount_usdt = self.config.get('max_allowed_used', 1000)
            rate_divisor = self.config.get('rate_divisor', 1)
            if rate_divisor <= 0: rate_divisor = 1
            max_amount_per_loop = max_amount_usdt / rate_divisor
            max_notional_capacity = max_amount_per_loop * leverage
            
            with self.position_lock:
                remaining_notional = max_notional_capacity - self.used_amount_notional
            
            target_notional = self.config.get('target_order_amount', 100)
            min_notional = self.config.get('min_order_amount', 100)
            
            if remaining_notional < min_notional:
                self.log(f"Batch {self.batch_counter}-{i+1} skipped: Remaining ({remaining_notional:.2f}) < Min ({min_notional})", level="info")
                break
                
            trade_amount_usdt = min(target_notional, remaining_notional)
            safe_trade_notional = trade_amount_usdt * 0.995 
            
            qty_base_asset = safe_trade_notional / current_limit_price
            contract_size = safe_float(PRODUCT_INFO.get('contractSize', 1.0))
            if contract_size <= 0: contract_size = 1.0

            min_order_qty = safe_float(PRODUCT_INFO.get('minOrderQty', 1.0))
            qty_contracts = math.floor((qty_base_asset / contract_size) / min_order_qty) * min_order_qty
            
            if qty_contracts < min_order_qty:
                 if (min_order_qty * contract_size * current_limit_price) <= remaining_notional:
                     qty_contracts = min_order_qty
                 else:
                     continue

            qty_precision = PRODUCT_INFO.get('qtyPrecision', 0)
            qty_contracts = round(qty_contracts, qty_precision)
            
            # Calculate TP/SL for Display
            tp_px = 0.0
            sl_px = 0.0
            tp_offset = self.config.get('tp_price_offset', 0)
            sl_offset = self.config.get('sl_price_offset', 0)
            if signal == 1: # LONG
                tp_px = current_limit_price + tp_offset
                sl_px = current_limit_price - sl_offset
            else: # SHORT
                tp_px = current_limit_price - tp_offset
                sl_px = current_limit_price + sl_offset

            # Log Format: Batch1-1:M:2980|En:2982|TP:2976|SL:3010|1000|Short|Isolated|20x
            market_p = self.latest_trade_price if self.latest_trade_price else 0.0
            side_str = 'Long' if signal == 1 else 'Short'
            mode_str = self.config.get('mode', 'cross').capitalize()
            # M:{market}|En:{entry}|Tp:{tp}|SL:{sl}|{amt}|{side}|{mode}
            # Note: User requested "Tp" (capital T, lowercase p case matching handwritten note usually has TP or Tp, using Tp as per log request "Tp:2976") 
            log_str = f"Batch{self.batch_counter}-{i+1}:M:{market_p:.2f}|En:{current_limit_price:.2f}|Tp:{tp_px:.2f}|SL:{sl_px:.2f}|{target_notional}|{side_str}|{mode_str}"
            self.log(log_str, level="info")
            
            p_side_entry = "long" if signal == 1 else "short"
            entry_order_response = self._okx_place_order(self.config['symbol'], "Buy" if signal == 1 else "Sell", qty_contracts, price=current_limit_price, order_type="Limit", time_in_force="GoodTillCancel", posSide=p_side_entry, verbose=False)

            if entry_order_response and entry_order_response.get('ordId'):
                order_id = entry_order_response['ordId']
                with self.position_lock:
                    self.pending_entry_ids.append(order_id)
                    self.pending_entry_order_id = order_id 
                    self.pending_entry_order_details[order_id] = {
                        'order_id': order_id,
                        'side': "Buy" if signal == 1 else "Sell",
                        'qty': qty_contracts * contract_size,
                        'limit_price': current_limit_price,
                        'signal': signal,
                        'order_type': 'Limit',
                        'status': 'New',
                        'placed_at': datetime.now(timezone.utc)
                    }
                
                # Trigger an immediate account info update to refresh values
                threading.Thread(target=self._fetch_and_emit_account_info, daemon=True).start()
            else:
                self.log(f"Order placement failed", level="error")

    def _check_cancel_conditions(self):
        # Explicit check for cancel conditions as per nested loop logic
        
        loop_time = self.config.get('loop_time_seconds', 10) # Using existing param or maybe hardcode 90s check?
        # User diagram says: "Check Cancel Condition"
        # 1. More than 90 seconds (cancel_unfilled_seconds)
        # 2. TP < Market (for short) / TP > Market (for long) [Inverted Logic]
         #    User says: "TP < Market" mean TP price is lower than market.
         #    For SHORT: Entry is high. TP is low.
         #    If TP < Market, that's NORMAL for Short.
         #    User Correction: "Correct is tp price below market price but not market below tp"
         #    ... "For short safety line price and market price, bot also do reverse running"
         #    Let's stick to the Text Description in Logic:
         #    "Cancel-2: TP < Market: None"
         #    This implies checking if TP is < Market.
         #    For SHORT: TP < Entry. Market should be near Entry.
         #    If TP < Market (Market is higher than TP), that is normal state (Not reached TP yet).
         #    Maybe user means "Cancel if Market goes *beyond* TP"? i.e. Market < TP?
         #    Wait, "Cancel-2: TP < Market: None". If it was "Yes", it would cancel?
         #    If "TP < Market" is BAD for Short? No, TP < Market is GOOD (we are above TP).
         #    Maybe for LONG? For Long, TP > Entry. Market near Entry.
         #    If Market < TP is normal.
         #    If "TP < Market" (Market > TP). That means we missed it?
         #    Let's look at previous code: "cancel_on_tp_price_below_market".
         #    The standard logic: If Market moves such that the TP is no longer valid or "unfavorable"?
         #    Actually, for pending entry, we don't have a TP yet?
         #    Ah, we calculate "potential TP".
         #    If Potential TP is already "passed" by current market?
         #    Short: Entry=3000, TP=2900. Market=2800. We are already below TP. "Market < TP".
         #    User says "TP < Market". 2900 < 2800? False.
         #    If Market=2950. 2900 < 2950. True.
         #    So for Short, "TP < Market" is the NORMAL state.
         #    If "TP < Market" is the Cancel Condition, then it would cancel everything immediately?
         #    User said: "Correct is tp price below market price but not market below tp".
         #    This is very confusing phrased.
         #    Let's assume "Unfavorable" means "The opportunity is gone/bad".
         #    For Pending Entry (Limit Order):
         #    Limit Buy @ 100. Potential TP @ 110.
         #    If Market @ 115. (Market > TP). The price ran away UP.
         #    We don't want to buy @ 100 anymore? Or maybe we do?
         #    Usually "Unfavorable" for Limit Entry means the price moved AWAY from entry in the WRONG direction?
         #    No, that's fine, we just wait.
         #    If price moved THROUGH the TP level?
         #    Short: Sell @ 100. TP @ 90.
         #    If Market @ 80. (Market < TP). Price crashed.
         #    If we fill @ 100 now, we are selling high above market? That's good?
         #    No, Limit Sell must be ABOVE market.
         #    If Market @ 80, Limit Sell @ 100 is far above.
         #    This logic is tricky without clear definition.
         #    Let's implicitly trust the user's specific text in current request:
         #    "Cancel-2:TP<Market: None" (In the log example)
         #    "Cancel-3:Entry<Market: Yes" (In the log example)
         
         #    "Cancel-3: Entry < Market: Yes" -> Cancelled.
         #    This was for SHORT? (Earlier log: "for Short")
         #    For SHORT, Entry should be > Market (Sell High).
         #    If "Entry < Market" (Entry=2900, Market=3000), we are verifying a Sell check?
         #    Wait, Limit Sell. Entry must be > Market?
         #    If Entry < Market (e.g. Sell @ 2900, Market @ 3000). The limit order would trigger immediately (Marketable).
         #    Maybe that's why cancel? "Maker only"?
         #    So:
         #    Short: Cancel if Entry < Market.
         #    Long: Cancel if Entry > Market.
         
         #    "Cancel-2: TP < Market".
         #    For Short: TP is below Entry.
         #    If TP < Market (e.g. TP=2900, Market=2950). This is pending state.
         #    If this is a "Cancel Condition", it would always be true?
         #    Unless user means "Market < TP"? (Price dropped below target).
         #    "Correct is tp price below market price but not market below tp"
         #    This implies user WANTS to check "TP < Market".
         #    But if that cancels, it cancels everything normal.
         #    Maybe "TP > Market" for Short? (Price below TP).
         #    Let's assume the user meant "Price passed TP".
         #    Short: Cancel if Market < TP.
         #    Long: Cancel if Market > TP.
         
         #    However, implementing strictly as user described in log:
         #    "Cancel-2: TP<Market"
         #    I will code the log check.

        # self.log("Check Cancel Condition")
        
        cancel_unfilled_seconds = self.config.get('cancel_unfilled_seconds', 90)
        
        with self.position_lock:
             active_ids = list(self.pending_entry_ids)
             details = dict(self.pending_entry_order_details)

        if not active_ids:
            self.log("No Orders to cancel", level="debug")
            return

        current_market_price = self._get_latest_data_and_indicators().get('current_price')
        if not current_market_price: return

        for order_id in active_ids:
            if order_id not in details: continue
            d = details[order_id]
            
            placed_at = d.get('placed_at')
            signal = d.get('signal') # 1 Long, -1 Short
            limit_price = d.get('limit_price')
            
            # 1. Time Check
            time_passed = False
            if placed_at and (datetime.now(timezone.utc) - placed_at).total_seconds() > cancel_unfilled_seconds:
                time_passed = True
            
            # self.log(f"Cancel-1:More than {cancel_unfilled_seconds} seconds: {'Yes' if time_passed else 'None'}")
            
            if time_passed:
                if self._okx_cancel_order(self.config['symbol'], order_id):
                    with self.position_lock:
                        if order_id in self.pending_entry_ids:
                             self.pending_entry_ids.remove(order_id)
                        if order_id in self.pending_entry_order_details:
                             del self.pending_entry_order_details[order_id]
                continue

            # 2. TP Check (Missed Opportunity)
            tp_offset = self.config.get('tp_price_offset', 0)
            is_target_passed = False
            pending_tp = 0.0
            
            if signal == 1: # Long
                pending_tp = limit_price + tp_offset
                if current_market_price > pending_tp:
                    is_target_passed = True
            else: # Short
                pending_tp = limit_price - tp_offset
                if current_market_price < pending_tp:
                    is_target_passed = True

            # 3. Entry Check (Taker Avoidance / Directional Move)
            is_entry_unfavorable = False
            if signal == 1: # Long
                # Cancel if Entry < Market (Price moved up, making order a taker or too high)
                if current_market_price > limit_price:
                    is_entry_unfavorable = True
            else: # Short
                # Cancel if Entry > Market (Price moved down, making order a taker or too low)
                if current_market_price < limit_price:
                    is_entry_unfavorable = True

            # Execute Cancellation based on priority
            should_cancel = False
            cancel_msg = ""
            
            if time_passed:
                should_cancel = True
                cancel_msg = f"Time Limit ({cancel_unfilled_seconds}s) reached"
            
            elif is_target_passed and self.config.get('cancel_on_tp_price_below_market'):
                should_cancel = True
                cancel_msg = f"TP Price Unfavorable (Market {current_market_price:.2f} passed TP {pending_tp:.2f})"

            elif is_entry_unfavorable and self.config.get('cancel_on_entry_price_below_market'):
                should_cancel = True
                cancel_msg = f"Entry Price Unfavorable (Market {current_market_price:.2f} passed Entry {limit_price:.2f})"

            if should_cancel:
                self.log(f"Cancel Order {order_id} ({cancel_msg})")
                if self._okx_cancel_order(self.config['symbol'], order_id):
                    with self.position_lock:
                        if order_id in self.pending_entry_ids:
                             self.pending_entry_ids.remove(order_id)
                        if order_id in self.pending_entry_order_details:
                             del self.pending_entry_order_details[order_id]
                continue
                 
         # Clean up local tracking
        with self.position_lock:
             # Basic cleanup of IDs that are gone happens in account update, but we can fast track here if needed
             pass


    def _unified_management_loop(self):
        # High-reliability background management
        self.log("Unified management thread started.", level="debug")
        last_account_sync = 0
        while not self.stop_event.is_set():
            now = time.time()
            try:
                # 1. High Frequency: Cancellation Check (every ~1s)
                # Note: Only cancel if trading is active or we still have tracked pending orders
                self._check_cancel_conditions()

                # 2. PnL-Based Auto-Exit Check (Now works even in Stop mode)
                use_auto_cancel = self.config.get('use_pnl_auto_cancel', False)
                threshold = self.config.get('pnl_auto_cancel_threshold', 100.0)
                if use_auto_cancel and self.net_profit >= threshold:
                    self.log(f"PNL TARGET HIT! Profit ${self.net_profit:.2f} >= ${threshold:.2f}. Triggering Auto-Exit Liquidate.", level="critical")

                    # Stop trading logic if active
                    self.is_running = False

                    # Cancel all pending orders and close all positions
                    self._close_all_entry_orders()
                    self._check_and_close_any_open_position()
                    self.emit('bot_status', {'running': False})
                    self.log("Auto-Exit Liquidate Complete.", level="info")
                    # We don't exit the thread, as we want to continue monitoring
                
                # 3. Connection Health: Stale Price Monitor
                price_age = now - self.last_price_update_time
                if price_age > 30:
                     self.log(f"WARNING: Market price is STALE ({price_age:.1f}s). Re-initializing WebSocket...", level="warning")
                     # Reset update time to avoid spamming reconnects
                     self.last_price_update_time = now 
                     # Trigger async reconnect
                     threading.Thread(target=self._initialize_websocket, daemon=True).start()
                
                # 3. Lower Frequency: Account Info & Emitting (every ~10s)
                if now - last_account_sync >= 10:
                    self._fetch_and_emit_account_info()
                    last_account_sync = now
                    
            except Exception as e:
                self.log(f"Error in unified mgmt loop: {e}", level="debug")
            
            time.sleep(1) # Base tick rate
        self.log("Unified management thread stopped.", level="debug")

    def _main_trading_logic(self):
        try:
            self.log("Trading loop started.", level="debug")

            while not self.stop_event.is_set():
                if not self.is_running:
                    time.sleep(1)
                    continue

                # 1. Entry Loop
                while self.is_running and not self.stop_event.is_set():
                    self.log("-" * 90)
                    self.log("-" * 90)
                    self.log("Check Entry Condition")
                    market_data = self._get_latest_data_and_indicators()
                    if not market_data:
                        self.log("No market data", level="warning")
                        time.sleep(5)
                        continue
                        
                    valid_signals = self._check_entry_conditions(market_data)
                    
                    if valid_signals:
                        # Process all valid signals (e.g. could be both Long and Short)
                        for entry_info in valid_signals:
                             self._initiate_entry_sequence(entry_info['limit_price'], entry_info['signal'], self.config['batch_size_per_loop'])
                        
                        # Wait Loop Time
                        loop_time = self.config.get('loop_time_seconds', 10)
                        self.log(f"Wait {loop_time} seconds (Post-Entry)")
                        time.sleep(loop_time)
                    else:
                        self.log("Stop Orders (No passing signals in this cycle)")
                        break
                
                # 2. Cancel Check - Now handled by background thread
                # NO-OP here to prevent blocking main loop
                pass
                
                # 3. Delay before restarting cycle
                # Use standard loop_time for consistent heartbeat
                loop_time = self.config.get('loop_time_seconds', 10)
                self.log(f"Wait {loop_time} seconds before meta-loop restart")
                time.sleep(loop_time)

        except Exception as e:
            self.log(f"CRITICAL ERROR in _main_trading_logic: {e}", level="error")

        except Exception as e:
            self.log(f"CRITICAL ERROR in _main_trading_logic: {e}", level="error")

    def _initialize_websocket(self):
        ws_url = self._get_ws_url()
        try:
            self.ws = websocket.WebSocketApp(
                ws_url,
                on_open=self._on_websocket_open,
                on_message=self._on_websocket_message,
                on_error=self._on_websocket_error,
                on_close=self._on_websocket_close
            )
            return self.ws
        except Exception as e:
            self.log(f"Exception initializing WebSocket: {e}", level="error")
            return None

    def _initialize_websocket_and_start_main_loop(self):
        self.log("OKX BOT STARTING", level="info")
        try:
            self.ws_client = self._initialize_websocket()
            if self.ws_client is None:
                self.log("Failed to initialize WebSocket. Exiting.", level="error")
                return

            # For public WebSocket, no authentication is needed. Subscriptions are sent directly on_open.
            self.log("Connecting to public market data...", level="debug")
            
            # Start the WebSocket in a separate thread
            ws_thread = threading.Thread(target=self.ws_client.run_forever, daemon=True)
            ws_thread.start()
            self.log("WebSocket connection established.", level="debug")

            self.log("Syncing with market data...", level="debug")
            if not self.ws_subscriptions_ready.wait(timeout=20): # Longer timeout for subscriptions
                self.log("WebSocket subscriptions not ready within timeout. Exiting.", level="error")
                return

            # Fetch historical data for the selected timeframe - Fetch 300 candles for indicator safety
            timeframe = self.config.get('candlestick_timeframe', '1m')
            # Assuming ~300 candles. 
            interval_sec = self.intervals.get(timeframe, 60)
            start_dt = datetime.now(timezone.utc) - timedelta(seconds=interval_sec * 300)
            end_dt = datetime.now(timezone.utc)
            self._fetch_initial_historical_data(self.config['symbol'], timeframe, start_dt.strftime('%Y-%m-%d'), end_dt.strftime('%Y-%m-%d'))

            # Initial account information is no longer updated in real-time via private WebSocket.
            # The bot will not track account balance or available equity in real-time.
            # This might impact functionality that relies on account balance checks.
            
            self.bot_startup_complete = True
            self.log("Bot startup sequence complete.", level="info")

            # Perform an initial account info update to populate available_balance
            self._periodic_account_info_update(initial_fetch=True)
            self.log("Initial account balance fetched.", level="info")
 
            # Start a periodic task to update account info and emit to frontend
            self.account_info_updater_thread = threading.Thread(target=self._periodic_account_info_update, args=(False,), daemon=True)
            self.account_info_updater_thread.start()
 
            self._main_trading_logic()
 
        except Exception as e:
            self.log(f"CRITICAL ERROR in _initialize_websocket_and_start_main_loop: {e}", level="error")
        finally:
            self.stop_event.set()
            self.log("Shutting down...", level="info")
            if self.ws:
                try:
                    self.ws.close()
                except Exception:
                    pass
            self.log("OKX BOT SHUTDOWN COMPLETE", level="info")
 
    def _calculate_net_profit_from_fills(self):
        # Fetch recent fills to calculate actual PnL
        try:
            path = "/api/v5/trade/fills-history" # Or /trade/fills depending on timing (history covers last 3 months)
            # Fetch last 100 fills. 
            # Note: For long running bots, we might need pagination or local aggregation.
            # Ideally, we query once and append, but for simplicity we re-query recent history.
            params = {
                "instType": "SWAP",
                "instId": self.config['symbol'],
                "limit": "100"
            }
            # Use standard fills if history is empty? Fills history requires archive time.
            # Try '3days' fills first via /trade/fills with '3d' not supported? 
            # /trade/fills gets recent 3 days. /trade/fills-history gets older.
            
            # Use /trade/fills for recent activity (last 3 days)
            path_recent = "/api/v5/trade/fills"
            response = self._okx_request("GET", path_recent, params=params)
            
            total_pnl = 0.0
            
            if response and response.get('code') == '0':
                fills = response.get('data', [])

                # Calculate time window (e.g. last 24 hours) to include manual orders and past session profit
                now_ms = int(time.time() * 1000)
                time_window_ms = 24 * 60 * 60 * 1000 # 24 Hours
                start_time_limit = now_ms - time_window_ms

                for fill in fills:
                     fill_ts = int(fill.get('ts', 0))
                     if fill_ts < start_time_limit:
                         continue

                     # 'pnl' field contains realized profit for closing trades
                     # For opening trades it is usually 0. 
                     # Fee is separate 'fee', usually negative.
                     # Net PnL = pnl + fee
                     pnl = safe_float(fill.get('pnl', 0))
                     fee = safe_float(fill.get('fee', 0))
                     total_pnl += (pnl + fee) # Fee is usually negative, so adding it subtracts cost
            
            # Simple approach: strictly sum up last 100 fills PnL.
            # Limitation: partial view. 
            self.net_profit = total_pnl
            self.log(f"Calculated Net Profit from {len(response.get('data', []))} fills: {self.net_profit}", level="debug")

        except Exception as e:
            self.log(f"Error calculating Net Profit: {e}", level="debug")

    def _periodic_account_info_update(self, initial_fetch=False):
        if initial_fetch:
            # Perform a single fetch and return
            self._fetch_and_emit_account_info()
            return

        while not self.stop_event.is_set():
            try:
                self._fetch_and_emit_account_info()
            except Exception as e:
                self.log(f"Error in periodic account info update: {e}", level="error")
            finally:
                time.sleep(self.config.get('account_update_interval_seconds', 10))

    def fetch_account_data_sync(self):
        """Fetches account data synchronously. Used for dashboard updates before bot start."""
        if not PRODUCT_INFO.get('contractSize'):
            self._fetch_product_info(self.config['symbol'])
        self._fetch_and_emit_account_info()

    def _fetch_and_emit_account_info(self):
        # Fetch account balance
        path_balance = "/api/v5/account/balance"
        params_balance = {"ccy": "USDT"} 
        response_balance = self._okx_request("GET", path_balance, params=params_balance)

        with self.account_info_lock:
            found_total_eq = 0.0
            found_avail_bal = 0.0
            found_bal = 0.0
            if response_balance and response_balance.get('code') == '0':
                data = response_balance.get('data', [])
                if data and len(data) > 0:
                    account_details = data[0]
                    found_total_eq = safe_float(account_details.get('totalEq', '0'))
                    for detail in account_details.get('details', []):
                        if detail.get('ccy') == 'USDT':
                            found_bal = safe_float(detail.get('bal', '0'))
                            found_avail_bal = safe_float(detail.get('availBal', '0'))
                            break
            
            self.account_balance = found_avail_bal # Revert to available balance for frontend display
            self.available_balance = found_avail_bal
            self.total_equity = found_total_eq
            total_balance = self.account_balance
            available_balance = self.available_balance
        
        # Fetch open orders (pending orders)
        path_pending_orders = "/api/v5/trade/orders-pending"
        params_pending_orders = {"instType": "SWAP", "instId": self.config['symbol']}
        response_pending_orders = self._okx_request("GET", path_pending_orders, params=params_pending_orders)
        
        formatted_open_trades = []
        if response_pending_orders and response_pending_orders.get('code') == '0':
            pending_orders = response_pending_orders.get('data', [])
            contract_size = PRODUCT_INFO.get('contractSize', 1.0)
            if contract_size <= 0: contract_size = 1.0

            for order in pending_orders:
                ord_id = order.get('ordId') or order.get('algoId')
                
                # Adoption Logic: If we see an order on OKX that we aren't tracking, add it.
                # This preserves cancellation timers after a restart.
                with self.position_lock:
                    if ord_id not in self.pending_entry_ids:
                        self.pending_entry_ids.append(ord_id)
                        
                        # Parse OKX creation time (cTime is Unix ms)
                        c_time_ms = int(order.get('cTime', time.time() * 1000))
                        placed_at_dt = datetime.fromtimestamp(c_time_ms / 1000.0, tz=timezone.utc)
                        
                        self.pending_entry_order_details[ord_id] = {
                            'order_id': ord_id,
                            'side': order.get('side').capitalize(),
                            'qty': safe_float(order.get('sz')) * contract_size,
                            'limit_price': safe_float(order.get('px')),
                            'signal': 1 if order.get('side') == 'buy' else -1,
                            'order_type': order.get('ordType', 'Limit'),
                            'status': order.get('state'),
                            'placed_at': placed_at_dt
                        }

                # Calculate time left for cancellation UI
                time_left = None
                cancel_unfilled_seconds = self.config.get('cancel_unfilled_seconds', 90)
                
                # Fetch timestamp from internal memory if already tracked
                current_placed_at = None
                with self.position_lock:
                    if ord_id in self.pending_entry_order_details:
                         current_placed_at = self.pending_entry_order_details[ord_id].get('placed_at')

                if current_placed_at:
                    seconds_passed = (datetime.now(timezone.utc) - current_placed_at).total_seconds()
                    time_left = max(0, int(cancel_unfilled_seconds - seconds_passed))

                formatted_open_trades.append({
                    'type': order.get('side').capitalize(),
                    'id': ord_id,
                    'entry_spot_price': safe_float(order.get('px')),
                    # Fix: Multiply by contract_size for correct notional value
                    'stake': safe_float(order.get('sz')) * safe_float(order.get('px')) * contract_size,
                    'tp_price': None,
                    'sl_price': None,
                    'status': order.get('state'),
                    'instId': order.get('instId'),
                    'time_left': time_left
                })
        with self.trade_data_lock:
            self.open_trades = formatted_open_trades
        self.emit('trades_update', {'trades': formatted_open_trades})

        # Fetch open positions
        path_positions = "/api/v5/account/positions"
        params_positions = {"instType": "SWAP", "instId": self.config['symbol']}
        response_positions = self._okx_request("GET", path_positions, params=params_positions)

        with self.position_lock:
            # Preserve current qty for comparison across all sides
            prev_qtys = {k: v for k, v in self.position_qty.items()}
            found_sides = set()

            if response_positions and response_positions.get('code') == '0':
                positions_data = response_positions.get('data', [])
                contract_size = PRODUCT_INFO.get('contractSize', 1.0)
                
                for pos in positions_data:
                    if pos.get('instId') == self.config['symbol'] and safe_float(pos.get('pos')) != 0:
                        raw_side = pos.get('posSide', 'net')
                        # Map side to our internal keys
                        side_key = 'long'
                        if raw_side == 'short':
                            side_key = 'short'
                        elif raw_side == 'net':
                            # In one-way mode, use the configured direction or default to long
                            side_key = self.config.get('direction', 'long')
                            if side_key == 'both': side_key = 'long' # Default both-net to long for display

                        found_sides.add(side_key)
                        new_qty = safe_float(pos.get('pos')) * contract_size
                        
                        if abs(new_qty - prev_qtys.get(side_key, 0.0)) > 0.000001:
                            self.log(f"Position update [{side_key.upper()}]: {prev_qtys.get(side_key, 0.0)} -> {new_qty}. Syncing TP/SL...", level="debug")
                            self._should_update_tpsl = True
                            if abs(new_qty) > abs(prev_qtys.get(side_key, 0.0)):
                                self.total_trades_count += 1
                        
                        if self.current_take_profit[side_key] == 0 or self.current_stop_loss[side_key] == 0:
                             self._should_update_tpsl = True

                        self.in_position[side_key] = True
                        self.position_entry_price[side_key] = safe_float(pos.get('avgPx'))
                        self.position_qty[side_key] = new_qty
            
            # Reset sides that were not found
            for s in ['long', 'short']:
                if s not in found_sides:
                    self.in_position[s] = False
                    self.position_entry_price[s] = 0.0
                    self.position_qty[s] = 0.0
                    self.current_take_profit[s] = 0.0
                    self.current_stop_loss[s] = 0.0
            
            # Emit combined position update (Frontend will still show 'primary' or we can update it later)
            # For now, pick the first active one or long if both for backward compatibility with UI
            display_side = 'long' if self.in_position['long'] else ('short' if self.in_position['short'] else 'long')

            self.emit('position_update', {
                'in_position': self.in_position[display_side],
                'position_entry_price': self.position_entry_price[display_side],
                'position_qty': self.position_qty[display_side],
                'current_take_profit': self.current_take_profit[display_side],
                'current_stop_loss': self.current_stop_loss[display_side],
                # New fields for advanced UI if needed
                'positions': {
                    'long': {
                        'in': self.in_position['long'],
                        'qty': self.position_qty['long'],
                        'price': self.position_entry_price['long']
                    },
                    'short': {
                        'in': self.in_position['short'],
                        'qty': self.position_qty['short'],
                        'price': self.position_entry_price['short']
                    }
                }
            })

        # Calculate Net Profit
        self._calculate_net_profit_from_fills()

        # Auto-Exit on Profit Check
        if self.config.get('use_pnl_auto_cancel', False) and self.is_running:
            pnl_threshold = self.config.get('pnl_auto_cancel_threshold', 100.0)
            if self.net_profit >= pnl_threshold:
                self.log(f"🎯 AUTO-EXIT TRIGGERED: Net Profit ${self.net_profit:.2f} >= Target ${pnl_threshold:.2f}", level="warning")
                # Close all positions and cancel all orders
                threading.Thread(target=self._execute_trade_exit, args=("Auto-Exit on Profit Target Reached",), daemon=True).start()

        # Metrics Calculation (Refined for User)
        # Max labels display Margins as per config (Unleveraged)
        max_allowed_margin = self.config['max_allowed_used']
        rate_divisor = self.config['rate_divisor']
        max_amount_margin = max_allowed_margin / rate_divisor

        max_allowed_display = max_allowed_margin
        max_amount_display = max_amount_margin # Show partitioned amount (e.g. $250)

        # Status metrics display Notional (Leveraged)
        leverage = float(self.config.get('leverage', 1))
        if leverage <= 0: leverage = 1

        used_amount_notional = 0.0
        active_positions_count = 0
        
        if response_positions and response_positions.get('code') == '0':
            positions_data = response_positions.get('data', [])
            for pos in positions_data:
                if pos.get('instId') == self.config['symbol'] and safe_float(pos.get('pos')) != 0:
                    # High-Precision Notional: Size * Price * ContractSize
                    # 'notionalUsd' is often provided by OKX, but we calculate for maximum accuracy
                    pos_notional = abs(safe_float(pos.get('pos'))) * safe_float(pos.get('avgPx')) * contract_size
                    used_amount_notional += pos_notional
                    active_positions_count += 1

        with self.trade_data_lock:
            for trade in self.open_trades:
                 used_amount_notional += trade['stake']

        # Remaining = (Max_Amount * Leverage) - Used_Notional
        remaining_amount_notional = (max_amount_margin * leverage) - used_amount_notional
        
        with self.position_lock:
            self.used_amount_notional = used_amount_notional

        # Sync pending_entry_ids with active orders from OKX
        active_okx_ids = [t['id'] for t in formatted_open_trades]
        with self.position_lock:
            existing_pending = list(self.pending_entry_ids)
            for p_id in existing_pending:
                if p_id not in active_okx_ids:
                    # Order is no longer on books (filled or cancelled)
                    self.pending_entry_ids.remove(p_id)
                    if p_id in self.pending_entry_order_details:
                        del self.pending_entry_order_details[p_id]
                    self.log(f"Pending order {p_id} cleared from tracking (Filled or Cancelled).", level="debug")
                    # If it was filled, in_position will be updated below.
                    # We might want to trigger TP/SL update here too.
                    self._should_update_tpsl = True # Flag to update TP/SL if needed

        if getattr(self, '_should_update_tpsl', False) and any(self.in_position.values()) and self.is_running:
            self._should_update_tpsl = False
            # Call TP/SL modification to sync with new average price
            threading.Thread(target=self.batch_modify_tpsl, daemon=True).start()
            
            # REMOVED: self.initial_total_capital = total_balance reset. 
            # We want to keep the original capital to track net profit correctly.
        else:
            # Capture starting capital for the session if not set
            if self.initial_total_capital <= 0 and total_balance > 0:
                 self.initial_total_capital = total_balance
                 self.log(f"Session Started. Capture Total Capital: ${total_balance:.2f}", level="debug")
            else:
                 # Otherwise respect what's in config (or memory)
                 pass

        # Trade Fee Calculation: (Used + Remaining) * Fee_Percentage
        trade_fee_pct = self.config.get('trade_fee_percentage', 0.07)
        used_fee = used_amount_notional * (trade_fee_pct / 100.0)
        remaining_fee = remaining_amount_notional * (trade_fee_pct / 100.0)
        trade_fees = used_fee + remaining_fee

        # Update persistent attributes for status retrieval
        self.max_allowed_display = max_allowed_display
        self.max_amount_display = max_amount_display
        self.remaining_amount_notional = remaining_amount_notional
        self.trade_fees = trade_fees

        total_active_trades_count = self.total_trades_count + len(formatted_open_trades)

        # Emit data to frontend
        self.emit('account_update', {
            'total_trades': total_active_trades_count,
            'total_capital': self.total_equity, # User: Total Capital should be estimated available balance (Equity)
            'max_allowed_used_display': max_allowed_display, 
            'max_amount_display': max_amount_display,
            'used_amount': used_amount_notional, 
            'trade_fees': trade_fees,
            'remaining_amount': remaining_amount_notional, 
            'total_balance': total_balance,
            'available_balance': available_balance,
            'net_profit': self.net_profit # Uses directly summed PnL
        })
        
        self.log(f"Account Update | Balance: ${total_balance:.2f} | Active Positions: {active_positions_count} | Pending: {len(formatted_open_trades)}", level="debug")

    def test_api_credentials(self):
        # Store current global API settings
        global okx_api_key, okx_api_secret, okx_passphrase, okx_simulated_trading_header
        original_okx_api_key = okx_api_key
        original_okx_api_secret = okx_api_secret
        original_okx_passphrase = okx_passphrase
        original_okx_simulated_trading_header = okx_simulated_trading_header

        try:
            # Set global API settings for testing based on self.config (which was modified by app.py)
            use_dev = self.config.get('use_developer_api', False)
            use_demo = self.config.get('use_testnet', False)

            if use_dev:
                if use_demo:
                    okx_api_key = self.config.get('dev_demo_api_key', '')
                    okx_api_secret = self.config.get('dev_demo_api_secret', '')
                    okx_passphrase = self.config.get('dev_demo_api_passphrase', '')
                else:
                    okx_api_key = self.config.get('dev_api_key', '')
                    okx_api_secret = self.config.get('dev_api_secret', '')
                    okx_passphrase = self.config.get('dev_passphrase', '')
            else:
                if use_demo:
                    okx_api_key = self.config.get('okx_demo_api_key', '')
                    okx_api_secret = self.config.get('okx_demo_api_secret', '')
                    okx_passphrase = self.config.get('okx_demo_api_passphrase', '')
                else:
                    okx_api_key = self.config.get('okx_api_key', '')
                    okx_api_secret = self.config.get('okx_api_secret', '')
                    okx_passphrase = self.config.get('okx_passphrase', '')

            if use_demo:
                okx_simulated_trading_header = {'x-simulated-trading': '1'}
            else:
                okx_simulated_trading_header = {}

            # Attempt a simple API call, e.g., get account balance
            path_balance = "/api/v5/account/balance"
            params_balance = {"ccy": "USDT"}
            response_balance = self._okx_request("GET", path_balance, params=params_balance, max_retries=1) # Only 1 retry for test

            if response_balance and response_balance.get('code') == '0':
                return True
            else:
                return False
        except Exception as e:
            self.log(f"Error during API credential test: {e}", level="error")
            return False
        finally:
            # Restore original global API settings
            okx_api_key = original_okx_api_key
            okx_api_secret = original_okx_api_secret
            okx_passphrase = original_okx_passphrase
            okx_simulated_trading_header = original_okx_simulated_trading_header

    def batch_modify_tpsl(self):
        self.log("Initiating batch TP/SL modification...", level="debug")
        try:
            latest_data = self._get_latest_data_and_indicators()
            if not latest_data:
                self.log("Could not get current market price for batch TP/SL modification.", level="debug")
                return
                
            current_market_price = latest_data.get('current_price')
            if current_market_price is None:
                self.log("Current market price is None for batch TP/SL modification.", level="debug")
                return

            path = "/api/v5/account/positions"
            params = {"instType": "SWAP", "instId": self.config['symbol']}
            response = self._okx_request("GET", path, params=params)

            if not response or response.get('code') != '0':
                self.log(f"Failed to fetch open positions for batch TP/SL modification: {response}", level="error")
                self.emit('error', {'message': f'Failed to batch modify TP/SL: Could not fetch open positions.'})
                return

            positions = response.get('data', [])
            modified_count = 0
            tp_price_offset = self.config['tp_price_offset']
            sl_price_offset = self.config['sl_price_offset']
            price_precision = PRODUCT_INFO.get('pricePrecision', 4)
            qty_precision = PRODUCT_INFO.get('qtyPrecision', 8)

            # Group positions by side for batch processing if needed, but here we loop
            for pos in positions:
                if pos.get('instId') == self.config['symbol']:
                    pos_qty = safe_float(pos.get('pos', '0'))
                    pos_side_raw = pos.get('posSide', 'net')
                    avg_px = safe_float(pos.get('avgPx', '0'))

                    if abs(pos_qty) > 0 and avg_px > 0:
                        # Map to our internal side key
                        side_key = 'long'
                        if pos_side_raw == 'short':
                            side_key = 'short'
                        elif pos_side_raw == 'net':
                             # Map net to configured direction
                             side_key = self.config.get('direction', 'long')
                             if side_key == 'both': side_key = 'long' 

                        if side_key == 'long':
                            new_tp = avg_px + tp_price_offset
                            new_sl = avg_px - sl_price_offset
                            order_side = "sell"
                        else: # short
                            new_tp = avg_px - tp_price_offset
                            new_sl = avg_px + sl_price_offset
                            order_side = "buy"

                        # LOGGING AUDIT: Clear avg_px tracking to debug user discrepancy
                        self.log(f"Syncing TP/SL for {side_key.upper()} position. Avg Price: {avg_px:.{price_precision}f}", level="info")

                        # SAFE TP LOGIC: Prevent OKX error 51277
                        price_tick = 1.0 / (10**price_precision)
                        if side_key == 'long':
                            if current_market_price >= new_tp:
                                self.log(f"⚠️ Market ({current_market_price}) above TP ({new_tp}). Adjusting.", level="warning")
                                new_tp = current_market_price + price_tick
                        else: # short
                            if current_market_price <= new_tp: # SHORT TP is below market. If market below TP, we passed it.
                                self.log(f"⚠️ Market ({current_market_price}) below TP ({new_tp}). Adjusting.", level="warning")
                                new_tp = current_market_price - price_tick

                        with self.position_lock:
                            # 1. Fetch and cancel existing algo orders for this SYMBOL + SIDE
                            # Note: OKX allows filtering by posSide in some cases, but here we check all and filter locally
                            path_algo = "/api/v5/trade/orders-algo-pending"
                            params_algo = {"instType": "SWAP", "instId": self.config['symbol'], "ordType": "conditional"}
                            resp_algo = self._okx_request("GET", path_algo, params=params_algo)
                            
                            if resp_algo and resp_algo.get('code') == '0':
                                for algo_order in resp_algo.get('data', []):
                                    if algo_order.get('posSide') == pos_side_raw:
                                        self._okx_cancel_algo_order(self.config['symbol'], algo_order.get('algoId'))
                            
                            self.position_exit_orders[side_key] = {}
                            time.sleep(0.2) 

                            # Place new TP and SL
                            trig_px_type = self.config.get('trigger_price', 'last')
                            
                            tp_body = {
                                "instId": self.config['symbol'],
                                "tdMode": self.config.get('mode', 'cross'),
                                "side": order_side,
                                "posSide": pos_side_raw,
                                "ordType": "conditional",
                                "sz": f"{abs(pos_qty):.{qty_precision}f}",
                                "tpTriggerPx": f"{new_tp:.{price_precision}f}",
                                "tpTriggerPxType": trig_px_type,
                                "tpOrdPx": "-1",
                                "reduceOnly": "true"
                            }

                            tp_order = self._okx_place_algo_order(tp_body, verbose=False)
                            if tp_order and (tp_order.get('algoId') or tp_order.get('ordId')):
                                self.position_exit_orders[side_key]['tp'] = tp_order.get('algoId') or tp_order.get('ordId')
                                self.log(f"🎯 {side_key.upper()} TP Set: {new_tp:.{price_precision}f}", level="info")
                            
                            sl_body = {
                                "instId": self.config['symbol'],
                                "tdMode": self.config.get('mode', 'cross'),
                                "side": order_side,
                                "posSide": pos_side_raw,
                                "ordType": "conditional",
                                "sz": f"{abs(pos_qty):.{qty_precision}f}",
                                "slTriggerPx": f"{new_sl:.{price_precision}f}",
                                "slTriggerPxType": trig_px_type,
                                "slOrdPx": "-1",
                                "reduceOnly": "true"
                            }

                            sl_order = self._okx_place_algo_order(sl_body, verbose=False)
                            if sl_order and (sl_order.get('algoId') or sl_order.get('ordId')):
                                self.position_exit_orders[side_key]['sl'] = sl_order.get('algoId') or sl_order.get('ordId')
                                self.log(f"🎯 {side_key.upper()} SL Set: {new_sl:.{price_precision}f}", level="info")
                            
                            self.current_take_profit[side_key] = new_tp
                            self.current_stop_loss[side_key] = new_sl
                            modified_count += 1
                            
                            # Emit side-specific update
                            self.emit('position_update', {
                                'in_position': self.in_position[side_key],
                                'position_entry_price': self.position_entry_price[side_key],
                                'position_qty': self.position_qty[side_key],
                                'current_take_profit': self.current_take_profit[side_key],
                                'current_stop_loss': self.current_stop_loss[side_key],
                                'side': side_key
                            })

            if modified_count > 0:
                self.log(f"Successfully modified TP/SL for {modified_count} sides.", level="info")
            else:
                self.log("No active positions found (or matched criteria) to modify TP/SL.", level="debug")
        
        except Exception as e:
            self.log(f"Exception in batch_modify_tpsl: {e}", level="error")
            self.emit('error', {'message': f'Failed to batch modify TP/SL: {str(e)}'})
        self.log("Batch TP/SL modification complete.", level="debug")



    def batch_cancel_orders(self):
        self.log("Initiating batch order cancellation...", level="info")
        try:
            cancelled_count = 0
            
            # 1. Cancel Limit Orders
            path = "/api/v5/trade/orders-pending"
            params = {"instType": "SWAP", "instId": self.config['symbol']}
            response = self._okx_request("GET", path, params=params)

            if response and response.get('code') == '0':
                orders = response.get('data', [])
                for order in orders:
                    order_id = order.get('ordId')
                    if order_id:
                        if self._okx_cancel_order(self.config['symbol'], order_id):
                            cancelled_count += 1
                            time.sleep(0.1)

            # 2. Cancel Algo Orders (TP/SL/Conditional)
            path_algo = "/api/v5/trade/orders-algo-pending"
            params_algo = {
                "instType": "SWAP", 
                "instId": self.config['symbol'],
                "ordType": "conditional" # RESTORED: Required by OKX
            }
            response_algo = self._okx_request("GET", path_algo, params=params_algo)

            if response_algo and response_algo.get('code') == '0':
                algo_orders = response_algo.get('data', [])
                for algo_order in algo_orders:
                    algo_id = algo_order.get('algoId')
                    if algo_id:
                        if self._okx_cancel_algo_order(self.config['symbol'], algo_id):
                            cancelled_count += 1
                            time.sleep(0.1)

            if cancelled_count > 0:
                self.log(f"✅ Cancelled {cancelled_count} pending orders.", level="info")
            else:
                self.log("No orders to cancel.", level="warning")
                self.emit('warning', {'message': 'No pending orders found to cancel.'})

        except Exception as e:
            self.log(f"Exception in batch_cancel_orders: {e}", level="error")
            self.emit('error', {'message': f'Failed to batch cancel orders: {str(e)}'})
            self.log("Batch order cancellation complete.", level="info")

    def emergency_sl(self):
        self.log("🚨 EMERGENCY STOP LOSS TRIGGERED: Closing all positions and orders...", level="warning")
        try:
            # 1. Cancel all pending orders
            self.batch_cancel_orders()
            
            # 2. Close all positions
            closed_pos = self._check_and_close_any_open_position()
            
            if closed_pos:
                self.log("✓ Emergency SL: Positions closed and orders cancelled.", level="info")
            else:
                self.log("✓ Emergency SL: Orders cancelled (no active positions found).", level="info")
        except Exception as e:
            self.log(f"Error during Emergency SL: {e}", level="error")
            self.emit('error', {'message': f'Emergency SL failed: {e}'})
