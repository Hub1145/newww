from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, emit
import json
import logging
import os
from bot_engine import TradingBotEngine

logging.basicConfig(
    level=logging.DEBUG, # Changed to DEBUG for more verbose logging
    format='%(asctime)s - %(levelname)s - %(message)s'
)

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SESSION_SECRET', 'dev-secret-key-change-in-production')
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

config_file = 'config.json'
bot_engine = None

def load_config():
    with open(config_file, 'r') as f:
        return json.load(f)

def save_config(config):
    with open(config_file, 'w') as f:
        json.dump(config, f, indent=2)

def emit_to_client(event, data):
    socketio.emit(event, data)

@app.route('/favicon.ico')
def favicon():
    return app.send_static_file('favicon.ico')


@app.route('/')
def index():
    return render_template('dashboard.html')

@app.route('/api/config', methods=['GET'])
def get_config():
    config = load_config()
    return jsonify(config)

@app.route('/api/config', methods=['POST'])
def update_config():
    global bot_engine

    try:
        new_config = request.json
        current_config = load_config()

        # Whitelist of all valid parameters
        allowed_params = [
            'okx_api_key', 'okx_api_secret', 'okx_passphrase', 'okx_demo_api_key', 'okx_demo_api_secret', 'okx_demo_api_passphrase', 'use_testnet', 'symbol',
            'short_safety_line_price', 'long_safety_line_price', 'leverage', 'max_allowed_used',
            'entry_price_offset', 'batch_offset', 'tp_price_offset', 'sl_price_offset',
            'loop_time_seconds', 'rate_divisor', 'batch_size_per_loop', 'min_order_amount',
            'target_order_amount', 'cancel_unfilled_seconds', 'cancel_on_tp_price_below_market',
            'cancel_on_entry_price_below_market', 'direction', 'mode', 'tp_amount', 'sl_amount',
            'trigger_price', 'tp_mode', 'tp_type', 'use_chg_open_close', 'min_chg_open_close',
            'max_chg_open_close', 'use_chg_high_low', 'min_chg_high_low', 'max_chg_high_low',
            'use_chg_high_close', 'min_chg_high_close', 'max_chg_high_close', 'candlestick_timeframe',
            'use_candlestick_conditions', 'log_level'
        ]

        # Update current_config with only allowed and present keys from new_config
        updates_made = False
        for key, value in new_config.items():
            if key in allowed_params:
                # Type conversion safety could be added here if needed, but JSON usually handles it well enough for basic types
                if current_config.get(key) != value:
                    current_config[key] = value
                    updates_made = True

        if bot_engine and bot_engine.is_running:
             # If running, we generally don't want to allow hot-swapping crucial config that might break state
             # But if the user insists, we might need a force flag. For now, keep safety check.
            return jsonify({'success': False, 'message': 'Please stop the bot before updating configuration'}), 400
 
        if updates_made:
            save_config(current_config)

            # If bot is not running, reload its config immediately if instance exists
            if bot_engine and not bot_engine.is_running:
                bot_engine.config = bot_engine._load_config()
            
        return jsonify({'success': True, 'message': 'Configuration updated successfully'})

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500
@app.route('/api/test_api_key', methods=['POST'])
def test_api_key_route():
    try:
        data = request.json
        api_key = data.get('api_key')
        api_secret = data.get('api_secret')
        passphrase = data.get('passphrase')
        use_testnet = data.get('use_testnet')

        if not all([api_key, api_secret, passphrase]):
            return jsonify({'success': False, 'message': 'All API credentials (Key, Secret, Passphrase) are required.'}), 400

        # Temporarily create a bot_engine instance to test credentials
        # This bypasses the global bot_engine state
        temp_bot_engine = TradingBotEngine(config_file, emit_to_client)
        temp_bot_engine.config['okx_api_key'] = api_key
        temp_bot_engine.config['okx_api_secret'] = api_secret
        temp_bot_engine.config['okx_passphrase'] = passphrase
        temp_bot_engine.config['okx_demo_api_key'] = api_key # Also set for demo if testnet is used
        temp_bot_engine.config['okx_demo_api_secret'] = api_secret
        temp_bot_engine.config['okx_demo_api_passphrase'] = passphrase
        temp_bot_engine.config['use_testnet'] = use_testnet
        
        # Re-initialize global API credentials for the temp bot engine based on the provided data
        if use_testnet:
            temp_bot_engine.config['okx_api_key'] = temp_bot_engine.config['okx_demo_api_key']
            temp_bot_engine.config['okx_api_secret'] = temp_bot_engine.config['okx_demo_api_secret']
            temp_bot_engine.config['okx_passphrase'] = temp_bot_engine.config['okx_demo_api_passphrase']
            temp_bot_engine.okx_simulated_trading_header = {'x-simulated-trading': '1'}
        else:
            temp_bot_engine.okx_simulated_trading_header = {}

        if temp_bot_engine.test_api_credentials():
            return jsonify({'success': True, 'message': 'API credentials are valid.'})
        else:
            return jsonify({'success': False, 'message': 'Invalid API credentials or connection error.'}), 401

    except Exception as e:
        logging.error(f'Error testing API key: {str(e)}', exc_info=True)
        return jsonify({'success': False, 'message': f'An unexpected error occurred: {str(e)}'}), 500


@app.route('/api/status', methods=['GET'])
def get_status():
    if not bot_engine:
        return jsonify({
            'running': False,
            'balance': 0.0,
            'open_trades': [],
            'in_position': False,
            'position_entry_price': 0.0,
            'position_qty': 0.0,
            'current_take_profit': 0.0,
            'current_stop_loss': 0.0
        })

    return jsonify({
        'running': bot_engine.is_running,
        'balance': bot_engine.account_balance, # Use actual account balance
        'open_trades': bot_engine.open_trades,
        'in_position': bot_engine.in_position,
        'position_entry_price': bot_engine.position_entry_price,
        'position_qty': bot_engine.position_qty,
        'current_take_profit': bot_engine.current_take_profit,
        'current_stop_loss': bot_engine.current_stop_loss
    })
 
@socketio.on('connect')
def handle_connect(sid):
    logging.info(f'Client connected: {sid}')
    emit('connection_status', {'connected': True}, room=sid)
 
    if bot_engine:
        emit('bot_status', {'running': bot_engine.is_running}, room=sid)
        # Emit current account info
        with bot_engine.account_info_lock:
            total_balance = bot_engine.account_balance
            available_balance = bot_engine.available_balance
            max_allowed_margin = bot_engine.config['max_allowed_used']
            rate_divisor = bot_engine.config['rate_divisor']
            leverage = float(bot_engine.config.get('leverage', 1)) 
            if leverage <= 0: leverage = 1
            
            # Max Amount Display -> Max Allowed (Unleveraged) / Divisor
            max_amount_display = max_allowed_margin / rate_divisor

            # Remaining Logic: (Max Amount * Leverage) - Used Notional
            # Max Amount is the display value (max_allowed / divisor)
            max_notional_capacity = max_amount_display * leverage
            
            # Use the used_amount_notional tracked by bot_engine which sums up positions
            used_amount_notional = 0.0
            with bot_engine.position_lock:
                 used_amount_notional = bot_engine.used_amount_notional

            remaining_amount_notional = max_notional_capacity - used_amount_notional

        emit('account_update', {
            'total_capital': bot_engine.initial_total_capital or total_balance,
            'max_allowed_used_display': max_allowed_margin, # Unleveraged
            'max_amount_display': max_amount_display,       # Unleveraged
            'used_amount': used_amount_notional,            # Leveraged
            'remaining_amount': remaining_amount_notional,  # Leveraged
            'total_balance': total_balance,
            'available_balance': available_balance,
            'net_profit': bot_engine.net_profit,
            'total_trades': len(bot_engine.open_trades)
        }, room=sid)
        
        emit('trades_update', {'trades': bot_engine.open_trades}, room=sid)
        # Emit current position data
        emit('position_update', {
            'in_position': bot_engine.in_position,
            'position_entry_price': bot_engine.position_entry_price,
            'position_qty': bot_engine.position_qty,
            'current_take_profit': bot_engine.current_take_profit,
            'current_stop_loss': bot_engine.current_stop_loss
        }, room=sid)
 
        for log in list(bot_engine.console_logs):
            emit('console_log', log, room=sid)

@socketio.on('disconnect')
def handle_disconnect():
    logging.info('Client disconnected')

@socketio.on('start_bot')
def handle_start_bot(data=None):
    global bot_engine
    print("--- DEBUG: handle_start_bot called ---", flush=True)

    try:
        config = load_config() # This is line 111

        if bot_engine and bot_engine.is_running:
            emit('error', {'message': 'Bot is already running'})
            return

        try:
            bot_engine = TradingBotEngine(config_file, emit_to_client) # Pass config_file
            bot_engine.start()
            emit('bot_status', {'running': True}) # Explicitly emit status after starting
            emit('success', {'message': 'Bot started successfully'})
        except Exception as e:
            logging.error(f'Error during bot_engine instantiation or start: {str(e)}', exc_info=True)
            emit('error', {'message': f'Failed to start bot: {str(e)}'})
    except Exception as e: # Catch errors from load_config()
        logging.error(f'Error loading configuration in handle_start_bot: {str(e)}', exc_info=True)
        emit('error', {'message': f'Failed to start bot due to config error: {str(e)}'})

@socketio.on('stop_bot')
def handle_stop_bot(data=None):
    global bot_engine

    try:
        if not bot_engine or not bot_engine.is_running:
            emit('error', {'message': 'Bot is not running'})
            return

        bot_engine.stop()
        emit('bot_status', {'running': False}) # Explicitly emit status after stopping
        emit('success', {'message': 'Bot stopped successfully'})

    except Exception as e:
        logging.error(f'Error stopping bot: {str(e)}')
        emit('error', {'message': f'Failed to stop bot: {str(e)}'})

@socketio.on('clear_console')
def handle_clear_console(data=None):
    if bot_engine:
        bot_engine.console_logs.clear()
    emit('console_cleared', {})

@socketio.on('batch_modify_tpsl')
def handle_batch_modify_tpsl(data=None):
    global bot_engine
    if bot_engine:
        bot_engine.batch_modify_tpsl()
    else:
        emit('error', {'message': 'Bot is not running.'})

@socketio.on('batch_cancel_orders')
def handle_batch_cancel_orders(data=None):
    global bot_engine
    if bot_engine:
        bot_engine.batch_cancel_orders()
    else:
        emit('error', {'message': 'Bot is not running.'})

@socketio.on('emergency_sl')
def handle_emergency_sl(data=None):
    global bot_engine
    if bot_engine:
        bot_engine.emergency_sl()
    else:
        emit('error', {'message': 'Bot is not running.'})


if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=5000, debug=False, use_reloader=False, log_output=True, allow_unsafe_werkzeug=True)
