const socket = io();

let currentConfig = null;
const configModal = new bootstrap.Modal(document.getElementById('configModal'));
let isBotRunning = false;
let orderExpirationCache = {}; // Cache to store calcualted expiration timestamps

document.addEventListener('DOMContentLoaded', () => {
    initializeTheme();
    loadConfig().then(() => {
        setupEventListeners();
        setupSocketListeners();
        loadStatus(); // Load initial status after listeners are set up
        startUITimers(); // Start local countdown ticks
    });
});

function initializeTheme() {
    const savedTheme = localStorage.getItem('theme') || 'dark';
    document.body.setAttribute('data-theme', savedTheme);
    document.getElementById('themeToggle').checked = savedTheme === 'light';
    updateThemeIcon(savedTheme);
}

function updateThemeIcon(theme) {
    const icon = document.getElementById('themeIcon');
    icon.className = theme === 'light' ? 'bi bi-sun-fill' : 'bi bi-moon-stars';
}

function setupEventListeners() {
    document.getElementById('themeToggle').addEventListener('change', (e) => {
        const theme = e.target.checked ? 'light' : 'dark';
        document.body.setAttribute('data-theme', theme);
        localStorage.setItem('theme', theme);
        updateThemeIcon(theme);
    });

    document.getElementById('startStopBtn').addEventListener('click', () => {
        const btn = document.getElementById('startStopBtn');
        btn.disabled = true; // Disable button to prevent double clicks

        if (isBotRunning) {
            socket.emit('stop_bot');
        } else {
            socket.emit('start_bot');
        }
    });

    document.getElementById('configBtn').addEventListener('click', () => {
        loadConfigToModal();
        configModal.show();
    });

    document.getElementById('saveConfigBtn').addEventListener('click', () => {
        saveConfig();
    });

    document.getElementById('clearConsoleBtn').addEventListener('click', () => {
        socket.emit('clear_console');
        document.getElementById('consoleOutput').innerHTML = '<p class="text-muted">Console cleared</p>';
    });

    // Event listener for Emergency SL button
    document.getElementById('emergencySlBtn').addEventListener('click', () => {
        if (confirm('Are you sure you want to trigger an emergency Stop Loss? This will close all open positions at market price.')) {
            socket.emit('emergency_sl');
        }
    });

    // Event listener for Batch Modify TP/SL button
    document.getElementById('batchModifyTPSLBtn').addEventListener('click', () => {
        if (confirm('Are you sure you want to batch modify TP/SL for all open orders?')) {
            socket.emit('batch_modify_tpsl');
        }
    });

    // Event listener for Batch Cancel Orders button
    document.getElementById('batchCancelOrdersBtn').addEventListener('click', () => {
        if (confirm('Are you sure you want to batch cancel all open orders?')) {
            socket.emit('batch_cancel_orders');
        }
    });

    // Event listener for useCandlestickConditions checkbox
    document.getElementById('useCandlestickConditions').addEventListener('change', toggleCandlestickInputs);
    // Call on load to set initial state
    toggleCandlestickInputs();

    // PnL Auto-Cancel listeners
    document.getElementById('usePnlAutoCancel').addEventListener('change', () => {
        saveLiveConfigs();
    });
    document.getElementById('pnlAutoCancelThreshold').addEventListener('change', () => {
        saveLiveConfigs();
    });
    document.getElementById('tradeFeePercentage').addEventListener('change', () => {
        saveLiveConfigs();
    });

    // Refresh Fees Button
    document.getElementById('refreshFeesBtn').addEventListener('click', () => {
        loadStatus();
    });

    document.getElementById('testApiKeyBtn').addEventListener('click', testApiKey);
}

function toggleCandlestickInputs() {
    const isChecked = document.getElementById('useCandlestickConditions').checked;
    const elementsToToggle = [
        document.getElementById('candlestickTimeframe'),
        document.getElementById('useChgOpenClose'),
        document.getElementById('minChgOpenClose'),
        document.getElementById('maxChgOpenClose'),
        document.getElementById('useChgHighLow'),
        document.getElementById('minChgHighLow'),
        document.getElementById('maxChgHighLow'),
        document.getElementById('useChgHighClose'),
        document.getElementById('minChgHighClose'),
        document.getElementById('maxChgHighClose'),
    ];

    elementsToToggle.forEach(element => {
        element.disabled = !isChecked;
        // Also ensure checkboxes are unchecked if the main toggle is off
        if (element.type === 'checkbox' && !isChecked) {
            element.checked = false;
        }
        // Also clear numeric inputs if disabled
        if (element.type === 'number' && !isChecked) {
            element.value = 0;
        }
    });
}

async function testApiKey() {
    const testBtn = document.getElementById('testApiKeyBtn');
    const originalBtnHtml = testBtn.innerHTML; // Store original button content
    testBtn.disabled = true;
    testBtn.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Testing...'; // Show loading spinner

    const useTestnet = document.getElementById('useTestnet').checked;
    const useDev = document.getElementById('useDeveloperApi').checked;
    let apiKey, apiSecret, passphrase;

    if (useDev) {
        if (useTestnet) {
            apiKey = document.getElementById('devDemoApiKey').value;
            apiSecret = document.getElementById('devDemoApiSecret').value;
            passphrase = document.getElementById('devDemoApiPassphrase').value;
        } else {
            apiKey = document.getElementById('devApiKey').value;
            apiSecret = document.getElementById('devApiSecret').value;
            passphrase = document.getElementById('devPassphrase').value;
        }
    } else {
        if (useTestnet) {
            apiKey = document.getElementById('okxDemoApiKey').value;
            apiSecret = document.getElementById('okxDemoApiSecret').value;
            passphrase = document.getElementById('okxDemoApiPassphrase').value;
        } else {
            apiKey = document.getElementById('okxApiKey').value;
            apiSecret = document.getElementById('okxApiSecret').value;
            passphrase = document.getElementById('okxPassphrase').value;
        }
    }

    try {
        const response = await fetch('/api/test_api_key', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                api_key: apiKey,
                api_secret: apiSecret,
                passphrase: passphrase,
                use_testnet: useTestnet
            }),
        });

        const result = await response.json();

        if (result.success) {
            showNotification('API Key test successful: ' + result.message, 'success');
        } else {
            showNotification('API Key test failed: ' + result.message, 'error');
        }
    } catch (error) {
        console.error('Error testing API key:', error);
        showNotification('Failed to connect to API test endpoint', 'error');
    } finally {
        testBtn.disabled = false;
        testBtn.innerHTML = originalBtnHtml; // Restore original button content
    }
}
function setupSocketListeners() {
    socket.on('connection_status', (data) => {
        console.log('Connected to server:', data);
    });

    socket.on('bot_status', (data) => {
        updateBotStatus(data.running);
    });

    socket.on('account_update', (data) => {
        updateAccountMetrics(data);
    });

    socket.on('trades_update', (data) => {
        updateOpenTrades(data.trades);
    });

    socket.on('position_update', (data) => {
        updatePositionDisplay(data);
    });

    socket.on('console_log', (data) => {
        addConsoleLog(data);
    });

    socket.on('console_cleared', () => {
        document.getElementById('consoleOutput').innerHTML = '<p class="text-muted">Console cleared</p>';
    });

    socket.on('price_update', (data) => {
    });

    socket.on('success', (data) => {
        showNotification(data.message, 'success');
    });

    socket.on('error', (data) => {
        showNotification(data.message, 'error');
        // Re-enable start/stop button if it was disabled during an attempt
        const btn = document.getElementById('startStopBtn');
        if (btn) btn.disabled = false;
    });

    socket.on('connect', () => {
        console.log('WebSocket connected');
        loadStatus();
    });

    socket.on('disconnect', () => {
        console.log('WebSocket disconnected');
    });
}

function updateBotStatus(running) {
    isBotRunning = running;
    const statusBadge = document.getElementById('botStatus');
    const startStopBtn = document.getElementById('startStopBtn');
    const btnIcon = startStopBtn.querySelector('i');
    const btnText = startStopBtn.querySelector('span');

    if (running) {
        statusBadge.textContent = 'Running';
        statusBadge.className = 'badge status-badge running';
        startStopBtn.className = 'btn btn-danger';
        btnIcon.className = 'bi bi-stop-fill';
        btnText.textContent = 'Stop';
    } else {
        statusBadge.textContent = 'Stopped';
        statusBadge.className = 'badge status-badge stopped';
        startStopBtn.className = 'btn btn-success';
        btnIcon.className = 'bi bi-play-fill';
        btnText.textContent = 'Start';
    }
    startStopBtn.disabled = false; // Re-enable the button
}

function updateAccountMetrics(data) {
    document.getElementById('totalCapital').textContent = `$${data.total_capital !== undefined ? Number(data.total_capital).toFixed(2) : '0.00'}`;
    document.getElementById('maxAllowedUsedDisplay').textContent = `$${data.max_allowed_used_display !== undefined ? Number(data.max_allowed_used_display).toFixed(2) : '0.00'}`;
    document.getElementById('maxAmountDisplay').textContent = `$${data.max_amount_display !== undefined ? Number(data.max_amount_display).toFixed(2) : '0.00'}`;
    document.getElementById('usedAmount').textContent = `$${data.used_amount !== undefined ? Number(data.used_amount).toFixed(2) : '0.00'}`;
    document.getElementById('remainingAmount').textContent = `$${data.remaining_amount !== undefined ? Number(data.remaining_amount).toFixed(2) : '0.00'}`;
    document.getElementById('balance').textContent = `$${data.total_balance !== undefined ? Number(data.total_balance).toFixed(2) : '0.00'}`;
    document.getElementById('netProfit').textContent = `$${data.net_profit !== undefined ? Number(data.net_profit).toFixed(2) : '0.00'}`;
    document.getElementById('totalTrades').textContent = data.total_trades !== undefined ? data.total_trades : '0';

    // Calculate fee breakdown based on user's formula
    const feeRate = currentConfig?.trade_fee_percentage || 0.07;
    const usedAmount = data.used_amount || 0;
    const remainingAmount = data.remaining_amount || 0;

    const usedFee = (usedAmount * feeRate) / 100;
    const remainingFee = (remainingAmount * feeRate) / 100;
    const totalFee = usedFee + remainingFee;

    document.getElementById('tradeFees').textContent = `$${totalFee.toFixed(2)}`;
    document.getElementById('usedFee').textContent = `$${usedFee.toFixed(2)}`;
    document.getElementById('remainingFee').textContent = `$${remainingFee.toFixed(2)}`;
    document.getElementById('feeRateDisplay').textContent = `${feeRate.toFixed(3)}%`;
}

function updatePositionDisplay(positionData) {
    const mlResultsContainer = document.getElementById('mlStrategyResults');

    if (!positionData || (!positionData.in_position && (!positionData.positions || (!positionData.positions.long.in && !positionData.positions.short.in)))) {
        mlResultsContainer.innerHTML = '<p class="text-muted">No active position.</p>';
        return;
    }

    let positionsToRender = [];

    if (positionData.positions) {
        if (positionData.positions.long.in) {
            positionsToRender.push({ side: 'LONG', ...positionData.positions.long });
        }
        if (positionData.positions.short.in) {
            positionsToRender.push({ side: 'SHORT', ...positionData.positions.short });
        }
    } else if (positionData.in_position) {
        // Fallback for older data format or primary display
        positionsToRender.push({
            side: (positionData.position_qty > 0 ? 'LONG' : 'SHORT'),
            price: positionData.position_entry_price,
            qty: positionData.position_qty,
            tp: positionData.current_take_profit,
            sl: positionData.current_stop_loss
        });
    }

    let positionHtml = '';
    positionsToRender.forEach(pos => {
        positionHtml += `
            <div class="position-card mb-2 p-2 border rounded ${pos.side.toLowerCase()}-bg">
                <div class="d-flex justify-content-between align-items-center mb-1">
                    <h6 class="mb-0 text-${pos.side === 'LONG' ? 'success' : 'danger'} font-weight-bold">${pos.side} POSITION</h6>
                    <span class="badge bg-${pos.side === 'LONG' ? 'success' : 'danger'}">Active</span>
                </div>
                <div class="row g-0">
                    <div class="col-6 small text-muted">Entry Price:</div>
                    <div class="col-6 small text-end">${Number(pos.price || 0).toFixed(4)}</div>
                    <div class="col-6 small text-muted">Quantity:</div>
                    <div class="col-6 small text-end">${Number(pos.qty || 0).toFixed(4)}</div>
                    <div class="col-6 small text-muted">Current TP:</div>
                    <div class="col-6 small text-end text-success">${Number(pos.tp || (positionData && positionData.current_take_profit) || 0).toFixed(4)}</div>
                    <div class="col-6 small text-muted">Current SL:</div>
                    <div class="col-6 small text-end text-danger">${Number(pos.sl || (positionData && positionData.current_stop_loss) || 0).toFixed(4)}</div>
                </div>
            </div>
        `;
    });

    mlResultsContainer.innerHTML = positionHtml;
}

function updateParametersDisplay() {
    const paramsContainer = document.getElementById('currentParams');
    if (currentConfig) {
        let configHtml = `
           <div class="param-item">
               <span class="param-label">Symbol:</span>
               <span class="param-value">${currentConfig.symbol}</span>
           </div>
           <div class="param-item">
               <span class="param-label">Direction:</span>
               <span class="param-value">${currentConfig.direction}</span>
           </div>
           <div class="param-item">
               <span class="param-label">Mode:</span>
               <span class="param-value">${currentConfig.mode}</span>
           </div>
           <div class="param-item">
               <span class="param-label">Leverage:</span>
               <span class="param-value">${currentConfig.leverage}x</span>
           </div>
           <div class="param-item">
               <span class="param-label">Max Allowed Used (USDT):</span>
               <span class="param-value">${currentConfig.max_allowed_used}</span>
           </div>
           <div class="param-item">
               <span class="param-label">Target Order Amount:</span>
               <span class="param-value">${currentConfig.target_order_amount}</span>
           </div>
           <div class="param-item">
               <span class="param-label">Min Order Amount:</span>
               <span class="param-value">${currentConfig.min_order_amount}</span>
           </div>
           <div class="param-item">
               <span class="param-label">Entry Price Offset:</span>
               <span class="param-value">${currentConfig.entry_price_offset}</span>
           </div>
           <div class="param-item">
               <span class="param-label">Batch Offset:</span>
               <span class="param-value">${currentConfig.batch_offset}</span>
           </div>
           <div class="param-item">
               <span class="param-label">Batch Size Per Loop:</span>
               <span class="param-value">${currentConfig.batch_size_per_loop}</span>
           </div>
           <div class="param-item">
               <span class="param-label">Loop Time:</span>
               <span class="param-value">${currentConfig.loop_time_seconds}s</span>
           </div>
           <div class="param-item">
               <span class="param-label">Rate Divisor:</span>
               <span class="param-value">${currentConfig.rate_divisor}</span>
           </div>
           <div class="param-item">
               <span class="param-label">Short Safety Line Price:</span>
               <span class="param-value">${currentConfig.short_safety_line_price}</span>
           </div>
           <div class="param-item">
               <span class="param-label">Long Safety Line Price:</span>
               <span class="param-value">${currentConfig.long_safety_line_price}</span>
           </div>
           <div class="param-item">
               <span class="param-label">TP Price Offset:</span>
               <span class="param-value">${currentConfig.tp_price_offset}</span>
           </div>
           <div class="param-item">
               <span class="param-label">SL Price Offset:</span>
               <span class="param-value">${currentConfig.sl_price_offset}</span>
           </div>
           <div class="param-item">
               <span class="param-label">TP Amount (%):</span>
               <span class="param-value">${currentConfig.tp_amount}</span>
           </div>
           <div class="param-item">
               <span class="param-label">SL Amount (%):</span>
               <span class="param-value">${currentConfig.sl_amount}</span>
           </div>
           <div class="param-item">
               <span class="param-label">Trigger Price:</span>
               <span class="param-value">${currentConfig.trigger_price}</span>
           </div>
           <div class="param-item">
               <span class="param-label">TP Mode:</span>
               <span class="param-value">${currentConfig.tp_mode}</span>
           </div>
           <div class="param-item">
               <span class="param-label">TP Type:</span>
               <span class="param-value">${currentConfig.tp_type}</span>
           </div>
            <div class="param-item">
                <span class="param-label">Trade Fee %:</span>
                <span class="param-value">${currentConfig.trade_fee_percentage}%</span>
            </div>
            <div class="param-item">
                <span class="param-label">Cancel Unfilled (s):</span>
                <span class="param-value">${currentConfig.cancel_unfilled_seconds}</span>
            </div>
           <div class="param-item">
               <span class="param-label">Cancel if TP unfavorable:</span>
               <span class="param-value">${currentConfig.cancel_on_tp_price_below_market ? 'Yes' : 'No'}</span>
           </div>
           <div class="param-item">
               <span class="param-label">Cancel if Entry unfavorable:</span>
               <span class="param-value">${currentConfig.cancel_on_entry_price_below_market ? 'Yes' : 'No'}</span>
           </div>
           <div class="param-item">
               <span class="param-label">Use Candlestick Conditions:</span>
               <span class="param-value">${currentConfig.use_candlestick_conditions ? 'Yes' : 'No'}</span>
           </div>
           <div class="param-item">
               <span class="param-label">Candlestick Timeframe:</span>
               <span class="param-value">${currentConfig.candlestick_timeframe}</span>
           </div>
           <div class="param-item">
               <span class="param-label">Use Chg Open/Close:</span>
               <span class="param-value">${currentConfig.use_chg_open_close ? 'Yes' : 'No'}</span>
           </div>
           <div class="param-item">
               <span class="param-label">Min Chg Open/Close:</span>
               <span class="param-value">${currentConfig.min_chg_open_close}</span>
           </div>
           <div class="param-item">
               <span class="param-label">Max Chg Open/Close:</span>
               <span class="param-value">${currentConfig.max_chg_open_close}</span>
           </div>
           <div class="param-item">
               <span class="param-label">Use Chg High/Low:</span>
               <span class="param-value">${currentConfig.use_chg_high_low ? 'Yes' : 'No'}</span>
           </div>
           <div class="param-item">
               <span class="param-label">Min Chg High/Low:</span>
               <span class="param-value">${currentConfig.min_chg_high_low}</span>
           </div>
           <div class="param-item">
               <span class="param-label">Max Chg High/Low:</span>
               <span class="param-value">${currentConfig.max_chg_high_low}</span>
           </div>
           <div class="param-item">
               <span class="param-label">Use Chg High/Close:</span>
               <span class="param-value">${currentConfig.use_chg_high_close ? 'Yes' : 'No'}</span>
           </div>
           <div class="param-item">
               <span class="param-label">Min Chg High/Close:</span>
               <span class="param-value">${currentConfig.min_chg_high_close}</span>
           </div>
           <div class="param-item">
               <span class="param-label">Max Chg High/Close:</span>
               <span class="param-value">${currentConfig.max_chg_high_close}</span>
           </div>
       `;
        paramsContainer.innerHTML = configHtml;
    } else {
        paramsContainer.innerHTML = '<p class="text-muted">No parameters loaded yet.</p>';
    }
}

function updateOpenTrades(trades) {
    const tradesContainer = document.getElementById('openTrades');

    if (!trades || trades.length === 0) {
        tradesContainer.innerHTML = '<p class="text-muted">No open positions</p>';
        return;
    }

    tradesContainer.innerHTML = trades.map(trade => {
        // Cache the expiration target timestamp to avoid server stutter
        if (trade.time_left !== null) {
            orderExpirationCache[trade.id] = Date.now() + (trade.time_left * 1000);
        } else {
            delete orderExpirationCache[trade.id];
        }

        return `
        <div class="trade-card ${trade.type.toLowerCase()}">
            <div class="trade-header">
                <span class="trade-type ${trade.type.toLowerCase()}">${trade.type}</span>
                <span class="trade-id">ID: ${trade.id} <span class="badge bg-warning text-dark ms-1 timer-badge" data-order-id="${trade.id}">${trade.time_left !== null ? trade.time_left + 's' : ''}</span></span>
            </div>
            <div class="trade-details">
                <div class="trade-detail-item">
                    <span class="trade-detail-label">Entry:</span>
                    <span class="trade-detail-value">${trade.entry_spot_price !== null ? trade.entry_spot_price.toFixed(4) : 'N/A'}</span>
                </div>
                <div class="trade-detail-item">
                    <span class="param-label">Target Order:</span>
                    <span class="param-value">$${trade.stake !== null ? trade.stake.toFixed(2) : 'N/A'}</span>
                </div>
                <div class="trade-detail-item">
                    <span class="trade-detail-label">TP:</span>
                    <span class="trade-detail-value text-success">${trade.tp_price !== null ? trade.tp_price.toFixed(4) : 'N/A'}</span>
                </div>
                <div class="trade-detail-item">
                    <span class="trade-detail-label">SL:</span>
                    <span class="trade-detail-value text-danger">${trade.sl_price !== null ? trade.sl_price.toFixed(4) : 'N/A'}</span>
                </div>
            </div>
        </div>
    `;
    }).join('');
}

function startUITimers() {
    // Precise local countdown timer
    setInterval(() => {
        const now = Date.now();
        const badges = document.querySelectorAll('.timer-badge');
        badges.forEach(badge => {
            const orderId = badge.getAttribute('data-order-id');
            const targetTime = orderExpirationCache[orderId];

            if (targetTime) {
                const remaining = Math.max(0, Math.floor((targetTime - now) / 1000));
                badge.textContent = remaining + 's';

                // Cleanup cache if reached 0
                if (remaining <= 0) delete orderExpirationCache[orderId];
            }
        });
    }, 1000);
}

function addConsoleLog(log) {
    const consoleOutput = document.getElementById('consoleOutput');

    if (consoleOutput.querySelector('.text-muted')) {
        consoleOutput.innerHTML = '';
    }

    const logLine = document.createElement('div');
    logLine.className = `console-line ${log.level}`;
    logLine.innerHTML = `
        <span class="console-timestamp">[${log.timestamp}]</span>
        <span class="console-message">${escapeHtml(log.message)}</span>
    `;

    // Check if user is near the bottom
    const isAtBottom = consoleOutput.scrollHeight - consoleOutput.clientHeight <= consoleOutput.scrollTop + 50;

    consoleOutput.appendChild(logLine);

    if (isAtBottom) {
        consoleOutput.scrollTop = consoleOutput.scrollHeight;
    }

    if (consoleOutput.children.length > 500) {
        consoleOutput.removeChild(consoleOutput.firstChild);
    }
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

async function loadConfig() {
    try {
        const response = await fetch('/api/config');
        currentConfig = await response.json();

        // Sync PnL Auto-Cancel UI
        if (currentConfig.use_pnl_auto_cancel !== undefined) {
            document.getElementById('usePnlAutoCancel').checked = currentConfig.use_pnl_auto_cancel;
        }
        if (currentConfig.pnl_auto_cancel_threshold !== undefined) {
            document.getElementById('pnlAutoCancelThreshold').value = currentConfig.pnl_auto_cancel_threshold;
        }

        // Initial dashboard trade fee % sync
        const feeInput = document.getElementById('tradeFeePercentage');
        if (feeInput) {
            feeInput.value = currentConfig.trade_fee_percentage !== undefined ? currentConfig.trade_fee_percentage : 0.07;
        }
    } catch (error) {
        console.error('Error loading config:', error);
        showNotification('Failed to load configuration', 'error');
    }
}

async function loadStatus() {
    try {
        const response = await fetch('/api/status');
        const status = await response.json();

        updateBotStatus(status.running);
        updateAccountMetrics(status);
        updateOpenTrades(status.open_trades);
        updatePositionDisplay(status);
        updateParametersDisplay(); // Call the new function to populate parameters tab
    } catch (error) {
        console.error('Error loading status:', error);
    }
}

function loadConfigToModal() {
    if (!currentConfig) return;

    document.getElementById('okxApiKey').value = currentConfig.okx_api_key;
    document.getElementById('okxApiSecret').value = currentConfig.okx_api_secret;
    document.getElementById('okxPassphrase').value = currentConfig.okx_passphrase;
    document.getElementById('okxDemoApiKey').value = currentConfig.okx_demo_api_key;
    document.getElementById('okxDemoApiSecret').value = currentConfig.okx_demo_api_secret;
    document.getElementById('okxDemoApiPassphrase').value = currentConfig.okx_demo_api_passphrase;
    document.getElementById('devApiKey').value = currentConfig.dev_api_key;
    document.getElementById('devApiSecret').value = currentConfig.dev_api_secret;
    document.getElementById('devPassphrase').value = currentConfig.dev_passphrase;
    document.getElementById('devDemoApiKey').value = currentConfig.dev_demo_api_key;
    document.getElementById('devDemoApiSecret').value = currentConfig.dev_demo_api_secret;
    document.getElementById('devDemoApiPassphrase').value = currentConfig.dev_demo_api_passphrase;
    document.getElementById('useTestnet').checked = currentConfig.use_testnet;
    document.getElementById('useDeveloperApi').checked = currentConfig.use_developer_api;
    document.getElementById('symbol').value = currentConfig.symbol;
    document.getElementById('shortSafetyLinePrice').value = currentConfig.short_safety_line_price;
    document.getElementById('longSafetyLinePrice').value = currentConfig.long_safety_line_price;
    document.getElementById('leverage').value = currentConfig.leverage;
    document.getElementById('maxAllowedUsed').value = currentConfig.max_allowed_used;
    document.getElementById('entryPriceOffset').value = currentConfig.entry_price_offset;
    document.getElementById('batchOffset').value = currentConfig.batch_offset;
    document.getElementById('tpPriceOffset').value = currentConfig.tp_price_offset;
    document.getElementById('slPriceOffset').value = currentConfig.sl_price_offset;
    document.getElementById('loopTimeSeconds').value = currentConfig.loop_time_seconds;
    document.getElementById('rateDivisor').value = currentConfig.rate_divisor;
    document.getElementById('batchSizePerLoop').value = currentConfig.batch_size_per_loop;
    document.getElementById('minOrderAmount').value = currentConfig.min_order_amount;
    document.getElementById('targetOrderAmount').value = currentConfig.target_order_amount;
    document.getElementById('cancelUnfilledSeconds').value = currentConfig.cancel_unfilled_seconds;
    document.getElementById('cancelOnTpPriceBelowMarket').checked = currentConfig.cancel_on_tp_price_below_market;
    document.getElementById('cancelOnEntryPriceBelowMarket').checked = currentConfig.cancel_on_entry_price_below_market;
    document.getElementById('tradeFeePercentage').value = currentConfig.trade_fee_percentage || 0.07;

    // New fields
    document.getElementById('direction').value = currentConfig.direction;
    document.getElementById('mode').value = currentConfig.mode;
    document.getElementById('tpAmount').value = currentConfig.tp_amount;
    document.getElementById('slAmount').value = currentConfig.sl_amount;
    document.getElementById('triggerPrice').value = currentConfig.trigger_price;
    document.getElementById('tpMode').value = currentConfig.tp_mode;
    document.getElementById('tpType').value = currentConfig.tp_type;
    document.getElementById('useCandlestickConditions').checked = currentConfig.use_candlestick_conditions;

    // Candlestick conditions
    document.getElementById('useChgOpenClose').checked = currentConfig.use_chg_open_close;
    document.getElementById('minChgOpenClose').value = currentConfig.min_chg_open_close;
    document.getElementById('maxChgOpenClose').value = currentConfig.max_chg_open_close;
    document.getElementById('useChgHighLow').checked = currentConfig.use_chg_high_low;
    document.getElementById('minChgHighLow').value = currentConfig.min_chg_high_low;
    document.getElementById('maxChgHighLow').value = currentConfig.max_chg_high_low;
    document.getElementById('useChgHighClose').checked = currentConfig.use_chg_high_close;
    document.getElementById('minChgHighClose').value = currentConfig.min_chg_high_close;
    document.getElementById('maxChgHighClose').value = currentConfig.max_chg_high_close;
    document.getElementById('candlestickTimeframe').value = currentConfig.candlestick_timeframe;
    document.getElementById('okxPosMode').value = currentConfig.okx_pos_mode || 'net_mode';

    // PnL Auto-Cancel (Modal Sync)
    const autCancelCheck = document.getElementById('usePnlAutoCancelModal');
    const autCancelThreshold = document.getElementById('pnlAutoCancelThresholdModal');
    if (autCancelCheck) autCancelCheck.checked = currentConfig.use_pnl_auto_cancel;
    if (autCancelThreshold) autCancelThreshold.value = currentConfig.pnl_auto_cancel_threshold;
}

// Helper to keep dashboard and modal in sync
document.addEventListener('change', (e) => {
    if (e.target.id === 'usePnlAutoCancel') {
        document.getElementById('usePnlAutoCancelModal').checked = e.target.checked;
    } else if (e.target.id === 'usePnlAutoCancelModal') {
        document.getElementById('usePnlAutoCancel').checked = e.target.checked;
    } else if (e.target.id === 'pnlAutoCancelThreshold') {
        document.getElementById('pnlAutoCancelThresholdModal').value = e.target.value;
    } else if (e.target.id === 'pnlAutoCancelThresholdModal') {
        document.getElementById('pnlAutoCancelThreshold').value = e.target.value;
    }
});

async function saveConfig() {
    const newConfig = {
        okx_api_key: document.getElementById('okxApiKey').value,
        okx_api_secret: document.getElementById('okxApiSecret').value,
        okx_passphrase: document.getElementById('okxPassphrase').value,
        okx_demo_api_key: document.getElementById('okxDemoApiKey').value,
        okx_demo_api_secret: document.getElementById('okxDemoApiSecret').value,
        okx_demo_api_passphrase: document.getElementById('okxDemoApiPassphrase').value,
        dev_api_key: document.getElementById('devApiKey').value,
        dev_api_secret: document.getElementById('devApiSecret').value,
        dev_passphrase: document.getElementById('devPassphrase').value,
        dev_demo_api_key: document.getElementById('devDemoApiKey').value,
        dev_demo_api_secret: document.getElementById('devDemoApiSecret').value,
        dev_demo_api_passphrase: document.getElementById('devDemoApiPassphrase').value,
        use_developer_api: document.getElementById('useDeveloperApi').checked,
        use_testnet: document.getElementById('useTestnet').checked,
        symbol: document.getElementById('symbol').value,
        short_safety_line_price: parseFloat(document.getElementById('shortSafetyLinePrice').value),
        long_safety_line_price: parseFloat(document.getElementById('longSafetyLinePrice').value),
        leverage: parseInt(document.getElementById('leverage').value),
        max_allowed_used: parseFloat(document.getElementById('maxAllowedUsed').value),
        entry_price_offset: parseFloat(document.getElementById('entryPriceOffset').value),
        batch_offset: parseFloat(document.getElementById('batchOffset').value),
        tp_price_offset: parseFloat(document.getElementById('tpPriceOffset').value),
        sl_price_offset: parseFloat(document.getElementById('slPriceOffset').value),
        loop_time_seconds: parseInt(document.getElementById('loopTimeSeconds').value),
        rate_divisor: parseInt(document.getElementById('rateDivisor').value),
        batch_size_per_loop: parseInt(document.getElementById('batchSizePerLoop').value),
        min_order_amount: parseFloat(document.getElementById('minOrderAmount').value),
        target_order_amount: parseFloat(document.getElementById('targetOrderAmount').value),
        cancel_unfilled_seconds: parseInt(document.getElementById('cancelUnfilledSeconds').value),
        cancel_on_tp_price_below_market: document.getElementById('cancelOnTpPriceBelowMarket').checked,
        cancel_on_entry_price_below_market: document.getElementById('cancelOnEntryPriceBelowMarket').checked,
        trade_fee_percentage: parseFloat(document.getElementById('tradeFeePercentage').value),

        // New fields
        direction: document.getElementById('direction').value,
        mode: document.getElementById('mode').value,
        tp_amount: parseFloat(document.getElementById('tpAmount').value),
        sl_amount: parseFloat(document.getElementById('slAmount').value),
        trigger_price: document.getElementById('triggerPrice').value,
        tp_mode: document.getElementById('tpMode').value,
        tp_type: document.getElementById('tpType').value,
        use_candlestick_conditions: document.getElementById('useCandlestickConditions').checked,

        // Candlestick conditions
        use_chg_open_close: document.getElementById('useChgOpenClose').checked,
        min_chg_open_close: parseFloat(document.getElementById('minChgOpenClose').value),
        max_chg_open_close: parseFloat(document.getElementById('maxChgOpenClose').value),
        use_chg_high_low: document.getElementById('useChgHighLow').checked,
        min_chg_high_low: parseFloat(document.getElementById('minChgHighLow').value),
        max_chg_high_low: parseFloat(document.getElementById('maxChgHighLow').value),
        use_chg_high_close: document.getElementById('useChgHighClose').checked,
        min_chg_high_close: parseFloat(document.getElementById('minChgHighClose').value),
        max_chg_high_close: parseFloat(document.getElementById('maxChgHighClose').value),
        candlestick_timeframe: document.getElementById('candlestickTimeframe').value,
        okx_pos_mode: document.getElementById('okxPosMode').value,

        // PnL Auto-Cancel
        use_pnl_auto_cancel: document.getElementById('usePnlAutoCancel').checked,
        pnl_auto_cancel_threshold: parseFloat(document.getElementById('pnlAutoCancelThreshold').value)
    };

    try {
        const response = await fetch('/api/config', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(newConfig),
        });

        const result = await response.json();

        if (result.success) {
            currentConfig = newConfig;
            configModal.hide();
            showNotification(result.message || 'Configuration saved successfully', 'success');
            updateParametersDisplay(); // Refresh the parameters display
        } else {
            showNotification(result.message, 'error');
        }
    } catch (error) {
        console.error('Error saving config:', error);
        showNotification('Failed to save configuration', 'error');
    }
}

// Function to save specific configs without closing modal (live updates)
async function saveLiveConfigs() {
    if (!currentConfig) return;

    const liveConfig = {
        use_pnl_auto_cancel: document.getElementById('usePnlAutoCancel').checked,
        pnl_auto_cancel_threshold: parseFloat(document.getElementById('pnlAutoCancelThreshold').value),
        trade_fee_percentage: parseFloat(document.getElementById('tradeFeePercentage').value)
    };

    try {
        const response = await fetch('/api/config', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(liveConfig)
        });
        const data = await response.json();
        if (data.success) {
            showNotification('Auto-Exit settings saved', 'success');
            // Update local currentConfig but don't reload everything
            currentConfig.use_pnl_auto_cancel = liveConfig.use_pnl_auto_cancel;
            currentConfig.pnl_auto_cancel_threshold = liveConfig.pnl_auto_cancel_threshold;
            currentConfig.trade_fee_percentage = liveConfig.trade_fee_percentage;
        } else {
            // Revert UI on error (e.g. bot running error)
            document.getElementById('usePnlAutoCancel').checked = currentConfig.use_pnl_auto_cancel;
            document.getElementById('pnlAutoCancelThreshold').value = currentConfig.pnl_auto_cancel_threshold;
            document.getElementById('tradeFeePercentage').value = currentConfig.trade_fee_percentage;
            showNotification(data.message, 'error');
        }
    } catch (error) {
        console.error('Error saving live config:', error);
    }
}

function showNotification(message, type) {
    const alertDiv = document.createElement('div');
    alertDiv.className = `alert alert-${type === 'success' ? 'success' : 'danger'} alert-dismissible fade show position-fixed top-0 start-50 translate-middle-x mt-3`;
    alertDiv.style.zIndex = '10000'; // Increased z-index to ensure visibility
    alertDiv.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
    `;

    document.body.appendChild(alertDiv);

    setTimeout(() => {
        alertDiv.remove();
    }, 5000);
}
