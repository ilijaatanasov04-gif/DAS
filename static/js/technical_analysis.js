let availableCoins = [];
let currentSymbol = '';
let selectedCoinData = null;
let priceChart = null;

document.addEventListener('DOMContentLoaded', function() {
    loadAvailableCoins();

    // Restore state from sessionStorage
    restoreState();

    const coinSelector = document.getElementById('coinSelector');
    const coinDropdown = document.getElementById('coinDropdown');

    coinSelector.addEventListener('focus', function() {
        filterCoins('');
        coinDropdown.classList.add('show');
    });

    coinSelector.addEventListener('input', function() {
        const searchTerm = this.value.trim();
        filterCoins(searchTerm);
        if (!coinDropdown.classList.contains('show')) {
            coinDropdown.classList.add('show');
        }
    });

    coinSelector.addEventListener('keypress', function(e) {
        if (e.key === 'Enter') {
            e.preventDefault();
            const searchTerm = this.value.trim().toUpperCase();
            if (searchTerm) {
                selectCoin(searchTerm);
                this.value = '';
                coinDropdown.classList.remove('show');
            }
        }
    });

    document.addEventListener('click', function(e) {
        if (!coinSelector.contains(e.target) && !coinDropdown.contains(e.target)) {
            coinDropdown.classList.remove('show');
        }
    });

});

function loadAvailableCoins() {
    fetch('/api/coins?limit=1000&offset=0')
        .then(response => response.json())
        .then(data => {
            availableCoins = data.coins.map(coin => ({
                symbol: coin.symbol,
                name: coin.name,
                price: coin.price,
                rank: coin.market_cap_rank
            }));
            filterCoins('');
            renderSelectedCoin();
        })
        .catch(error => {
            console.error('Error loading coins:', error);
            showError('Failed to load cryptocurrency list');
        });
}

function filterCoins(searchTerm) {
    const dropdown = document.getElementById('coinDropdown');
    const term = searchTerm.toLowerCase();

    let filteredCoins = availableCoins.filter(coin => {
        if (term) {
            return coin.symbol.toLowerCase().includes(term) ||
                   coin.name.toLowerCase().includes(term);
        }
        return true;
    });

    filteredCoins = filteredCoins.slice(0, 50);

    if (filteredCoins.length === 0) {
        dropdown.innerHTML = '<div class="dropdown-item text-muted">No coins found</div>';
        return;
    }

    dropdown.innerHTML = filteredCoins.map(coin => `
        <a class="dropdown-item" href="#" onclick="event.preventDefault(); selectCoin('${coin.symbol}')">
            <div class="d-flex justify-content-between align-items-center">
                <div>
                    <strong>${coin.symbol}</strong>
                    <small class="text-muted ms-2">${coin.name}</small>
                </div>
                <small class="text-muted">$${coin.price.toFixed(2)}</small>
            </div>
        </a>
    `).join('');
}

function selectCoin(symbol) {
    symbol = symbol.toUpperCase();

    const coinInfo = availableCoins.find(c => c.symbol === symbol);
    if (!coinInfo && availableCoins.length > 0) {
        showError(`${symbol} not found in available cryptocurrencies`);
        return;
    }

    currentSymbol = symbol;
    selectedCoinData = coinInfo;

    document.getElementById('coinSelector').value = '';
    document.getElementById('coinDropdown').classList.remove('show');

    renderSelectedCoin();
    updateAnalyzeButton();
    saveState();
}

function removeCoin() {
    currentSymbol = '';
    selectedCoinData = null;
    renderSelectedCoin();
    updateAnalyzeButton();
    saveState();
    document.getElementById('analysisResults').style.display = 'none';
}

function renderSelectedCoin() {
    const container = document.getElementById('selectedCoin');

    if (!currentSymbol) {
        container.innerHTML = '<small class="text-muted">No coin selected</small>';
        return;
    }

    const coinInfo = selectedCoinData || { name: currentSymbol, price: 0 };

    container.innerHTML = `
        <span class="badge bg-primary" style="font-size: 0.95rem;">
            <strong>${currentSymbol}</strong> - ${coinInfo.name}
            <i class="bi bi-x-circle ms-2" onclick="removeCoin()" style="cursor: pointer;"></i>
        </span>
    `;
}

function updateAnalyzeButton() {
    const analyzeBtn = document.getElementById('analyzeBtn');
    if (currentSymbol) {
        analyzeBtn.disabled = false;
        analyzeBtn.innerHTML = `<i class="bi bi-graph-up-arrow"></i> Analyze ${currentSymbol}`;
    } else {
        analyzeBtn.disabled = true;
        analyzeBtn.innerHTML = '<i class="bi bi-graph-up-arrow"></i> Analyze';
    }
}

function analyzeSymbol() {
    if (!currentSymbol) {
        showError('Please select a cryptocurrency');
        return;
    }

    document.getElementById('loadingSpinner').style.display = 'block';
    document.getElementById('analysisResults').style.display = 'none';
    document.getElementById('errorAlert').style.display = 'none';

    fetch(`/api/technical-analysis/${currentSymbol}`)
        .then(response => response.json())
        .then(data => {
            document.getElementById('loadingSpinner').style.display = 'none';

            if (data.error) {
                showError(data.error);
                return;
            }

            displayResults(data);
        })
        .catch(error => {
            document.getElementById('loadingSpinner').style.display = 'none';
            showError('Error fetching analysis: ' + error.message);
        });
}

function displayResults(data) {
    // Market info
    document.getElementById('currentPrice').textContent = '$' + data.current_price.toFixed(2);
    document.getElementById('marketCap').textContent = formatLargeNumber(data.market_cap);
    document.getElementById('volume24h').textContent = formatLargeNumber(data.volume_24h);

    // Overall recommendation
    const recommendation = data.overall.recommendation;
    const recommendationEl = document.getElementById('overallRecommendation');

    let badgeClass = 'bg-warning';
    let barClass = 'bg-warning';
    if (recommendation.includes('BUY')) {
        badgeClass = 'bg-success';
        barClass = 'bg-success';
    } else if (recommendation.includes('SELL')) {
        badgeClass = 'bg-danger';
        barClass = 'bg-danger';
    }

    recommendationEl.innerHTML = `<span class="badge ${badgeClass}">${recommendation}</span>`;

    // Signal strength bar
    const strengthBar = document.getElementById('signalStrengthBar');
    const strengthText = document.getElementById('signalStrengthText');
    strengthBar.className = `progress-bar progress-bar-striped progress-bar-animated ${barClass}`;
    strengthBar.style.width = data.overall.strength + '%';
    strengthText.textContent = data.overall.strength + '% Confidence';

    document.getElementById('buyCount').textContent = data.overall.buy_count;
    document.getElementById('sellCount').textContent = data.overall.sell_count;
    document.getElementById('holdCount').textContent = data.overall.hold_count;

    // Render price chart
    renderPriceChart(data.chart_data, data.current_price, data.levels);

    // Support/Resistance levels
    renderLevels(data.levels);

    // Trading suggestions
    renderTradingSuggestions(data.trading);

    // Oscillators table
    const oscillatorsTable = document.getElementById('oscillatorsTable');
    oscillatorsTable.innerHTML = '';
    data.oscillators.forEach(osc => {
        const row = createSignalRow(osc);
        oscillatorsTable.appendChild(row);
    });

    // Moving averages table
    const maTable = document.getElementById('movingAveragesTable');
    maTable.innerHTML = '';
    data.moving_averages.forEach(ma => {
        const row = createSignalRow(ma);
        maTable.appendChild(row);
    });

    document.getElementById('analysisResults').style.display = 'block';

    // Scroll to results
    document.getElementById('analysisResults').scrollIntoView({ behavior: 'smooth', block: 'start' });
}

function renderPriceChart(chartData, currentPrice, levels) {
    const ctx = document.getElementById('priceChart').getContext('2d');

    if (priceChart) {
        priceChart.destroy();
    }

    const dates = chartData.map(d => d.date);
    const prices = chartData.map(d => d.price);

    // Create annotations for support/resistance
    const annotations = {};

    levels.resistance.forEach((level, i) => {
        annotations[`resistance${i}`] = {
            type: 'line',
            yMin: level,
            yMax: level,
            borderColor: 'rgba(220, 53, 69, 0.5)',
            borderWidth: 2,
            borderDash: [5, 5],
            label: {
                content: `R: $${level}`,
                enabled: true,
                position: 'end'
            }
        };
    });

    levels.support.forEach((level, i) => {
        annotations[`support${i}`] = {
            type: 'line',
            yMin: level,
            yMax: level,
            borderColor: 'rgba(40, 167, 69, 0.5)',
            borderWidth: 2,
            borderDash: [5, 5],
            label: {
                content: `S: $${level}`,
                enabled: true,
                position: 'end'
            }
        };
    });

    if (levels.pivot) {
        annotations['pivot'] = {
            type: 'line',
            yMin: levels.pivot,
            yMax: levels.pivot,
            borderColor: 'rgba(0, 123, 255, 0.5)',
            borderWidth: 2,
            label: {
                content: `Pivot: $${levels.pivot}`,
                enabled: true,
                position: 'center'
            }
        };
    }

    priceChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: dates,
            datasets: [{
                label: 'Price',
                data: prices,
                borderColor: 'rgb(75, 192, 192)',
                backgroundColor: 'rgba(75, 192, 192, 0.1)',
                tension: 0.1,
                fill: true,
                borderWidth: 2,
                pointRadius: 1,
                pointHoverRadius: 5
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: {
                mode: 'index',
                intersect: false,
            },
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            return 'Price: $' + context.parsed.y.toFixed(2);
                        }
                    }
                }
            },
            scales: {
                y: {
                    beginAtZero: false,
                    ticks: {
                        callback: function(value) {
                            return '$' + value.toFixed(2);
                        }
                    }
                },
                x: {
                    ticks: {
                        maxTicksLimit: 10
                    }
                }
            }
        }
    });
}

function renderLevels(levels) {
    const resistanceEl = document.getElementById('resistanceLevels');
    const supportEl = document.getElementById('supportLevels');
    const pivotEl = document.getElementById('pivotLevel');

    if (levels.resistance.length > 0) {
        resistanceEl.innerHTML = levels.resistance.map(r =>
            `<div class="badge bg-danger mb-1 me-1" style="font-size: 0.9rem;">$${r}</div>`
        ).join('');
    } else {
        resistanceEl.innerHTML = '<small class="text-muted">No resistance detected</small>';
    }

    if (levels.support.length > 0) {
        supportEl.innerHTML = levels.support.map(s =>
            `<div class="badge bg-success mb-1 me-1" style="font-size: 0.9rem;">$${s}</div>`
        ).join('');
    } else {
        supportEl.innerHTML = '<small class="text-muted">No support detected</small>';
    }

    pivotEl.innerHTML = `<div class="badge bg-primary" style="font-size: 0.9rem;">$${levels.pivot}</div>`;
}

function renderTradingSuggestions(trading) {
    document.getElementById('tradingAction').innerHTML =
        `<i class="bi bi-info-circle-fill"></i> <strong>${trading.action}</strong>`;

    document.getElementById('entryPrice').textContent =
        trading.entry_price ? '$' + trading.entry_price : '-';
    document.getElementById('stopLoss').textContent =
        trading.stop_loss ? '$' + trading.stop_loss : '-';
    document.getElementById('takeProfit').textContent =
        trading.take_profit ? '$' + trading.take_profit : '-';
    document.getElementById('riskReward').textContent =
        trading.risk_reward ? '1:' + trading.risk_reward : '-';
}

function createSignalRow(signal) {
    const row = document.createElement('tr');

    let badgeClass = 'bg-warning';
    if (signal.signal === 'BUY') badgeClass = 'bg-success';
    else if (signal.signal === 'SELL') badgeClass = 'bg-danger';

    row.innerHTML = `
        <td><strong>${signal.indicator}</strong></td>
        <td><code>${signal.value}</code></td>
        <td><span class="badge ${badgeClass}">${signal.signal}</span></td>
        <td><small>${signal.reason}</small></td>
    `;

    return row;
}

function formatLargeNumber(num) {
    if (num >= 1e9) {
        return '$' + (num / 1e9).toFixed(2) + 'B';
    } else if (num >= 1e6) {
        return '$' + (num / 1e6).toFixed(2) + 'M';
    } else if (num >= 1e3) {
        return '$' + (num / 1e3).toFixed(2) + 'K';
    }
    return '$' + num.toFixed(2);
}

function showError(message) {
    const errorAlert = document.getElementById('errorAlert');
    document.getElementById('errorMessage').textContent = message;
    errorAlert.style.display = 'block';

    setTimeout(() => {
        errorAlert.style.display = 'none';
    }, 7000);
}

// ==================== STATE PERSISTENCE ====================

function saveState() {
    const state = {
        symbol: currentSymbol,
        coinData: selectedCoinData
    };
    sessionStorage.setItem('technicalAnalysisState', JSON.stringify(state));
}

function restoreState() {
    const savedState = sessionStorage.getItem('technicalAnalysisState');
    if (savedState) {
        try {
            const state = JSON.parse(savedState);
            if (state.symbol) {
                currentSymbol = state.symbol;
                selectedCoinData = state.coinData;

                // Wait for coins to load, then restore UI
                setTimeout(() => {
                    renderSelectedCoin();
                    updateAnalyzeButton();
                    // Auto-analyze if there was a selected coin
                    if (currentSymbol) {
                        analyzeSymbol();
                    }
                }, 500);
            }
        } catch (e) {
            console.error('Error restoring state:', e);
        }
    }
}
