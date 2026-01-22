let priceChart = null;
let volumeChart = null;
let currentSymbols = [];
let currentPeriod = '3m';
let normalize = false;
let availableCoins = [];

// Color palette for different coins
const CHART_COLORS = [
    { border: 'rgb(75, 192, 192)', background: 'rgba(75, 192, 192, 0.1)' },  // Teal
    { border: 'rgb(255, 99, 132)', background: 'rgba(255, 99, 132, 0.1)' },  // Red
    { border: 'rgb(54, 162, 235)', background: 'rgba(54, 162, 235, 0.1)' },  // Blue
    { border: 'rgb(255, 206, 86)', background: 'rgba(255, 206, 86, 0.1)' },  // Yellow
    { border: 'rgb(153, 102, 255)', background: 'rgba(153, 102, 255, 0.1)' }, // Purple
    { border: 'rgb(255, 159, 64)', background: 'rgba(255, 159, 64, 0.1)' },  // Orange
    { border: 'rgb(46, 204, 113)', background: 'rgba(46, 204, 113, 0.1)' },  // Green
    { border: 'rgb(231, 76, 60)', background: 'rgba(231, 76, 60, 0.1)' }     // Dark Red
];

document.addEventListener('DOMContentLoaded', function() {
    // Load available coins
    loadAvailableCoins();

    // Restore state from sessionStorage
    restoreState();

    // Check if symbol is in URL params (overrides saved state)
    const urlParams = new URLSearchParams(window.location.search);
    const symbol = urlParams.get('symbol');
    if (symbol) {
        // Clear previous state and use URL param
        currentSymbols = [];
        setTimeout(() => {
            addCoin(symbol.toUpperCase());
            loadChart();
        }, 500);
    } else if (currentSymbols.length === 0) {
        document.getElementById('noDataMessage').style.display = 'block';
        document.getElementById('chartContainer').style.display = 'none';
        document.getElementById('priceChartCard').style.display = 'none';
        document.getElementById('volumeStatsRow').style.display = 'none';
    } else {
        // Restore chart with saved state
        setTimeout(() => {
            if (currentSymbols.length > 0) {
                loadChart();
            }
        }, 500);
    }

    // Setup event listeners
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
                addCoin(searchTerm);
                this.value = '';
                coinDropdown.classList.remove('show');
            }
        }
    });

    // Close dropdown when clicking outside
    document.addEventListener('click', function(e) {
        if (!coinSelector.contains(e.target) && !coinDropdown.contains(e.target)) {
            coinDropdown.classList.remove('show');
        }
    });

    document.getElementById('loadChartBtn').addEventListener('click', loadChart);

    document.querySelectorAll('input[name="period"]').forEach(radio => {
        radio.addEventListener('change', function() {
            currentPeriod = this.value;
            saveState();
            if (currentSymbols.length > 0) {
                loadChart();
            }
        });
    });

    document.getElementById('normalizeCheckbox').addEventListener('change', function() {
        normalize = this.checked;
        saveState();
        if (currentSymbols.length > 0) {
            loadChart();
        }
    });
});

// ==================== STATE PERSISTENCE ====================

function saveState() {
    const state = {
        symbols: currentSymbols,
        period: currentPeriod,
        normalize: normalize
    };
    sessionStorage.setItem('chartsState', JSON.stringify(state));
}

function restoreState() {
    const savedState = sessionStorage.getItem('chartsState');
    if (savedState) {
        try {
            const state = JSON.parse(savedState);
            currentSymbols = state.symbols || [];
            currentPeriod = state.period || '3m';
            normalize = state.normalize || false;

            // Restore UI elements
            if (currentSymbols.length > 0) {
                renderSelectedCoins();
                updateLoadButton();
            }

            // Restore period radio button
            const periodRadio = document.querySelector(`input[name="period"][value="${currentPeriod}"]`);
            if (periodRadio) {
                periodRadio.checked = true;
            }

            // Restore normalize checkbox
            const normalizeCheckbox = document.getElementById('normalizeCheckbox');
            if (normalizeCheckbox) {
                normalizeCheckbox.checked = normalize;
            }
        } catch (e) {
            console.error('Error restoring state:', e);
        }
    }
}

function loadAvailableCoins() {
    // Fetch all coins from API
    fetch('/api/coins?limit=1000&offset=0')
        .then(response => response.json())
        .then(data => {
            availableCoins = data.coins.map(coin => ({
                symbol: coin.symbol,
                name: coin.name,
                rank: coin.market_cap_rank
            }));
            filterCoins(''); // Initialize dropdown
        })
        .catch(error => {
            console.error('Error loading coins:', error);
        });
}

function filterCoins(searchTerm) {
    const dropdown = document.getElementById('coinDropdown');
    const term = searchTerm.toLowerCase();

    let filteredCoins = availableCoins.filter(coin => {
        // Exclude already selected coins
        if (currentSymbols.includes(coin.symbol)) {
            return false;
        }
        // Filter by search term
        if (term) {
            return coin.symbol.toLowerCase().includes(term) ||
                   coin.name.toLowerCase().includes(term);
        }
        return true;
    });

    // Limit to top 50 results for better UX
    filteredCoins = filteredCoins.slice(0, 50);

    if (filteredCoins.length === 0) {
        dropdown.innerHTML = '<div class="dropdown-item text-muted">No coins found</div>';
        return;
    }

    dropdown.innerHTML = filteredCoins.map(coin => `
        <a class="dropdown-item" href="#" onclick="event.preventDefault(); addCoin('${coin.symbol}')">
            <div class="d-flex justify-content-between align-items-center">
                <div>
                    <strong>${coin.symbol}</strong>
                    <small class="text-muted ms-2">${coin.name}</small>
                </div>
                <small class="text-muted">#${coin.rank}</small>
            </div>
        </a>
    `).join('');
}

function addCoin(symbol) {
    symbol = symbol.toUpperCase();

    // Check if already selected
    if (currentSymbols.includes(symbol)) {
        return;
    }

    // Check if valid coin
    const coinExists = availableCoins.find(c => c.symbol === symbol);
    if (!coinExists && availableCoins.length > 0) {
        showError(`${symbol} not found in available cryptocurrencies`);
        return;
    }

    currentSymbols.push(symbol);
    renderSelectedCoins();
    updateLoadButton();
    saveState();

    // Clear search input
    document.getElementById('coinSelector').value = '';
    document.getElementById('coinDropdown').classList.remove('show');

    // Refresh dropdown to remove selected coin
    filterCoins('');
}

function removeCoin(symbol) {
    currentSymbols = currentSymbols.filter(s => s !== symbol);
    renderSelectedCoins();
    updateLoadButton();
    saveState();

    // Refresh dropdown
    const searchTerm = document.getElementById('coinSelector').value.trim();
    filterCoins(searchTerm);

    if (currentSymbols.length === 0) {
        document.getElementById('noDataMessage').style.display = 'block';
        document.getElementById('chartContainer').style.display = 'none';
        document.getElementById('priceChartCard').style.display = 'none';
        document.getElementById('volumeStatsRow').style.display = 'none';
        if (priceChart) priceChart.destroy();
        if (volumeChart) volumeChart.destroy();
        document.getElementById('statsContainer').innerHTML = '<p class="text-muted">Load a chart to see statistics</p>';
    } else {
        loadChart();
    }
}

function renderSelectedCoins() {
    const container = document.getElementById('selectedCoins');

    if (currentSymbols.length === 0) {
        container.innerHTML = '<small class="text-muted">No coins selected. Search and select coins to compare.</small>';
        return;
    }

    const html = currentSymbols.map((symbol, index) => {
        const color = CHART_COLORS[index % CHART_COLORS.length];
        const coinInfo = availableCoins.find(c => c.symbol === symbol);
        const name = coinInfo ? coinInfo.name : symbol;

        return `
            <span class="badge me-2 mb-2" style="background-color: ${color.border}; font-size: 0.9rem;">
                <strong>${symbol}</strong>
                <small class="ms-1">${name}</small>
                <i class="bi bi-x-circle ms-2" onclick="removeCoin('${symbol}')" style="cursor: pointer;"></i>
            </span>
        `;
    }).join('');

    container.innerHTML = `<div class="mb-2"><small class="text-muted">Selected coins:</small><br>${html}</div>`;
}

function updateLoadButton() {
    const loadBtn = document.getElementById('loadChartBtn');
    if (currentSymbols.length > 0) {
        loadBtn.disabled = false;
        loadBtn.innerHTML = `<i class="bi bi-graph-up-arrow"></i> Load ${currentSymbols.length} Chart${currentSymbols.length > 1 ? 's' : ''}`;
    } else {
        loadBtn.disabled = true;
        loadBtn.innerHTML = '<i class="bi bi-graph-up-arrow"></i> Load Charts';
    }
}

function loadChart() {
    if (currentSymbols.length === 0) {
        showError('Please select at least one cryptocurrency');
        return;
    }

    currentPeriod = document.querySelector('input[name="period"]:checked').value;
    normalize = document.getElementById('normalizeCheckbox').checked;

    document.getElementById('noDataMessage').style.display = 'none';
    document.getElementById('chartContainer').style.display = 'block';

    // Fetch data for all symbols
    Promise.all(currentSymbols.map(symbol => fetchOHLCVData(symbol)))
        .then(results => {
            const validResults = results.filter(r => r.data && r.data.length > 0);

            if (validResults.length === 0) {
                showError(`No data found for any of the symbols: ${currentSymbols.join(', ')}`);
                document.getElementById('noDataMessage').style.display = 'block';
                document.getElementById('chartContainer').style.display = 'none';
                document.getElementById('priceChartCard').style.display = 'none';
                document.getElementById('volumeStatsRow').style.display = 'none';
                return;
            }

            // Remove symbols with no data
            const invalidSymbols = results.filter(r => !r.data || r.data.length === 0).map(r => r.symbol);
            if (invalidSymbols.length > 0) {
                invalidSymbols.forEach(symbol => {
                    currentSymbols = currentSymbols.filter(s => s !== symbol);
                });
                renderSelectedCoins();
                updateLoadButton();
                showError(`No data found for: ${invalidSymbols.join(', ')}`);
            }

            // Show chart sections
            document.getElementById('priceChartCard').style.display = 'block';
            document.getElementById('volumeStatsRow').style.display = 'flex';

            renderPriceChart(validResults);
            renderVolumeChart(validResults);
            renderStatistics(validResults);
        })
        .catch(error => {
            console.error('Error loading charts:', error);
            showError('Failed to load chart data');
        });
}

function fetchOHLCVData(symbol) {
    return fetch(`/api/ohlcv/${symbol}?period=${currentPeriod}`)
        .then(response => response.json())
        .then(data => ({ symbol, data }))
        .catch(error => {
            console.error(`Error fetching ${symbol}:`, error);
            return { symbol, data: [] };
        });
}

function normalizeData(data) {
    if (data.length === 0) return [];

    const firstPrice = data[0].close;
    return data.map(d => ({
        ...d,
        close: ((d.close - firstPrice) / firstPrice) * 100,
        open: ((d.open - firstPrice) / firstPrice) * 100,
        high: ((d.high - firstPrice) / firstPrice) * 100,
        low: ((d.low - firstPrice) / firstPrice) * 100
    }));
}

function renderPriceChart(results) {
    const ctx = document.getElementById('priceChart').getContext('2d');

    if (priceChart) {
        priceChart.destroy();
    }

    // Find the latest start date across all coins (most recent date any coin started)
    const latestStartDate = new Date(Math.max(...results.map(r => new Date(r.data[0].date))));

    // Filter all datasets to start from the latest start date
    const filteredResults = results.map(result => ({
        ...result,
        data: result.data.filter(d => new Date(d.date) >= latestStartDate)
    }));

    // Use the dates from the first filtered dataset
    const labels = filteredResults[0].data.map(d => d.date);

    // Create datasets for each symbol
    const datasets = filteredResults.map((result, index) => {
        const color = CHART_COLORS[index % CHART_COLORS.length];
        const data = normalize ? normalizeData(result.data) : result.data;
        const prices = data.map(d => d.close);

        return {
            label: `${result.symbol} ${normalize ? '(% Change)' : '(USD)'}`,
            data: prices,
            borderColor: color.border,
            backgroundColor: color.background,
            tension: 0.1,
            fill: results.length === 1, // Only fill if single coin
            borderWidth: 2,
            pointRadius: 0,
            pointHoverRadius: 5
        };
    });

    priceChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: datasets
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
                    display: true,
                    position: 'top'
                },
                tooltip: {
                    mode: 'index',
                    intersect: false,
                    callbacks: {
                        label: function(context) {
                            let label = context.dataset.label || '';
                            if (label) {
                                label += ': ';
                            }
                            if (normalize) {
                                label += context.parsed.y.toFixed(2) + '%';
                            } else {
                                label += '$' + context.parsed.y.toFixed(8);
                            }
                            return label;
                        }
                    }
                }
            },
            scales: {
                y: {
                    beginAtZero: false,
                    ticks: {
                        callback: function(value) {
                            if (normalize) {
                                return value.toFixed(1) + '%';
                            }
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

function renderVolumeChart(results) {
    const ctx = document.getElementById('volumeChart').getContext('2d');

    if (volumeChart) {
        volumeChart.destroy();
    }

    // Find the latest start date across all coins
    const latestStartDate = new Date(Math.max(...results.map(r => new Date(r.data[0].date))));

    // Filter all datasets to start from the latest start date
    const filteredResults = results.map(result => ({
        ...result,
        data: result.data.filter(d => new Date(d.date) >= latestStartDate)
    }));

    const labels = filteredResults[0].data.map(d => d.date);

    // Create stacked bar datasets for volume
    const datasets = filteredResults.map((result, index) => {
        const color = CHART_COLORS[index % CHART_COLORS.length];
        const volumes = result.data.map(d => d.volume);

        return {
            label: `${result.symbol} Volume`,
            data: volumes,
            backgroundColor: color.background,
            borderColor: color.border,
            borderWidth: 1
        };
    });

    volumeChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: datasets
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: {
                    display: true,
                    position: 'top'
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    stacked: false,
                    ticks: {
                        callback: function(value) {
                            return formatLargeNumber(value);
                        }
                    }
                },
                x: {
                    stacked: false,
                    ticks: {
                        maxTicksLimit: 10
                    }
                }
            }
        }
    });
}

function renderStatistics(results) {
    let statsHtml = '<div class="row">';

    results.forEach((result, index) => {
        const prices = result.data.map(d => d.close);
        const volumes = result.data.map(d => d.volume);

        const currentPrice = prices[prices.length - 1];
        const startPrice = prices[0];
        const priceChange = currentPrice - startPrice;
        const priceChangePercent = (priceChange / startPrice) * 100;

        const highPrice = Math.max(...result.data.map(d => d.high));
        const lowPrice = Math.min(...result.data.map(d => d.low));
        const avgVolume = volumes.reduce((a, b) => a + b, 0) / volumes.length;

        const color = CHART_COLORS[index % CHART_COLORS.length];
        const coinInfo = availableCoins.find(c => c.symbol === result.symbol);
        const coinName = coinInfo ? coinInfo.name : result.symbol;

        statsHtml += `
            <div class="col-md-${results.length === 1 ? 12 : 6} mb-3">
                <div class="card" style="border-left: 4px solid ${color.border};">
                    <div class="card-body">
                        <h6 class="card-title" style="color: ${color.border};">
                            <strong>${result.symbol}</strong> <small class="text-muted">${coinName}</small>
                        </h6>
                        <table class="table table-sm">
                            <tr>
                                <th>Current Price:</th>
                                <td>$${currentPrice.toFixed(8)}</td>
                            </tr>
                            <tr>
                                <th>Period Change:</th>
                                <td class="${priceChange >= 0 ? 'text-success' : 'text-danger'}">
                                    ${priceChange >= 0 ? '+' : ''}$${priceChange.toFixed(8)}
                                    (${priceChangePercent >= 0 ? '+' : ''}${priceChangePercent.toFixed(2)}%)
                                </td>
                            </tr>
                            <tr>
                                <th>High:</th>
                                <td>$${highPrice.toFixed(8)}</td>
                            </tr>
                            <tr>
                                <th>Low:</th>
                                <td>$${lowPrice.toFixed(8)}</td>
                            </tr>
                            <tr>
                                <th>Avg Volume:</th>
                                <td>${formatLargeNumber(avgVolume)}</td>
                            </tr>
                            <tr>
                                <th>Data Points:</th>
                                <td>${result.data.length} days</td>
                            </tr>
                        </table>
                    </div>
                </div>
            </div>
        `;
    });

    statsHtml += '</div>';

    document.getElementById('statsContainer').innerHTML = statsHtml;
}

function formatLargeNumber(num) {
    if (num >= 1e9) {
        return (num / 1e9).toFixed(2) + 'B';
    } else if (num >= 1e6) {
        return (num / 1e6).toFixed(2) + 'M';
    } else if (num >= 1e3) {
        return (num / 1e3).toFixed(2) + 'K';
    }
    return num.toFixed(2);
}

function showError(message) {
    const alertDiv = document.createElement('div');
    alertDiv.className = 'alert alert-danger alert-dismissible fade show';
    alertDiv.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
    `;
    document.querySelector('main').insertBefore(alertDiv, document.querySelector('main').firstChild);

    setTimeout(() => {
        alertDiv.remove();
    }, 5000);
}
