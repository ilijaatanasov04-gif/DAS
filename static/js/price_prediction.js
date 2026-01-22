let availableCoins = [];
let currentSymbol = '';
let selectedCoinData = null;
let predictionChart = null;
let validationChart = null;

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
    updateButtons();
    saveState();
}

function removeCoin() {
    currentSymbol = '';
    selectedCoinData = null;
    renderSelectedCoin();
    updateButtons();
    saveState();
    document.getElementById('predictionResults').style.display = 'none';
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

function updateButtons() {
    const predictBtn = document.getElementById('predictBtn');
    const trainBtn = document.getElementById('trainBtn');

    if (currentSymbol) {
        predictBtn.disabled = false;
        if (trainBtn) trainBtn.disabled = false;
    } else {
        predictBtn.disabled = true;
        if (trainBtn) trainBtn.disabled = true;
    }
}

function predictPrice() {
    if (!currentSymbol) {
        showError('Please select a cryptocurrency');
        return;
    }

    const daysAhead = document.getElementById('daysAhead').value;
    const lookback = document.getElementById('lookbackDays').value;

    document.getElementById('loadingSpinner').style.display = 'block';
    document.getElementById('predictionResults').style.display = 'none';
    document.getElementById('errorAlert').style.display = 'none';

    fetch(`/api/predict-price/${currentSymbol}?days=${daysAhead}&lookback=${lookback}`)
        .then(response => response.json())
        .then(data => {
            document.getElementById('loadingSpinner').style.display = 'none';

            if (!data.success) {
                showError(data.error || 'Failed to generate predictions');
                return;
            }

            displayPredictions(data);
        })
        .catch(error => {
            document.getElementById('loadingSpinner').style.display = 'none';
            showError('Error fetching predictions: ' + error.message);
        });
}

function displayPredictions(data) {
    // Display model metrics
    document.getElementById('rmse').textContent = '$' + data.evaluation.rmse;
    document.getElementById('mape').textContent = data.evaluation.mape.toFixed(2) + '%';
    document.getElementById('r2score').textContent = data.evaluation.r2_score;

    // Render prediction chart
    renderPredictionChart(data);

    // Render validation chart
    renderValidationChart(data);

    // Populate predictions table
    populatePredictionsTable(data);

    document.getElementById('predictionResults').style.display = 'block';
    document.getElementById('predictionResults').scrollIntoView({ behavior: 'smooth', block: 'start' });
}

function renderPredictionChart(data) {
    const ctx = document.getElementById('predictionChart').getContext('2d');

    if (predictionChart) {
        predictionChart.destroy();
    }

    const currentPrice = data.current_price;
    const futureDates = data.predictions.dates;
    const futurePrices = data.predictions.prices;

    // Add current price as starting point
    const allDates = ['Today', ...futureDates];
    const allPrices = [currentPrice, ...futurePrices];

    predictionChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: allDates,
            datasets: [
                {
                    label: 'Predicted Price',
                    data: allPrices,
                    borderColor: 'rgb(54, 162, 235)',
                    backgroundColor: 'rgba(54, 162, 235, 0.1)',
                    fill: true,
                    tension: 0.4,
                    borderWidth: 3,
                    pointRadius: 5,
                    pointHoverRadius: 7
                },
                {
                    label: 'Current Price',
                    data: [currentPrice, null],
                    borderColor: 'rgb(75, 192, 192)',
                    backgroundColor: 'rgba(75, 192, 192, 0.2)',
                    pointRadius: 8,
                    pointHoverRadius: 10,
                    borderWidth: 2
                }
            ]
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
                    callbacks: {
                        label: function(context) {
                            return context.dataset.label + ': $' + context.parsed.y.toFixed(2);
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
                }
            }
        }
    });
}

function renderValidationChart(data) {
    const ctx = document.getElementById('validationChart').getContext('2d');

    if (validationChart) {
        validationChart.destroy();
    }

    const actualPrices = data.evaluation.actual_prices;
    const predictedPrices = data.evaluation.predicted_prices;
    const labels = actualPrices.map((_, i) => `Day ${i + 1}`);

    validationChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [
                {
                    label: 'Actual Price',
                    data: actualPrices,
                    borderColor: 'rgb(75, 192, 192)',
                    backgroundColor: 'rgba(75, 192, 192, 0.1)',
                    tension: 0.1,
                    borderWidth: 2,
                    pointRadius: 3
                },
                {
                    label: 'Predicted Price',
                    data: predictedPrices,
                    borderColor: 'rgb(255, 99, 132)',
                    backgroundColor: 'rgba(255, 99, 132, 0.1)',
                    tension: 0.1,
                    borderWidth: 2,
                    pointRadius: 3,
                    borderDash: [5, 5]
                }
            ]
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
                    callbacks: {
                        label: function(context) {
                            return context.dataset.label + ': $' + context.parsed.y.toFixed(2);
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
                }
            }
        }
    });
}

function populatePredictionsTable(data) {
    const tbody = document.getElementById('predictionsTable');
    const currentPrice = data.current_price;
    const predictions = data.predictions;

    tbody.innerHTML = '';

    predictions.dates.forEach((date, index) => {
        const price = predictions.prices[index];
        const change = price - currentPrice;
        const changePercent = (change / currentPrice) * 100;
        const changeClass = change >= 0 ? 'text-success' : 'text-danger';
        const changeIcon = change >= 0 ? 'bi-arrow-up' : 'bi-arrow-down';

        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${date}</td>
            <td><strong>$${price.toFixed(2)}</strong></td>
            <td class="${changeClass}">
                <i class="bi ${changeIcon}"></i>
                ${change >= 0 ? '+' : ''}$${change.toFixed(2)}
            </td>
            <td class="${changeClass}">
                <i class="bi ${changeIcon}"></i>
                ${change >= 0 ? '+' : ''}${changePercent.toFixed(2)}%
            </td>
        `;
        tbody.appendChild(row);
    });
}

function trainNewModel() {
    if (!currentSymbol) {
        showError('Please select a cryptocurrency');
        return;
    }

    if (!confirm(`Train a new LSTM model for ${currentSymbol}? This may take several minutes.`)) {
        return;
    }

    const lookback = document.getElementById('lookbackDays').value;

    document.getElementById('loadingSpinner').style.display = 'block';
    document.getElementById('errorAlert').style.display = 'none';

    fetch(`/api/train-model/${currentSymbol}`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            lookback: parseInt(lookback),
            epochs: 50
        })
    })
        .then(response => response.json())
        .then(data => {
            document.getElementById('loadingSpinner').style.display = 'none';

            if (!data.success) {
                showError(data.error || 'Failed to train model');
                return;
            }

            alert(`Model trained successfully for ${currentSymbol}!\n\nRMSE: ${data.evaluation.rmse}\nMAPE: ${data.evaluation.mape}%\nR²: ${data.evaluation.r2_score}`);

            // Auto-predict after training
            predictPrice();
        })
        .catch(error => {
            document.getElementById('loadingSpinner').style.display = 'none';
            showError('Error training model: ' + error.message);
        });
}

function showError(message) {
    const errorAlert = document.getElementById('errorAlert');
    document.getElementById('errorMessage').textContent = message;
    errorAlert.style.display = 'block';

    setTimeout(() => {
        errorAlert.style.display = 'none';
    }, 10000);
}

// ==================== STATE PERSISTENCE ====================

function saveState() {
    const state = {
        symbol: currentSymbol,
        coinData: selectedCoinData,
        daysAhead: document.getElementById('daysAhead').value,
        lookbackDays: document.getElementById('lookbackDays').value
    };
    sessionStorage.setItem('pricePredictionState', JSON.stringify(state));
}

function restoreState() {
    const savedState = sessionStorage.getItem('pricePredictionState');
    if (savedState) {
        try {
            const state = JSON.parse(savedState);

            // Restore dropdown values
            if (state.daysAhead) {
                document.getElementById('daysAhead').value = state.daysAhead;
            }
            if (state.lookbackDays) {
                document.getElementById('lookbackDays').value = state.lookbackDays;
            }

            // Restore selected coin
            if (state.symbol) {
                currentSymbol = state.symbol;
                selectedCoinData = state.coinData;

                // Wait for coins to load, then restore UI
                setTimeout(() => {
                    renderSelectedCoin();
                    updateButtons();
                    // Auto-predict if there was a selected coin
                    if (currentSymbol) {
                        predictPrice();
                    }
                }, 500);
            }
        } catch (e) {
            console.error('Error restoring state:', e);
        }
    }
}
