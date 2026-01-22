let availableCoins = [];
let selectedCoin = null;
let portfolioItemToDelete = null;

document.addEventListener('DOMContentLoaded', function() {
    loadPortfolio();
    loadAvailableCoins();

    const coinSelector = document.getElementById('portfolioSymbol');
    const coinDropdown = document.getElementById('portfolioCoinDropdown');

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
        selectedCoin = null;
        document.getElementById('selectedPortfolioSymbol').value = '';
        document.getElementById('selectedPortfolioCoin').innerHTML = '';
    });

    document.addEventListener('click', function(e) {
        if (!coinSelector.contains(e.target) && !coinDropdown.contains(e.target)) {
            coinDropdown.classList.remove('show');
        }
    });

    document.getElementById('addPortfolioBtn').addEventListener('click', addPortfolioPosition);

    const confirmDeleteBtn = document.getElementById('confirmDeletePortfolioBtn');
    if (confirmDeleteBtn) {
        confirmDeleteBtn.addEventListener('click', function() {
            if (portfolioItemToDelete !== null) {
                fetch(`/api/portfolio/${portfolioItemToDelete}`, {
                    method: 'DELETE'
                })
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        loadPortfolio();
                        const deleteModal = bootstrap.Modal.getInstance(document.getElementById('deletePortfolioModal'));
                        deleteModal.hide();
                        portfolioItemToDelete = null;
                    }
                })
                .catch(error => {
                    console.error('Error:', error);
                });
            }
        });
    }

    setInterval(loadPortfolio, 30000);
});

function loadAvailableCoins() {
    fetch('/api/coins?limit=1000&offset=0')
        .then(response => response.json())
        .then(data => {
            availableCoins = data.coins.map(coin => ({
                symbol: coin.symbol,
                name: coin.name,
                rank: coin.market_cap_rank,
                price: coin.price
            }));
            filterCoins('');
        })
        .catch(error => {
            console.error('Error loading coins:', error);
        });
}

function filterCoins(searchTerm) {
    const dropdown = document.getElementById('portfolioCoinDropdown');
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
        <a class="dropdown-item" href="#" onclick="event.preventDefault(); selectCoin('${coin.symbol}', '${coin.name}', ${coin.price})">
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

function selectCoin(symbol, name, price) {
    selectedCoin = { symbol, name, price };
    document.getElementById('selectedPortfolioSymbol').value = symbol;
    document.getElementById('portfolioSymbol').value = '';
    document.getElementById('selectedPortfolioCoin').innerHTML = `
        <span class="badge bg-primary" style="font-size: 0.9rem;">
            <strong>${symbol}</strong> <small>${name}</small>
            <i class="bi bi-x-circle ms-2" onclick="clearCoinSelection()" style="cursor: pointer;"></i>
        </span>
    `;
    document.getElementById('portfolioCoinDropdown').classList.remove('show');

    // Auto-fill current price as purchase price
    document.getElementById('portfolioPurchasePrice').value = price.toFixed(8);
}

function clearCoinSelection() {
    selectedCoin = null;
    document.getElementById('selectedPortfolioSymbol').value = '';
    document.getElementById('selectedPortfolioCoin').innerHTML = '';
    document.getElementById('portfolioSymbol').value = '';
    document.getElementById('portfolioPurchasePrice').value = '';
}

function loadPortfolio() {
    fetch('/api/portfolio')
        .then(response => response.json())
        .then(data => {
            renderPortfolio(data);
        })
        .catch(error => {
            console.error('Error loading portfolio:', error);
        });
}

function renderPortfolio(data) {
    const tbody = document.getElementById('portfolioBody');
    const emptyMessage = document.getElementById('emptyPortfolio');
    const table = document.getElementById('portfolioTable');
    const items = data.items || [];
    const summary = data.summary || {};

    // Update summary cards
    document.getElementById('totalPurchaseValue').textContent = '$' + formatLargeNumber(summary.total_purchase_value || 0);
    document.getElementById('totalCurrentValue').textContent = '$' + formatLargeNumber(summary.total_current_value || 0);

    const totalProfitLoss = summary.total_profit_loss || 0;
    const totalProfitLossPercentage = summary.total_profit_loss_percentage || 0;

    document.getElementById('totalProfitLoss').textContent = (totalProfitLoss >= 0 ? '+$' : '-$') + formatLargeNumber(Math.abs(totalProfitLoss));
    document.getElementById('totalProfitLoss').className = 'mb-0 ' + (totalProfitLoss >= 0 ? 'text-success' : 'text-danger');

    document.getElementById('totalProfitLossPercentage').textContent = (totalProfitLossPercentage >= 0 ? '+' : '') + totalProfitLossPercentage.toFixed(2) + '%';
    document.getElementById('totalProfitLossPercentage').className = 'mb-0 ' + (totalProfitLossPercentage >= 0 ? 'text-success' : 'text-danger');

    if (items.length === 0) {
        emptyMessage.style.display = 'block';
        table.style.display = 'none';
        return;
    }

    emptyMessage.style.display = 'none';
    table.style.display = 'block';

    tbody.innerHTML = items.map(item => {
        const profitLossClass = item.profit_loss >= 0 ? 'text-success' : 'text-danger';
        const profitLossSign = item.profit_loss >= 0 ? '+$' : '-$';
        const percentageSign = item.profit_loss >= 0 ? '+' : '';

        return `
            <tr>
                <td>${item.market_cap_rank || '-'}</td>
                <td><strong>${item.symbol}</strong></td>
                <td>${item.name}</td>
                <td>${formatNumber(item.amount)}</td>
                <td>$${formatNumber(item.purchase_price)}</td>
                <td>$${formatNumber(item.current_price)}</td>
                <td>$${formatLargeNumber(item.purchase_value)}</td>
                <td>$${formatLargeNumber(item.current_value)}</td>
                <td class="${profitLossClass}"><strong>${profitLossSign}${formatLargeNumber(Math.abs(item.profit_loss))}</strong></td>
                <td class="${profitLossClass}"><strong>${percentageSign}${item.profit_loss_percentage.toFixed(2)}%</strong></td>
                <td>
                    <button class="btn btn-sm btn-outline-primary" onclick="viewChart('${item.symbol}')">
                        <i class="bi bi-graph-up"></i>
                    </button>
                    <button class="btn btn-sm btn-outline-danger" onclick="deletePortfolioItem(${item.id})">
                        <i class="bi bi-trash"></i>
                    </button>
                </td>
            </tr>
        `;
    }).join('');
}

function addPortfolioPosition() {
    const symbol = document.getElementById('selectedPortfolioSymbol').value;
    const amount = parseFloat(document.getElementById('portfolioAmount').value);
    const purchasePrice = parseFloat(document.getElementById('portfolioPurchasePrice').value);

    if (!symbol || !amount || !purchasePrice) {
        return;
    }

    if (amount <= 0 || purchasePrice <= 0) {
        return;
    }

    const coinData = availableCoins.find(c => c.symbol === symbol);
    const name = coinData ? coinData.name : symbol;

    fetch('/api/portfolio', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({
            symbol: symbol,
            name: name,
            amount: amount,
            purchase_price: purchasePrice
        })
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            loadPortfolio();

            const modal = bootstrap.Modal.getInstance(document.getElementById('addPortfolioModal'));
            modal.hide();
            document.getElementById('portfolioForm').reset();
            clearCoinSelection();
        }
    })
    .catch(error => {
        console.error('Error:', error);
    });
}

function deletePortfolioItem(id) {
    portfolioItemToDelete = id;
    const deleteModal = new bootstrap.Modal(document.getElementById('deletePortfolioModal'));
    deleteModal.show();
}

function viewChart(symbol) {
    window.location.href = `/charts?symbol=${symbol}`;
}

function formatNumber(num) {
    if (num >= 1) {
        return num.toFixed(2);
    } else if (num >= 0.01) {
        return num.toFixed(4);
    } else {
        return num.toFixed(8);
    }
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
