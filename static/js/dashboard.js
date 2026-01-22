let currentPage = 0;
const limit = 50;
let currentSearch = '';
let currentSort = 'market_cap_rank';
let currentOrder = 'asc';
let watchlistSymbols = new Set();

document.addEventListener('DOMContentLoaded', function() {
    if (window.isAuthenticated) {
        loadWatchlist();
    }
    loadCoins();

    document.getElementById('searchInput').addEventListener('input', debounce(function(e) {
        currentSearch = e.target.value;
        currentPage = 0;
        loadCoins();
    }, 500));

    document.getElementById('sortSelect').addEventListener('change', function(e) {
        currentSort = e.target.value;
        currentPage = 0;
        loadCoins();
    });

    document.getElementById('orderSelect').addEventListener('change', function(e) {
        currentOrder = e.target.value;
        currentPage = 0;
        loadCoins();
    });

    document.getElementById('updateDataBtn').addEventListener('click', updateData);
});

function loadWatchlist() {
    fetch('/api/watchlist')
        .then(response => response.json())
        .then(data => {
            watchlistSymbols = new Set(data.map(item => item.symbol));
            // Re-render coins if they're already loaded
            if (document.getElementById('coinsTableBody').children.length > 0) {
                loadCoins();
            }
        })
        .catch(error => {
            console.error('Error loading watchlist:', error);
        });
}

function loadCoins() {
    const offset = currentPage * limit;
    const url = `/api/coins?search=${currentSearch}&sort=${currentSort}&order=${currentOrder}&limit=${limit}&offset=${offset}`;

    fetch(url)
        .then(response => response.json())
        .then(data => {
            renderCoins(data.coins);
            renderPagination(data.total);
        })
        .catch(error => {
            console.error('Error loading coins:', error);
            showError('Failed to load cryptocurrency data');
        });
}

function renderCoins(coins) {
    const tbody = document.getElementById('coinsTableBody');

    if (coins.length === 0) {
        tbody.innerHTML = '<tr><td colspan="8" class="text-center">No cryptocurrencies found</td></tr>';
        return;
    }

    tbody.innerHTML = coins.map(coin => {
        const isInWatchlist = watchlistSymbols.has(coin.symbol);
        const watchlistBtnClass = isInWatchlist ? 'btn-warning' : 'btn-outline-warning';
        const starIcon = isInWatchlist ? 'bi-star-fill' : 'bi-star';

        // Only show watchlist button if user is authenticated
        const watchlistButton = window.isAuthenticated ?
            `<button class="btn btn-sm ${watchlistBtnClass}" onclick="addToWatchlist('${coin.symbol}', '${coin.name}')" title="Add to Watchlist">
                <i class="bi ${starIcon}"></i>
            </button>` :
            `<button class="btn btn-sm btn-outline-secondary" onclick="promptLogin()" title="Login to add to watchlist">
                <i class="bi bi-star"></i>
            </button>`;

        return `
            <tr>
                <td>${coin.market_cap_rank}</td>
                <td><strong>${coin.symbol}</strong></td>
                <td>${coin.name}</td>
                <td>$${formatNumber(coin.price)}</td>
                <td>$${formatLargeNumber(coin.market_cap)}</td>
                <td>$${formatLargeNumber(coin.volume_24h)}</td>
                <td>${(coin.liquidity_score * 100).toFixed(3)}%</td>
                <td>
                    <button class="btn btn-sm btn-outline-primary" onclick="viewChart('${coin.symbol}')">
                        <i class="bi bi-graph-up"></i>
                    </button>
                    ${watchlistButton}
                </td>
            </tr>
        `;
    }).join('');
}

function renderPagination(total) {
    const totalPages = Math.ceil(total / limit);
    const pagination = document.getElementById('pagination');

    if (totalPages <= 1) {
        pagination.innerHTML = '';
        return;
    }

    let html = '';

    // Previous button
    html += `
        <li class="page-item ${currentPage === 0 ? 'disabled' : ''}">
            <a class="page-link" href="#" onclick="changePage(${currentPage - 1}); return false;">Previous</a>
        </li>
    `;

    // Page numbers (show max 5 pages)
    const startPage = Math.max(0, currentPage - 2);
    const endPage = Math.min(totalPages - 1, currentPage + 2);

    for (let i = startPage; i <= endPage; i++) {
        html += `
            <li class="page-item ${i === currentPage ? 'active' : ''}">
                <a class="page-link" href="#" onclick="changePage(${i}); return false;">${i + 1}</a>
            </li>
        `;
    }

    // Next button
    html += `
        <li class="page-item ${currentPage >= totalPages - 1 ? 'disabled' : ''}">
            <a class="page-link" href="#" onclick="changePage(${currentPage + 1}); return false;">Next</a>
        </li>
    `;

    pagination.innerHTML = html;
}

function changePage(page) {
    currentPage = page;
    loadCoins();
    window.scrollTo(0, 0);
}

function viewChart(symbol) {
    window.location.href = `/charts?symbol=${symbol}`;
}

function addToWatchlist(symbol, name) {
    // Check if already in watchlist - if so, remove it
    if (watchlistSymbols.has(symbol)) {
        removeFromWatchlist(symbol);
        return;
    }

    fetch('/api/watchlist', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({ symbol, name })
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            watchlistSymbols.add(symbol);
            loadCoins(); // Re-render to update button
        }
    })
    .catch(error => {
        console.error('Error:', error);
    });
}

function removeFromWatchlist(symbol) {
    // First, get the watchlist to find the item ID
    fetch('/api/watchlist')
        .then(response => response.json())
        .then(watchlist => {
            const item = watchlist.find(w => w.symbol === symbol);
            if (!item) {
                return;
            }

            // Delete the item
            fetch(`/api/watchlist/${item.id}`, {
                method: 'DELETE'
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    watchlistSymbols.delete(symbol);
                    loadCoins(); // Re-render to update button
                }
            })
            .catch(error => {
                console.error('Error:', error);
            });
        })
        .catch(error => {
            console.error('Error:', error);
        });
}

function updateData() {
    const btn = document.getElementById('updateDataBtn');
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm"></span> Updating...';

    fetch('/api/update-data', { method: 'POST' })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                showSuccess('Data updated successfully');
                loadCoins();
            } else {
                showError(data.error || 'Failed to update data');
            }
        })
        .catch(error => {
            console.error('Error:', error);
            showError('Failed to update data');
        })
        .finally(() => {
            btn.disabled = false;
            btn.innerHTML = '<i class="bi bi-arrow-repeat"></i> Update Data';
        });
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

function debounce(func, wait) {
    let timeout;
    return function(...args) {
        clearTimeout(timeout);
        timeout = setTimeout(() => func.apply(this, args), wait);
    };
}

function showSuccess(message) {
    showAlert(message, 'success');
}

function showError(message) {
    showAlert(message, 'danger');
}

function showAlert(message, type) {
    const alertDiv = document.createElement('div');
    alertDiv.className = `alert alert-${type} alert-dismissible fade show`;
    alertDiv.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
    `;
    document.querySelector('main').insertBefore(alertDiv, document.querySelector('main').firstChild);

    setTimeout(() => {
        alertDiv.remove();
    }, 5000);
}

function promptLogin() {
    showAlert('Please <a href="/login" class="alert-link">log in</a> or <a href="/register" class="alert-link">sign up</a> to add coins to your watchlist.', 'info');
}
