document.addEventListener('DOMContentLoaded', function() {
    loadWatchlist();

    // Refresh every 30 seconds
    setInterval(loadWatchlist, 30000);
});

function loadWatchlist() {
    fetch('/api/watchlist')
        .then(response => response.json())
        .then(data => {
            renderWatchlist(data);
        })
        .catch(error => {
            console.error('Error loading watchlist:', error);
            showError('Failed to load watchlist');
        });
}

function renderWatchlist(items) {
    const tbody = document.getElementById('watchlistBody');
    const emptyMessage = document.getElementById('emptyWatchlist');
    const table = document.getElementById('watchlistTable');

    if (items.length === 0) {
        emptyMessage.style.display = 'block';
        table.style.display = 'none';
        return;
    }

    emptyMessage.style.display = 'none';
    table.style.display = 'block';

    tbody.innerHTML = items.map(item => `
        <tr>
            <td>${item.market_cap_rank || '-'}</td>
            <td><strong>${item.symbol}</strong></td>
            <td>${item.name}</td>
            <td>$${formatNumber(item.price)}</td>
            <td>$${formatLargeNumber(item.market_cap)}</td>
            <td>$${formatLargeNumber(item.volume_24h)}</td>
            <td>${item.liquidity_score ? (item.liquidity_score * 100).toFixed(3) + '%' : '-'}</td>
            <td>
                <button class="btn btn-sm btn-outline-primary" onclick="viewChart('${item.symbol}')">
                    <i class="bi bi-graph-up"></i>
                </button>
                <button class="btn btn-sm btn-warning" onclick="removeFromWatchlist(${item.id}, '${item.symbol}')">
                    <i class="bi bi-star-fill"></i>
                </button>
            </td>
        </tr>
    `).join('');
}

function viewChart(symbol) {
    window.location.href = `/charts?symbol=${symbol}`;
}

function removeFromWatchlist(id, symbol) {
    fetch(`/api/watchlist/${id}`, {
        method: 'DELETE'
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            loadWatchlist();
        }
    })
    .catch(error => {
        console.error('Error:', error);
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
