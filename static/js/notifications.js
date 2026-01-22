let availableCoins = [];
let selectedCoin = null;
let notificationToDelete = null;

document.addEventListener('DOMContentLoaded', function() {
    loadNotifications();
    loadAvailableCoins();

    const coinSelector = document.getElementById('notifSymbol');
    const coinDropdown = document.getElementById('notifCoinDropdown');

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
        // Clear selection if user is typing
        selectedCoin = null;
        document.getElementById('selectedSymbol').value = '';
        document.getElementById('selectedCoin').innerHTML = '';
    });

    // Close dropdown when clicking outside
    document.addEventListener('click', function(e) {
        if (!coinSelector.contains(e.target) && !coinDropdown.contains(e.target)) {
            coinDropdown.classList.remove('show');
        }
    });

    document.getElementById('createNotificationBtn').addEventListener('click', createNotification);

    // Confirm delete button handler
    const confirmDeleteBtn = document.getElementById('confirmDeleteBtn');
    if (confirmDeleteBtn) {
        confirmDeleteBtn.addEventListener('click', function() {
            if (notificationToDelete !== null) {
                fetch(`/api/notifications/${notificationToDelete}`, {
                    method: 'DELETE'
                })
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        loadNotifications();
                        const deleteModal = bootstrap.Modal.getInstance(document.getElementById('deleteNotificationModal'));
                        deleteModal.hide();
                        notificationToDelete = null;
                    }
                })
                .catch(error => {
                    console.error('Error:', error);
                });
            }
        });
    }

    // Check notifications every 5 seconds for faster response
    setInterval(checkNotifications, 5000);

    // Also do an immediate check
    checkNotifications();
});

function loadAvailableCoins() {
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
    const dropdown = document.getElementById('notifCoinDropdown');
    const term = searchTerm.toLowerCase();

    let filteredCoins = availableCoins.filter(coin => {
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
        <a class="dropdown-item" href="#" onclick="event.preventDefault(); selectCoin('${coin.symbol}', '${coin.name}')">
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

function selectCoin(symbol, name) {
    selectedCoin = { symbol, name };
    document.getElementById('selectedSymbol').value = symbol;
    document.getElementById('notifSymbol').value = '';
    document.getElementById('selectedCoin').innerHTML = `
        <span class="badge bg-primary" style="font-size: 0.9rem;">
            <strong>${symbol}</strong> <small>${name}</small>
            <i class="bi bi-x-circle ms-2" onclick="clearCoinSelection()" style="cursor: pointer;"></i>
        </span>
    `;
    document.getElementById('notifCoinDropdown').classList.remove('show');
}

function clearCoinSelection() {
    selectedCoin = null;
    document.getElementById('selectedSymbol').value = '';
    document.getElementById('selectedCoin').innerHTML = '';
    document.getElementById('notifSymbol').value = '';
}

function loadNotifications() {
    fetch('/api/notifications')
        .then(response => response.json())
        .then(data => {
            renderNotifications(data);
        })
        .catch(error => {
            console.error('Error loading notifications:', error);
            showError('Failed to load notifications');
        });
}

function renderNotifications(notifications) {
    const active = notifications.filter(n => !n.triggered);
    const triggered = notifications.filter(n => n.triggered);

    const activeContainer = document.getElementById('activeNotifications');
    const triggeredContainer = document.getElementById('triggeredNotifications');

    if (active.length === 0) {
        activeContainer.innerHTML = '<p class="text-muted">No active notifications</p>';
    } else {
        activeContainer.innerHTML = active.map(n => `
            <div class="card mb-2">
                <div class="card-body p-3">
                    <div class="d-flex justify-content-between align-items-center">
                        <div>
                            <h6 class="mb-1"><strong>${n.symbol}</strong></h6>
                            <small class="text-muted">
                                Alert when price goes <strong>${n.condition}</strong> $${n.target_price.toFixed(2)}
                            </small>
                            ${n.current_price ? `<br><small>Current: $${n.current_price.toFixed(2)}</small>` : ''}
                        </div>
                        <button class="btn btn-sm btn-outline-danger" onclick="deleteNotification(${n.id})">
                            <i class="bi bi-trash"></i>
                        </button>
                    </div>
                </div>
            </div>
        `).join('');
    }

    if (triggered.length === 0) {
        triggeredContainer.innerHTML = '<p class="text-muted">No triggered notifications</p>';
    } else {
        triggeredContainer.innerHTML = triggered.map(n => `
            <div class="card mb-2 border-success">
                <div class="card-body p-3">
                    <div class="d-flex justify-content-between align-items-center">
                        <div>
                            <h6 class="mb-1">
                                <i class="bi bi-check-circle-fill text-success"></i>
                                <strong>${n.symbol}</strong>
                            </h6>
                            <small class="text-muted">
                                Went ${n.condition} $${n.target_price.toFixed(2)}
                            </small>
                            <br>
                            <small>Price: $${n.current_price.toFixed(2)}</small>
                            <br>
                            <small class="text-muted">Triggered: ${new Date(n.triggered_at).toLocaleString()}</small>
                        </div>
                        <button class="btn btn-sm btn-outline-danger" onclick="deleteNotification(${n.id})">
                            <i class="bi bi-trash"></i>
                        </button>
                    </div>
                </div>
            </div>
        `).join('');
    }
}

function createNotification() {
    const symbol = document.getElementById('selectedSymbol').value;
    const condition = document.getElementById('notifCondition').value;
    const targetPrice = parseFloat(document.getElementById('notifPrice').value);

    if (!symbol || !condition || !targetPrice) {
        return;
    }

    if (targetPrice <= 0) {
        return;
    }

    fetch('/api/notifications', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({
            symbol: symbol,
            condition: condition,
            target_price: targetPrice
        })
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            loadNotifications();

            // Close modal and reset form
            const modal = bootstrap.Modal.getInstance(document.getElementById('createNotificationModal'));
            modal.hide();
            document.getElementById('notificationForm').reset();
            clearCoinSelection();
        }
    })
    .catch(error => {
        console.error('Error:', error);
    });
}

function deleteNotification(id) {
    notificationToDelete = id;
    const deleteModal = new bootstrap.Modal(document.getElementById('deleteNotificationModal'));
    deleteModal.show();
}

function checkNotifications() {
    fetch('/api/notifications/check')
        .then(response => response.json())
        .then(data => {
            if (data.triggered && data.triggered.length > 0) {
                loadNotifications();
            }
        })
        .catch(error => {
            console.error('Error checking notifications:', error);
        });
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
