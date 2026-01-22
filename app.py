from flask import Flask, render_template, request, jsonify, redirect, url_for, flash
from flask_login import LoginManager, login_user, logout_user, login_required, current_user
from flask_cors import CORS
from models import db, User, Watchlist, Notification, Portfolio
from crypto import init_db, run_pipeline, DB_PATH, get_db_connection, ensure_ohlcv_data
from technical_analysis import analyze_symbol
import sqlite3
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from threading import Lock

load_dotenv()

app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'dev-secret-key')
DATA_DIR = os.getenv('DATA_DIR', os.path.dirname(os.path.abspath(__file__)))
os.makedirs(DATA_DIR, exist_ok=True)
USERS_DB_PATH = os.getenv('USERS_DB_PATH', os.path.join(DATA_DIR, 'users.db'))
app.config['SQLALCHEMY_DATABASE_URI'] = f"sqlite:///{USERS_DB_PATH}"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Enable CORS for Next.js frontend
CORS(app, resources={r"/api/*": {"origins": "http://localhost:3000"}})

db.init_app(app)
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'
login_manager.login_message = 'Please log in or sign up to access this page.'
login_manager.login_message_category = 'info'

pipeline_lock = Lock()

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))

# Initialize databases
with app.app_context():
    db.create_all()
    init_db()

# ==================== AUTHENTICATION ROUTES ====================

@app.route('/register', methods=['GET', 'POST'])
def register():
    if current_user.is_authenticated:
        return redirect(url_for('index'))

    if request.method == 'POST':
        email = request.form.get('email')
        password = request.form.get('password')
        confirm_password = request.form.get('confirm_password')

        if password != confirm_password:
            flash('Passwords do not match', 'danger')
            return redirect(url_for('register'))

        if User.query.filter_by(email=email).first():
            flash('Email already registered', 'danger')
            return redirect(url_for('register'))

        user = User(email=email)
        user.set_password(password)
        db.session.add(user)
        db.session.commit()

        flash('Sign up successful! Please log in.', 'success')
        return redirect(url_for('login'))

    return render_template('register.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('index'))

    if request.method == 'POST':
        email = request.form.get('email')
        password = request.form.get('password')

        user = User.query.filter_by(email=email).first()

        if user and user.check_password(password):
            login_user(user)
            flash('Logged in successfully!', 'success')
            return redirect(url_for('index'))
        else:
            flash('Invalid email or password', 'danger')

    return render_template('login.html')

@app.route('/logout')
@login_required
def logout():
    logout_user()
    flash('Logged out successfully!', 'success')
    return redirect(url_for('index'))

# ==================== MAIN ROUTES ====================

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/charts')
def charts():
    return render_template('charts.html')

@app.route('/watchlist')
@login_required
def watchlist_page():
    return render_template('watchlist.html')

@app.route('/notifications')
@login_required
def notifications_page():
    return render_template('notifications.html')

@app.route('/portfolio')
@login_required
def portfolio_page():
    return render_template('portfolio.html')

# ==================== API ENDPOINTS ====================

@app.route('/api/coins')
def get_coins():
    search = request.args.get('search', '').upper()
    sort_by = request.args.get('sort', 'market_cap_rank')
    order = request.args.get('order', 'asc')
    limit = int(request.args.get('limit', 100))
    offset = int(request.args.get('offset', 0))

    conn = get_db_connection()
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    query = "SELECT * FROM top_coins WHERE 1=1"
    params = []

    if search:
        query += " AND (symbol LIKE ? OR name LIKE ?)"
        params.extend([f'%{search}%', f'%{search}%'])

    valid_sorts = ['market_cap_rank', 'price', 'market_cap', 'volume_24h', 'liquidity_score']
    if sort_by not in valid_sorts:
        sort_by = 'market_cap_rank'

    order_dir = 'DESC' if order == 'desc' else 'ASC'
    query += f" ORDER BY {sort_by} {order_dir} LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    c.execute(query, params)
    rows = c.fetchall()

    coins = [dict(row) for row in rows]

    c.execute("SELECT COUNT(*) as count FROM top_coins")
    total = c.fetchone()['count']

    conn.close()

    return jsonify({
        'coins': coins,
        'total': total,
        'limit': limit,
        'offset': offset
    })

@app.route('/api/coin/<symbol>')
def get_coin_details(symbol):
    conn = get_db_connection()
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    c.execute("SELECT * FROM top_coins WHERE symbol = ?", (symbol.upper(),))
    coin = c.fetchone()

    if not coin:
        conn.close()
        return jsonify({'error': 'Coin not found'}), 404

    conn.close()
    return jsonify(dict(coin))

@app.route('/api/ohlcv/<symbol>')
def get_ohlcv_data(symbol):
    period = request.args.get('period', '1m')  # 1m, 3m, 6m, 1y, 10y
    pair = symbol.upper() + 'USDT'

    period_map = {
        '1m': 30,
        '3m': 90,
        '6m': 180,
        '1y': 365,
        '10y': 3650
    }

    days = period_map.get(period, 30)
    cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

    conn = get_db_connection()
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    c.execute("""
        SELECT date, open, high, low, close, volume
        FROM ohlcv_data
        WHERE symbol = ? AND date >= ?
        ORDER BY date ASC
    """, (pair, cutoff_date))

    rows = c.fetchall()
    data = [dict(row) for row in rows]

    if not data:
        conn.close()
        ensure_ohlcv_data(symbol)
        conn = get_db_connection()
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("""
            SELECT date, open, high, low, close, volume
            FROM ohlcv_data
            WHERE symbol = ? AND date >= ?
            ORDER BY date ASC
        """, (pair, cutoff_date))
        rows = c.fetchall()
        data = [dict(row) for row in rows]

    conn.close()
    return jsonify(data)

# ==================== WATCHLIST API ====================

@app.route('/api/watchlist', methods=['GET'])
@login_required
def get_watchlist():
    watchlist_items = Watchlist.query.filter_by(user_id=current_user.id).all()

    items = []
    conn = get_db_connection()
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    for item in watchlist_items:
        c.execute("SELECT * FROM top_coins WHERE symbol = ?", (item.symbol,))
        coin = c.fetchone()

        if coin:
            items.append({
                'id': item.id,
                'symbol': item.symbol,
                'name': item.name,
                'added_at': item.added_at.isoformat(),
                'price': coin['price'],
                'market_cap': coin['market_cap'],
                'volume_24h': coin['volume_24h'],
                'market_cap_rank': coin['market_cap_rank'],
                'liquidity_score': coin['liquidity_score']
            })

    conn.close()
    return jsonify(items)

@app.route('/api/watchlist', methods=['POST'])
@login_required
def add_to_watchlist():
    data = request.get_json()
    symbol = data.get('symbol', '').upper()
    name = data.get('name', '')

    if not symbol:
        return jsonify({'error': 'Symbol is required'}), 400

    existing = Watchlist.query.filter_by(user_id=current_user.id, symbol=symbol).first()
    if existing:
        return jsonify({'error': 'Already in watchlist'}), 400

    watchlist_item = Watchlist(user_id=current_user.id, symbol=symbol, name=name)
    db.session.add(watchlist_item)
    db.session.commit()

    return jsonify({'success': True, 'id': watchlist_item.id})

@app.route('/api/watchlist/<int:item_id>', methods=['DELETE'])
@login_required
def remove_from_watchlist(item_id):
    item = Watchlist.query.filter_by(id=item_id, user_id=current_user.id).first()

    if not item:
        return jsonify({'error': 'Item not found'}), 404

    db.session.delete(item)
    db.session.commit()

    return jsonify({'success': True})

# ==================== NOTIFICATIONS API ====================

@app.route('/api/notifications', methods=['GET'])
@login_required
def get_notifications():
    notifications = Notification.query.filter_by(user_id=current_user.id).order_by(Notification.created_at.desc()).all()

    return jsonify([{
        'id': n.id,
        'symbol': n.symbol,
        'condition': n.condition,
        'target_price': n.target_price,
        'current_price': n.current_price,
        'triggered': n.triggered,
        'triggered_at': n.triggered_at.isoformat() if n.triggered_at else None,
        'created_at': n.created_at.isoformat()
    } for n in notifications])

@app.route('/api/notifications', methods=['POST'])
@login_required
def create_notification():
    data = request.get_json()
    symbol = data.get('symbol', '').upper()
    condition = data.get('condition')
    target_price = data.get('target_price')

    if not all([symbol, condition, target_price]):
        return jsonify({'error': 'Missing required fields'}), 400

    if condition not in ['above', 'below']:
        return jsonify({'error': 'Invalid condition'}), 400

    notification = Notification(
        user_id=current_user.id,
        symbol=symbol,
        condition=condition,
        target_price=float(target_price)
    )

    db.session.add(notification)
    db.session.commit()

    return jsonify({'success': True, 'id': notification.id})

@app.route('/api/notifications/<int:notif_id>', methods=['DELETE'])
@login_required
def delete_notification(notif_id):
    notification = Notification.query.filter_by(id=notif_id, user_id=current_user.id).first()

    if not notification:
        return jsonify({'error': 'Notification not found'}), 404

    db.session.delete(notification)
    db.session.commit()

    return jsonify({'success': True})

@app.route('/api/notifications/check')
@login_required
def check_notifications():
    """Check and trigger notifications based on current prices"""
    notifications = Notification.query.filter_by(user_id=current_user.id, triggered=False).all()

    conn = get_db_connection()
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    triggered = []
    for notif in notifications:
        c.execute("SELECT price FROM top_coins WHERE symbol = ?", (notif.symbol,))
        coin = c.fetchone()

        if coin and notif.check_trigger(coin['price']):
            triggered.append({
                'id': notif.id,
                'symbol': notif.symbol,
                'condition': notif.condition,
                'target_price': notif.target_price,
                'current_price': coin['price']
            })

    conn.close()
    db.session.commit()

    return jsonify({'triggered': triggered})

# ==================== PORTFOLIO ROUTES ====================

@app.route('/api/portfolio', methods=['GET'])
@login_required
def get_portfolio():
    portfolio_items = Portfolio.query.filter_by(user_id=current_user.id).all()

    items = []
    total_purchase_value = 0
    total_current_value = 0

    conn = get_db_connection()
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    for item in portfolio_items:
        c.execute("SELECT * FROM top_coins WHERE symbol = ?", (item.symbol,))
        coin = c.fetchone()

        if coin:
            current_price = coin['price']
            stats = item.calculate_profit_loss(current_price)

            items.append({
                'id': item.id,
                'symbol': item.symbol,
                'name': item.name,
                'amount': item.amount,
                'purchase_price': item.purchase_price,
                'current_price': current_price,
                'purchased_at': item.purchased_at.isoformat(),
                'purchase_value': stats['purchase_value'],
                'current_value': stats['current_value'],
                'profit_loss': stats['profit_loss'],
                'profit_loss_percentage': stats['profit_loss_percentage'],
                'market_cap_rank': coin['market_cap_rank']
            })

            total_purchase_value += stats['purchase_value']
            total_current_value += stats['current_value']

    conn.close()

    total_profit_loss = total_current_value - total_purchase_value
    total_profit_loss_percentage = (total_profit_loss / total_purchase_value) * 100 if total_purchase_value > 0 else 0

    return jsonify({
        'items': items,
        'summary': {
            'total_purchase_value': total_purchase_value,
            'total_current_value': total_current_value,
            'total_profit_loss': total_profit_loss,
            'total_profit_loss_percentage': total_profit_loss_percentage
        }
    })

@app.route('/api/portfolio', methods=['POST'])
@login_required
def add_to_portfolio():
    data = request.get_json()
    symbol = data.get('symbol', '').upper()
    name = data.get('name', '')
    amount = float(data.get('amount', 0))
    purchase_price = float(data.get('purchase_price', 0))

    if not symbol or amount <= 0 or purchase_price <= 0:
        return jsonify({'error': 'Invalid data'}), 400

    portfolio_item = Portfolio(
        user_id=current_user.id,
        symbol=symbol,
        name=name,
        amount=amount,
        purchase_price=purchase_price
    )
    db.session.add(portfolio_item)
    db.session.commit()

    return jsonify({'success': True, 'id': portfolio_item.id})

@app.route('/api/portfolio/<int:item_id>', methods=['DELETE'])
@login_required
def delete_portfolio_item(item_id):
    item = Portfolio.query.filter_by(id=item_id, user_id=current_user.id).first()

    if not item:
        return jsonify({'error': 'Item not found'}), 404

    db.session.delete(item)
    db.session.commit()

    return jsonify({'success': True})

# ==================== TECHNICAL ANALYSIS ROUTES ====================

@app.route('/technical-analysis')
def technical_analysis_page():
    return render_template('technical_analysis.html')

@app.route('/api/technical-analysis/<symbol>')
def get_technical_analysis(symbol):
    """Get technical analysis for a coin"""
    try:
        ensure_ohlcv_data(symbol)
        result = analyze_symbol(symbol)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ==================== LSTM PRICE PREDICTION ROUTES ====================

@app.route('/price-prediction')
def price_prediction_page():
    return render_template('price_prediction.html')

@app.route('/api/predict-price/<symbol>')
def predict_crypto_price(symbol):
    """Predict future prices using LSTM"""
    try:
        from lstm_prediction import predict_price

        days_ahead = int(request.args.get('days', 7))
        lookback = int(request.args.get('lookback', 30))

        ensure_ohlcv_data(symbol)
        result = predict_price(symbol, days_ahead=days_ahead, lookback_days=lookback)
        return jsonify(result)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/train-model/<symbol>', methods=['POST'])
@login_required
def train_lstm_model(symbol):
    """Train LSTM model for a specific cryptocurrency"""
    try:
        from lstm_prediction import train_model_for_symbol

        data = request.get_json() or {}
        lookback = int(data.get('lookback', 30))
        epochs = int(data.get('epochs', 50))

        result = train_model_for_symbol(symbol, lookback_days=lookback, epochs=epochs)
        return jsonify(result)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ==================== ADMIN/MAINTENANCE ROUTES ====================

@app.route('/api/update-data', methods=['POST'])
@login_required
def update_data():
    """Trigger data pipeline update"""
    if not pipeline_lock.acquire(blocking=False):
        return jsonify({'success': False, 'error': 'Update already in progress'}), 429

    try:
        run_pipeline()
        return jsonify({'success': True, 'message': 'Data updated successfully'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        pipeline_lock.release()

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)
