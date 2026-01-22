from flask_sqlalchemy import SQLAlchemy
from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime

db = SQLAlchemy()

class User(UserMixin, db.Model):
    __tablename__ = 'users'

    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password_hash = db.Column(db.String(200), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    watchlists = db.relationship('Watchlist', backref='user', lazy=True, cascade='all, delete-orphan')
    notifications = db.relationship('Notification', backref='user', lazy=True, cascade='all, delete-orphan')
    portfolio_items = db.relationship('Portfolio', backref='user', lazy=True, cascade='all, delete-orphan')

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

class Watchlist(db.Model):
    __tablename__ = 'watchlists'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    symbol = db.Column(db.String(20), nullable=False)
    name = db.Column(db.String(100))
    added_at = db.Column(db.DateTime, default=datetime.utcnow)

    __table_args__ = (db.UniqueConstraint('user_id', 'symbol', name='_user_symbol_uc'),)

class Notification(db.Model):
    __tablename__ = 'notifications'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    symbol = db.Column(db.String(20), nullable=False)
    condition = db.Column(db.String(10), nullable=False)  # 'above' or 'below'
    target_price = db.Column(db.Float, nullable=False)
    current_price = db.Column(db.Float)
    triggered = db.Column(db.Boolean, default=False)
    triggered_at = db.Column(db.DateTime)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def check_trigger(self, current_price):
        self.current_price = current_price

        if self.triggered:
            return False

        if self.condition == 'above' and current_price >= self.target_price:
            self.triggered = True
            self.triggered_at = datetime.utcnow()
            return True
        elif self.condition == 'below' and current_price <= self.target_price:
            self.triggered = True
            self.triggered_at = datetime.utcnow()
            return True

        return False

class Portfolio(db.Model):
    __tablename__ = 'portfolio'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    symbol = db.Column(db.String(20), nullable=False)
    name = db.Column(db.String(100))
    amount = db.Column(db.Float, nullable=False)  # Amount of coins owned
    purchase_price = db.Column(db.Float, nullable=False)  # Price per coin at purchase
    purchased_at = db.Column(db.DateTime, default=datetime.utcnow)

    def calculate_profit_loss(self, current_price):
        """Calculate profit/loss for this portfolio item"""
        purchase_value = self.amount * self.purchase_price
        current_value = self.amount * current_price
        profit_loss = current_value - purchase_value
        profit_loss_percentage = (profit_loss / purchase_value) * 100 if purchase_value > 0 else 0
        return {
            'purchase_value': purchase_value,
            'current_value': current_value,
            'profit_loss': profit_loss,
            'profit_loss_percentage': profit_loss_percentage
        }
