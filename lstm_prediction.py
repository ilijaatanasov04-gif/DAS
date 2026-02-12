import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error, mean_absolute_percentage_error, r2_score
from sqlalchemy import text
from crypto import ensure_ohlcv_data, get_db_engine, fetch_mapping
from datetime import datetime, timedelta
import pickle
import os

# Import TensorFlow/Keras for LSTM
try:
    import tensorflow as tf
    from tensorflow import keras
    from tensorflow.keras.models import Sequential, load_model
    from tensorflow.keras.layers import LSTM, Dense, Dropout
    from tensorflow.keras.callbacks import EarlyStopping
    TENSORFLOW_AVAILABLE = True
except ImportError:
    TENSORFLOW_AVAILABLE = False
    print("TensorFlow not available. Please install: pip install tensorflow")


class LSTMPricePredictor:
    """LSTM model for cryptocurrency price prediction"""

    def __init__(self, symbol, lookback_days=30):
        self.symbol = symbol.upper()
        self.lookback_days = lookback_days
        self.model = None
        self.scaler = MinMaxScaler(feature_range=(0, 1))
        self.model_path = f'models/lstm_{self.symbol}.h5'
        self.scaler_path = f'models/scaler_{self.symbol}.pkl'

        # Create models directory if it doesn't exist
        os.makedirs('models', exist_ok=True)

    def get_historical_data(self, days=365):
        """Fetch historical OHLCV data from database"""
        pair = self.symbol + 'USDT'
        cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

        query = """
            SELECT date, open, high, low, close, volume
            FROM ohlcv_data
            WHERE symbol = :symbol AND date >= :cutoff_date
            ORDER BY date ASC
        """

        engine = get_db_engine()
        df = pd.read_sql_query(text(query), engine, params={"symbol": pair, "cutoff_date": cutoff_date})

        if df.empty:
            ensure_ohlcv_data(self.symbol)
            df = pd.read_sql_query(text(query), engine, params={"symbol": pair, "cutoff_date": cutoff_date})
            if df.empty:
                return None

        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')

        return df

    def prepare_data(self, df, target_column='close'):
        """Prepare data for LSTM training"""
        # Use all OHLCV features
        features = ['open', 'high', 'low', 'close', 'volume']
        data = df[features].values

        # Normalize the data
        scaled_data = self.scaler.fit_transform(data)

        # Create sequences for LSTM
        X, y = [], []

        for i in range(self.lookback_days, len(scaled_data)):
            X.append(scaled_data[i-self.lookback_days:i])
            # Predict close price (index 3 in features)
            y.append(scaled_data[i, 3])

        X, y = np.array(X), np.array(y)

        # Split into train (70%) and validation (30%)
        split_idx = int(len(X) * 0.7)

        X_train, X_val = X[:split_idx], X[split_idx:]
        y_train, y_val = y[:split_idx], y[split_idx:]

        return X_train, X_val, y_train, y_val

    def build_model(self, input_shape):
        """Build LSTM model architecture"""
        model = Sequential([
            # First LSTM layer with dropout
            LSTM(units=50, return_sequences=True, input_shape=input_shape),
            Dropout(0.2),

            # Second LSTM layer
            LSTM(units=50, return_sequences=True),
            Dropout(0.2),

            # Third LSTM layer
            LSTM(units=50, return_sequences=False),
            Dropout(0.2),

            # Dense layers
            Dense(units=25),
            Dense(units=1)
        ])

        # Compile model with MSE loss function as specified
        model.compile(optimizer='adam', loss='mean_squared_error')

        return model

    def train(self, epochs=50, batch_size=32):
        """Train the LSTM model"""
        if not TENSORFLOW_AVAILABLE:
            raise Exception("TensorFlow is not installed")

        # Get historical data
        df = self.get_historical_data(days=730)  # 2 years of data

        if df is None or len(df) < self.lookback_days + 50:
            raise Exception(f"Insufficient data for {self.symbol}")

        # Prepare data
        X_train, X_val, y_train, y_val = self.prepare_data(df)

        # Build model
        self.model = self.build_model(input_shape=(X_train.shape[1], X_train.shape[2]))

        # Early stopping to prevent overfitting
        early_stop = EarlyStopping(monitor='val_loss', patience=10, restore_best_weights=True)

        # Train model
        history = self.model.fit(
            X_train, y_train,
            validation_data=(X_val, y_val),
            epochs=epochs,
            batch_size=batch_size,
            callbacks=[early_stop],
            verbose=1
        )

        # Save model and scaler
        self.model.save(self.model_path)
        with open(self.scaler_path, 'wb') as f:
            pickle.dump(self.scaler, f)

        return history

    def load_trained_model(self):
        """Load previously trained model"""
        if not TENSORFLOW_AVAILABLE:
            raise Exception("TensorFlow is not installed")

        if os.path.exists(self.model_path) and os.path.exists(self.scaler_path):
            self.model = load_model(self.model_path)
            with open(self.scaler_path, 'rb') as f:
                self.scaler = pickle.load(f)
            return True
        return False

    def evaluate(self):
        """Evaluate model performance on validation data"""
        if self.model is None:
            if not self.load_trained_model():
                raise Exception("No trained model found")

        # Get data
        df = self.get_historical_data(days=730)
        if df is None:
            raise Exception(f"No data available for {self.symbol}")

        # Prepare data
        X_train, X_val, y_train, y_val = self.prepare_data(df)

        # Make predictions
        predictions = self.model.predict(X_val)

        # Inverse transform to get actual prices
        # Create dummy array with same shape as original features
        dummy = np.zeros((len(predictions), 5))
        dummy[:, 3] = predictions.flatten()
        predictions_actual = self.scaler.inverse_transform(dummy)[:, 3]

        dummy[:, 3] = y_val
        y_val_actual = self.scaler.inverse_transform(dummy)[:, 3]

        # Calculate metrics
        rmse = np.sqrt(mean_squared_error(y_val_actual, predictions_actual))
        mape = mean_absolute_percentage_error(y_val_actual, predictions_actual) * 100
        r2 = r2_score(y_val_actual, predictions_actual)

        return {
            'rmse': round(rmse, 2),
            'mape': round(mape, 2),
            'r2_score': round(r2, 4),
            'actual_prices': y_val_actual.tolist()[-30:],  # Last 30 actual prices
            'predicted_prices': predictions_actual.tolist()[-30:]  # Last 30 predictions
        }

    def predict_future(self, days_ahead=7):
        """Predict future prices"""
        if self.model is None:
            if not self.load_trained_model():
                raise Exception("No trained model found")

        # Get recent data
        df = self.get_historical_data(days=365)
        if df is None or len(df) < self.lookback_days:
            raise Exception(f"Insufficient data for {self.symbol}")

        # Prepare data
        features = ['open', 'high', 'low', 'close', 'volume']
        data = df[features].values
        scaled_data = self.scaler.transform(data)

        # Get last lookback_days of data
        last_sequence = scaled_data[-self.lookback_days:]

        predictions = []
        current_sequence = last_sequence.copy()

        # Predict future prices
        for _ in range(days_ahead):
            # Reshape for prediction
            input_seq = current_sequence.reshape(1, self.lookback_days, len(features))

            # Predict next price
            next_pred = self.model.predict(input_seq, verbose=0)[0, 0]

            # Store prediction
            predictions.append(next_pred)

            # Create next input sequence
            # Use predicted close price, assume other values stay similar
            next_row = current_sequence[-1].copy()
            next_row[3] = next_pred  # Update close price

            # Shift sequence
            current_sequence = np.vstack([current_sequence[1:], next_row])

        # Inverse transform predictions to actual prices
        dummy = np.zeros((len(predictions), 5))
        dummy[:, 3] = predictions
        predictions_actual = self.scaler.inverse_transform(dummy)[:, 3]

        # Generate future dates
        last_date = df['date'].iloc[-1]
        future_dates = [(last_date + timedelta(days=i+1)).strftime('%Y-%m-%d')
                        for i in range(days_ahead)]

        return {
            'dates': future_dates,
            'prices': [round(p, 2) for p in predictions_actual]
        }


def train_model_for_symbol(symbol, lookback_days=30, epochs=50):
    """Train LSTM model for a specific cryptocurrency"""
    predictor = LSTMPricePredictor(symbol, lookback_days=lookback_days)

    try:
        history = predictor.train(epochs=epochs)
        evaluation = predictor.evaluate()

        return {
            'success': True,
            'symbol': symbol,
            'evaluation': evaluation,
            'message': f'Model trained successfully for {symbol}'
        }
    except Exception as e:
        return {
            'success': False,
            'symbol': symbol,
            'error': str(e)
        }


def predict_price(symbol, days_ahead=7, lookback_days=30):
    """Predict future prices for a cryptocurrency"""
    predictor = LSTMPricePredictor(symbol, lookback_days=lookback_days)

    try:
        # Try to load existing model first
        if not predictor.load_trained_model():
            # Train new model if not exists
            predictor.train(epochs=50)

        # Get predictions
        predictions = predictor.predict_future(days_ahead=days_ahead)

        # Get evaluation metrics
        evaluation = predictor.evaluate()

        # Get current price
        coin = fetch_mapping(
            "SELECT price FROM top_coins WHERE UPPER(symbol) = :symbol",
            {"symbol": symbol.upper()}
        )
        current_price = coin['price'] if coin else None

        return {
            'success': True,
            'symbol': symbol.upper(),
            'current_price': current_price,
            'predictions': predictions,
            'evaluation': evaluation,
            'lookback_days': lookback_days
        }

    except Exception as e:
        return {
            'success': False,
            'symbol': symbol,
            'error': str(e)
        }
