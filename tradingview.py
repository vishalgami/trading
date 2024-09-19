from tvDatafeed import TvDatafeed, Interval
import pandas as pd
import pytz
import mysql.connector
from mysql.connector import Error
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from flask import Flask, jsonify
import os

app = Flask(__name__)

# Step 1: Login to TradingView
tv = TvDatafeed(username='itsmevishalgami', password='')

from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError, PyMongoError

# MongoDB connection
def create_mongo_connection():
    try:
        # Replace with your MongoDB connection string
        mongo_uri = 'mongodb+srv://vishalgami:vishalgami@trading.dc0fv.mongodb.net/?retryWrites=true&w=majority&appName=Trading'
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)  # 5 seconds timeout
        db = client['trading']  # Replace 'trading' with your database name
        # Check if the server is reachable
        client.admin.command('ping')
        print("Connection to MongoDB database successful")
        return db
    except ServerSelectionTimeoutError as e:
        print(f"Connection Timeout Error: {e}")
        return None
    except PyMongoError as e:
        print(f"PyMongo Error: {e}")
        return None

# Create MongoDB collection and ensure indexes
def create_collections(db):
    db.etf_data.create_index([("datetime", 1), ("symbol", 1)], unique=True)
    # db.limit_orders.create_index([("id", 1)], unique=True)

# Insert data into MongoDB
def insert_data(db, data, symbol):
    collection = db.etf_data
    for index, row in data.iterrows():
        document = {
            'datetime': index.to_pydatetime(),
            'symbol': symbol,
            'open': row['open'],
            'high': row['high'],
            'low': row['low'],
            'close': row['close'],
            'volume': row['volume'],
            'sma20': row['SMA20'] if pd.notna(row['SMA20']) else None
        }
        collection.update_one(
            {'datetime': index.to_pydatetime(), 'symbol': symbol},
            {'$set': document},
            upsert=True
        )

def fetch_latest_data(symbol):
    data = tv.get_hist(symbol=symbol, exchange='NSE', interval=Interval.in_daily, n_bars=21)
    ist = pytz.timezone('Asia/Kolkata')
    data.index = data.index.tz_localize(pytz.utc).tz_convert(ist)
    data['SMA20'] = data['close'].rolling(window=20).mean()
    latest_data = data.iloc[-1:]
    
    db = create_mongo_connection()
    print(db.connect)
    if db is not None:
        insert_data(db, latest_data, symbol)
    return latest_data

def send_email(subject, body):
    sender_email = 'itsmevishal4@gmail.com'
    receiver_email = 'itsmevishalgami@gmail.com'
    password = 'aplc lbda hkai ncik'
    
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = subject
    
    msg.attach(MIMEText(body, 'plain'))
    
    try:
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            print(password)
            server.login(sender_email, password)
            text = msg.as_string()
            server.sendmail(sender_email, receiver_email, text)
            print("Email sent successfully")
    except Exception as e:
        print(f"Error sending email: {e}")

def execute_strategy(symbol):
    data = fetch_latest_data(symbol)
    latest_row = data.iloc[-1]
    date_time = data.index
    //last_close = latest_row['close']
    last_close = 'l56'
    last_sma20 = latest_row['SMA20']
    
    if last_close < last_sma20:
        subject = f"[IMPORTANT] Price Alert for {symbol}"
        body = f"The last closing price of {symbol} - {date_time} is {last_close}, which is below the 20-period SMA of {last_sma20}."
        send_email(subject, body)
    return {"symbol": symbol, "last_close": last_close, "last_sma20": last_sma20}

@app.route('/call')
def run_strategy():
    symbol = 'MON100'
    result = execute_strategy(symbol)
    return jsonify(result)

if __name__ == "__main__":
    # run_strategy()
    app.run(host='0.0.0.0', port=5000)
