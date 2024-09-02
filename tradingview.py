from tvDatafeed import TvDatafeed, Interval
import pandas as pd
import pytz
import mysql.connector
from mysql.connector import Error
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from flask import Flask
import os

app = Flask(__name__)

# Step 1: Login to TradingView
tv = TvDatafeed(username='itsmevishalgami', password='')

# Database connection functions
def create_connection():
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='root',
            database='trading'
        )
        if connection.is_connected():
            print("Connection to MySQL database successful")
            return connection
    except Error as e:
        print(f"Error: {e}")
        return None

def create_table(cursor):
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS etf_data (
        datetime DATETIME,
        symbol VARCHAR(10),
        open FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        volume INT,
        sma20 FLOAT,
        PRIMARY KEY (datetime, symbol)
    )
    '''
    cursor.execute(create_table_query)
    
    create_orders_table_query = '''
    CREATE TABLE IF NOT EXISTS limit_orders (
        id INT AUTO_INCREMENT PRIMARY KEY,
        symbol VARCHAR(10),
        order_type VARCHAR(10),
        price FLOAT,
        investment FLOAT,
        status VARCHAR(20),
        filled FLOAT,
        created_at DATETIME,
        updated_at DATETIME
    )
    '''
    cursor.execute(create_orders_table_query)

def insert_data(cursor, data, symbol):
    insert_query = '''
    INSERT INTO etf_data (datetime, symbol, open, high, low, close, volume, sma20)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        open=VALUES(open),
        high=VALUES(high),
        low=VALUES(low),
        close=VALUES(close),
        volume=VALUES(volume),
        sma20=VALUES(sma20)
    '''
    for index, row in data.iterrows():
        values = (
            index,
            symbol,
            row['open'],
            row['high'],
            row['low'],
            row['close'],
            row['volume'],
            row['SMA20'] if pd.notna(row['SMA20']) else None
        )
        cursor.execute(insert_query, values)

def insert_order(cursor, symbol, order_type, price, investment, status, filled):
    insert_query = '''
    INSERT INTO limit_orders (symbol, order_type, price, investment, status, filled, created_at, updated_at)
    VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
    '''
    values = (symbol, order_type, price, investment, status, filled)
    cursor.execute(insert_query, values)

def fetch_latest_data(symbol):
    data = tv.get_hist(symbol=symbol, exchange='NSE', interval=Interval.in_daily, n_bars=21)
    ist = pytz.timezone('Asia/Kolkata')
    data.index = data.index.tz_localize(pytz.utc).tz_convert(ist)
    data['SMA20'] = data['close'].rolling(window=20).mean()
    latest_data = data.iloc[-1:]
    
    connection = create_connection()
    if connection:
        cursor = connection.cursor()
        insert_data(cursor, latest_data, symbol)
        connection.commit()
        cursor.close()
        connection.close()
    print(latest_data)
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
    last_close = latest_row['close']
    last_sma20 = latest_row['SMA20']
    
    if last_close > last_sma20:
        subject = f"[IMPORTANT] Price Alert for {symbol}"
        body = f"The last closing price of {symbol} is {last_close}, which is below the 20-period SMA of {last_sma20}."
        send_email(subject, body)

def main():
    symbol = 'MON100'
    execute_strategy(symbol)

if __name__ == "__main__":
    main()
    app.run(host='0.0.0.0', port=5000)
