from flask import Flask
import threading
import os
import logging
import requests
import time

app = Flask(__name__)

@app.route('/')
def home():
    return "🤖 Bot is running!"

@app.route('/ping')
def ping():
    return "pong"

@app.route('/health')
def health():
    return "OK"

@app.route('/up')
def up():
    return "🟢 Bot is alive and running!"

def start_web_server():
    try:
        # Use Render's PORT environment variable, fallback to 10000
        port = int(os.environ.get('PORT', 10000))
        app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)
    except Exception as e:
        logging.error(f"Web server error: {e}")

def self_ping():
    """Ping own service to prevent sleep - every 3 minutes"""
    # Initial sleep to let the server start
    time.sleep(10)
    
    while True:
        try:
            # Using your actual Render URL
            requests.get('https://aura-farming-pss1.onrender.com/ping', timeout=10)
            print(f"✅ Self-ping successful at {time.strftime('%Y-%m-%d %H:%M:%S')}")
        except Exception as e:
            print(f"❌ Self-ping failed: {e}")
        
        # Sleep for 3 minutes (180 seconds)
        time.sleep(180)

def start_self_ping():
    """Start self-pinging in background thread"""
    ping_thread = threading.Thread(target=self_ping)
    ping_thread.daemon = True
    ping_thread.start()
    print("🔄 Self-ping service started (3-minute intervals)")

def keep_alive():
    # Start web server in a separate thread
    server_thread = threading.Thread(target=start_web_server)
    server_thread.daemon = True
    server_thread.start()
    
    # Start self-pinging to prevent Render from sleeping
    start_self_ping()

# Optional: If you want to run this file directly for testing
if __name__ == "__main__":
    print("🚀 Starting web server and self-ping service...")
    keep_alive()
    
    # Keep the main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("🛑 Server stopped by user")
