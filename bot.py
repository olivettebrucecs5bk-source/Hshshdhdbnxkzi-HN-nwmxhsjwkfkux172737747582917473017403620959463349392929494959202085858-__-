"""
Roblox Presence Tracker Bot — FIXED NO SPAM + STABLE
"""

import asyncio
import aiohttp
import copy
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("RobloxBot")

WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").strip()
PORT = int(os.getenv("PORT", 10000))

TARGET_IDS = [7475107931, 3606074031, 4256847258]
CHECK_TIME = 10
COOLDOWN = 10
DATA_FILE = "data.json"

# ─────────────────────────────────────────
# STATE
# ─────────────────────────────────────────
app = Flask(__name__)

state_lock = threading.Lock()
user_state = {}
last_sent = {}

_main_loop = None

# ─────────────────────────────────────────
# LOAD / SAVE
# ─────────────────────────────────────────
def load_data():
    if not os.path.exists(DATA_FILE):
        return {}, {}
    try:
        with open(DATA_FILE, "r") as f:
            data = json.load(f)
        return data.get("user_state", {}), data.get("last_sent", {})
    except:
        return {}, {}

def save_data():
    with state_lock:
        data = {"user_state": user_state, "last_sent": last_sent}
    try:
        with open(DATA_FILE, "w") as f:
            json.dump(data, f)
    except:
        pass

user_state, last_sent = load_data()

# ─────────────────────────────────────────
# HTTP
# ─────────────────────────────────────────
_session = None

async def get_session():
    global _session
    if _session is None or _session.closed:
        _session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))
    return _session

async def safe_post(url, payload):
    s = await get_session()
    try:
        async with s.post(url, json=payload) as r:
            if r.status == 200:
                return await r.json()
    except:
        pass
    return None

# ─────────────────────────────────────────
# WEBHOOK (ANTI RATE LIMIT)
# ─────────────────────────────────────────
webhook_lock = asyncio.Lock()

async def send_webhook(uid, state, is_test=False):
    if not WEBHOOK_URL:
        log.error("❌ Missing WEBHOOK_URL")
        return

    async with webhook_lock:  # 🔥 CHỐNG SPAM
        label = "🧪 TEST" if is_test else ["OFFLINE ⚫", "ONLINE 🔵", "IN GAME 🟢"][state]

        payload = {
            "content": f"{label} | User: {uid}"
        }

        s = await get_session()

        for _ in range(3):
            try:
                async with s.post(WEBHOOK_URL, json=payload) as r:
                    if r.status == 204 or r.status == 200:
                        log.info("✅ Webhook sent")
                        return
                    elif r.status == 429:
                        log.warning("⚠️ Rate limit → sleep 5s")
                        await asyncio.sleep(5)
            except:
                await asyncio.sleep(2)

# ─────────────────────────────────────────
# MONITOR (NO SPAM)
# ─────────────────────────────────────────
async def monitor():
    log.info("🚀 Monitor started!")

    while True:
        try:
            data = await safe_post(
                "https://presence.roblox.com/v1/presence/users",
                {"userIds": TARGET_IDS}
            )

            if data:
                now = time.time()

                for p in data.get("userPresences", []):
                    uid = str(p["userId"])
                    state = p["userPresenceType"]

                    prev = user_state.get(uid, -1)

                    # ✅ CHỈ GỬI KHI THAY ĐỔI
                    if prev != state and (now - last_sent.get(uid, 0) > COOLDOWN):
                        asyncio.create_task(send_webhook(uid, state))
                        user_state[uid] = state
                        last_sent[uid] = now

                        await asyncio.sleep(1)  # 🔥 tránh spam nhiều user

            save_data()

        except Exception as e:
            log.error(f"Monitor error: {e}")

        await asyncio.sleep(CHECK_TIME)

# ─────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────
async def main_async():
    global _main_loop
    _main_loop = asyncio.get_running_loop()
    asyncio.create_task(monitor())
    await asyncio.Event().wait()

def start_loop():
    asyncio.run(main_async())

threading.Thread(target=start_loop, daemon=True).start()

# ─────────────────────────────────────────
# TEST START
# ─────────────────────────────────────────
def test_webhook_start():
    if not WEBHOOK_URL:
        print("❌ No webhook URL")
        return
    try:
        res = requests.post(WEBHOOK_URL, json={"content": "🚀 BOT START OK"})
        print("TEST:", res.status_code)
    except Exception as e:
        print("TEST ERROR:", e)

test_webhook_start()

# ─────────────────────────────────────────
# FLASK
# ─────────────────────────────────────────
@app.route("/")
def home():
    return "BOT RUNNING OK"

@app.route("/test-webhook")
def test_webhook():
    if _main_loop:
        asyncio.run_coroutine_threadsafe(
            send_webhook(TARGET_IDS[0], 1, is_test=True),
            _main_loop
        )
        return jsonify({"msg": "Sent test webhook!"})
    return jsonify({"msg": "Loop not ready"})

@app.route("/ping")
def ping():
    return jsonify({"ok": True})

# ─────────────────────────────────────────
# RUN
# ─────────────────────────────────────────
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
