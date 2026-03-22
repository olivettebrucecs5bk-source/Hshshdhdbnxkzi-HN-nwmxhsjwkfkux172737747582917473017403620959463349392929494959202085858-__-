"""
Roblox Presence Tracker Bot — FULL FIX + DEBUG + TEST READY
"""

import asyncio
import aiohttp
import copy
import json
import logging
import os
import random
import threading
import time
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify, render_template_string

# ─────────────────────────────────────────
# LOGGING & CONFIG
# ─────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("RobloxBot")

WEBHOOK_URL   = os.getenv("WEBHOOK_URL", "").strip()
RENDER_URL    = os.getenv("RENDER_URL", "")
KEEP_ALIVE    = os.getenv("RENDER_KEEP_ALIVE", "false").lower() == "true"
PORT          = int(os.getenv("PORT", 10000))

TARGET_IDS = [7475107931, 3606074031, 4256847258]
CHECK_TIME, COOLDOWN, SAVE_EVERY, DATA_FILE = 10, 5, 30, "data.json"

# ─────────────────────────────────────────
# STATE & FLASK
# ─────────────────────────────────────────
app = Flask(__name__)
state_lock, status_lock = threading.Lock(), threading.Lock()
user_cache, game_cache, user_state, last_sent = {}, {}, {}, {}
bot_status = {"start_time": time.time(), "last_check": time.time(), "errors": 0, "checks_done": 0, "webhooks_sent": 0}

_main_loop = None

# ─────────────────────────────────────────
# DATA
# ─────────────────────────────────────────
def load_data():
    if not os.path.exists(DATA_FILE): return {}, {}
    try:
        with open(DATA_FILE, "r") as f: raw = json.load(f)
        return raw.get("user_state", {}), raw.get("last_sent", {})
    except: return {}, {}

def save_data():
    with state_lock: snapshot = copy.deepcopy({"user_state": user_state, "last_sent": last_sent})
    try:
        with open(DATA_FILE, "w") as f: json.dump(snapshot, f, indent=2)
    except Exception as e: log.error(f"save_data error: {e}")

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

async def safe_get(url):
    s = await get_session()
    for _ in range(3):
        try:
            async with s.get(url) as r:
                if r.status == 200:
                    return await r.json()
        except:
            pass
        await asyncio.sleep(2)
    return None

async def safe_post(url, payload):
    s = await get_session()
    for _ in range(3):
        try:
            async with s.post(url, json=payload) as r:
                if r.status == 200:
                    return await r.json()
        except:
            pass
        await asyncio.sleep(2)
    return None

# ─────────────────────────────────────────
# ROBLOX
# ─────────────────────────────────────────
async def fetch_avatar(uid):
    data = await safe_get(f"https://thumbnails.roblox.com/v1/users/avatar-headshot?userIds={uid}&size=150x150&format=Png")
    return data["data"][0]["imageUrl"] if data else ""

async def fetch_game_name(un_id):
    if not un_id: return "Unknown"
    if un_id in game_cache: return game_cache[un_id]
    data = await safe_get(f"https://games.roblox.com/v1/games?universeIds={un_id}")
    if data:
        name = data["data"][0]["name"]
        game_cache[un_id] = name
        return name
    return "Unknown"

STATUS_LABEL = {0: "OFFLINE ⚫", 1: "ONLINE 🔵", 2: "IN GAME 🟢"}
STATUS_COLOR = {0: 0x95A5A6, 1: 0x3498DB, 2: 0x2ECC71}

# ─────────────────────────────────────────
# WEBHOOK (FIX FULL)
# ─────────────────────────────────────────
async def send_webhook(uid, state, game=None, universe=None, switch=False, is_test=False):
    if not WEBHOOK_URL:
        log.error("❌ WEBHOOK_URL trống!")
        return

    label = "🧪 TEST CONNECTION" if is_test else STATUS_LABEL.get(state, "UNKNOWN")

    embed = {
        "title": "🔄 SWITCH GAME" if switch else label,
        "color": 0xFFFFFF if is_test else (0x9B59B6 if switch else STATUS_COLOR.get(state, 0x95A5A6)),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "fields": [
            {"name": "User ID", "value": str(uid), "inline": True},
            {"name": "Trạng thái", "value": label, "inline": True}
        ],
    }

    if is_test:
        embed["description"] = "Nếu thấy tin này → webhook OK ✅"

    avatar = user_cache.get(str(uid), {}).get("avatar")
    if avatar:
        embed["thumbnail"] = {"url": avatar}

    s = await get_session()

    for attempt in range(3):
        try:
            log.info(f"📡 Sending webhook {attempt+1}")

            async with s.post(WEBHOOK_URL, json={"embeds": [embed]}) as r:
                body = await r.text()

                log.info(f"📨 Status: {r.status}")
                log.info(f"📨 Body: {body[:200]}")

                if r.status == 429:
                    try:
                        delay = float(json.loads(body).get("retry_after", 5))
                    except:
                        delay = 5.0
                    log.warning(f"⚠️ Rate limit {delay}s")
                    await asyncio.sleep(delay)
                    continue

                if r.status not in (200, 204):
                    log.error(f"❌ Webhook failed: {r.status}")
                else:
                    with status_lock:
                        bot_status["webhooks_sent"] += 1
                    log.info("✅ Webhook OK")
                    return

        except Exception as e:
            log.error(f"💥 Webhook error: {e}")
            await asyncio.sleep(2)

# ─────────────────────────────────────────
# MONITOR
# ─────────────────────────────────────────
async def monitor():
    log.info("🚀 Monitor started!")
    while True:
        try:
            data = await safe_post("https://presence.roblox.com/v1/presence/users", {"userIds": TARGET_IDS})
            if data:
                for p in data.get("userPresences", []):
                    uid = str(p["userId"])
                    state = p["userPresenceType"]

                    # 🔥 FORCE TEST (đảm bảo webhook chạy)
                    asyncio.create_task(send_webhook(uid, state, is_test=True))

            with status_lock:
                bot_status["checks_done"] += 1

        except Exception as e:
            log.error(f"Monitor error: {e}")

        await asyncio.sleep(15)

# ─────────────────────────────────────────
# MAIN
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
# TEST WHEN START
# ─────────────────────────────────────────
def test_webhook_start():
    if not WEBHOOK_URL:
        print("❌ No webhook URL")
        return
    try:
        res = requests.post(WEBHOOK_URL, json={"content": "🚀 BOT START TEST"})
        print("TEST:", res.status_code, res.text)
    except Exception as e:
        print("TEST ERROR:", e)

test_webhook_start()

# ─────────────────────────────────────────
# FLASK
# ─────────────────────────────────────────
@app.route("/")
def home():
    return "BOT RUNNING OK"

@app.route("/test-webhook", methods=["POST"])
def test_webhook_route():
    if _main_loop:
        asyncio.run_coroutine_threadsafe(send_webhook(TARGET_IDS[0], 1, is_test=True), _main_loop)
        return jsonify({"msg": "Sent!"})
    return jsonify({"msg": "Loop chưa chạy"})

@app.route("/ping")
def ping():
    return jsonify({"ok": True})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
