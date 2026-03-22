"""
Roblox Presence Tracker Bot — Optimized for Render + Gunicorn
==============================================================
Env vars required:
  WEBHOOK_URL      — Discord Webhook URL
  PORT             — (auto-set by Render, default 10000)
  RENDER_KEEP_ALIVE— Set to "true" để bot tự ping tránh sleep (Render free tier)
  RENDER_URL       — URL của app (vd: https://yourapp.onrender.com)
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

from flask import Flask, jsonify, render_template_string

# ─────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("RobloxBot")

# ─────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────
WEBHOOK_URL   = os.getenv("WEBHOOK_URL", "").strip()
RENDER_URL    = os.getenv("RENDER_URL", "")
KEEP_ALIVE    = os.getenv("RENDER_KEEP_ALIVE", "false").lower() == "true"
PORT          = int(os.getenv("PORT", 10000))

TARGET_IDS: list[int] = [
    7475107931,
    3606074031,
    4256847258,
]

CHECK_TIME  = 10
COOLDOWN    = 5
SAVE_EVERY  = 30
DATA_FILE   = "data.json"

# ─────────────────────────────────────────
#  FLASK
# ─────────────────────────────────────────
app = Flask(__name__)

state_lock  = threading.Lock()
status_lock = threading.Lock()

# ─────────────────────────────────────────
#  SHARED STATE
# ─────────────────────────────────────────
user_cache: dict = {}
game_cache: dict = {}
user_state: dict = {}
last_sent:  dict = {}

bot_status: dict = {
    "start_time":    time.time(),
    "last_check":    time.time(),
    "errors":        0,
    "checks_done":   0,
    "webhooks_sent": 0,
}

# ─────────────────────────────────────────
#  DATA PERSISTENCE
# ─────────────────────────────────────────
def load_data() -> tuple[dict, dict]:
    if not os.path.exists(DATA_FILE):
        return {}, {}
    try:
        with open(DATA_FILE, "r") as f:
            raw = json.load(f)
        return raw.get("user_state", {}), raw.get("last_sent", {})
    except Exception as e:
        log.error(f"load_data error: {e}")
        return {}, {}


def save_data() -> None:
    with state_lock:
        snapshot = copy.deepcopy({"user_state": user_state, "last_sent": last_sent})
    try:
        with open(DATA_FILE, "w") as f:
            json.dump(snapshot, f, indent=2)
    except Exception as e:
        log.error(f"save_data error: {e}")


user_state, last_sent = load_data()

# ─────────────────────────────────────────
#  SESSION
# ─────────────────────────────────────────
_session: aiohttp.ClientSession | None = None
TIMEOUT = aiohttp.ClientTimeout(total=15)

async def get_session() -> aiohttp.ClientSession:
    global _session
    if _session is None or _session.closed:
        _session = aiohttp.ClientSession(timeout=TIMEOUT)
        log.info("🔁 HTTP session created.")
    return _session

# ─────────────────────────────────────────
#  SAFE HTTP
# ─────────────────────────────────────────
async def safe_get(url: str) -> dict | None:
    s = await get_session()
    for attempt in range(3):
        try:
            async with s.get(url) as r:
                if r.status == 200:
                    return await r.json()
                log.warning(f"GET {url} → {r.status}")
        except Exception as e:
            log.warning(f"safe_get attempt {attempt+1}: {e}")
        await asyncio.sleep(2)
    return None


async def safe_post(url: str, payload: dict) -> dict | None:
    s = await get_session()
    for attempt in range(3):
        try:
            async with s.post(url, json=payload) as r:
                if r.status == 200:
                    return await r.json()
                log.warning(f"POST {url} → {r.status}")
        except Exception as e:
            log.warning(f"safe_post attempt {attempt+1}: {e}")
        await asyncio.sleep(2)
    return None

# ─────────────────────────────────────────
#  ROBLOX HELPERS
# ─────────────────────────────────────────
async def fetch_avatar(uid: int) -> str:
    data = await safe_get(
        f"https://thumbnails.roblox.com/v1/users/avatar-headshot"
        f"?userIds={uid}&size=150x150&format=Png&isCircular=false"
    )
    try:
        return data["data"][0]["imageUrl"]
    except Exception:
        return ""


async def fetch_game_name(universe_id: int | None) -> str:
    if not universe_id:
        return "Unknown"
    if universe_id in game_cache:
        return game_cache[universe_id]
    data = await safe_get(f"https://games.roblox.com/v1/games?universeIds={universe_id}")
    try:
        name = data["data"][0]["name"]
        game_cache[universe_id] = name
        return name
    except Exception:
        return "Unknown"

# ─────────────────────────────────────────
#  WEBHOOK
# ─────────────────────────────────────────
STATUS_LABEL = {0: "OFFLINE ⚫", 1: "ONLINE 🔵", 2: "IN GAME 🟢"}
STATUS_COLOR = {0: 0x95A5A6,    1: 0x3498DB,    2: 0x2ECC71}

async def send_webhook(
    uid:      str,
    state:    int,
    game:     str | None = None,
    universe: int | None = None,
    switch:   bool       = False,
) -> None:
    if not WEBHOOK_URL:
        log.error("WEBHOOK_URL chưa set!")
        return

    label = STATUS_LABEL.get(state, "UNKNOWN")
    embed: dict = {
        "title":     "🔄 SWITCH GAME" if switch else label,
        "color":     0x9B59B6 if switch else STATUS_COLOR.get(state, 0x95A5A6),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "fields": [
            {"name": "User ID",    "value": uid,   "inline": True},
            {"name": "Trạng thái", "value": label, "inline": True},
        ],
    }

    avatar = user_cache.get(uid, {}).get("avatar", "")
    if avatar:
        embed["thumbnail"] = {"url": avatar}

    if state == 2:
        if game:
            embed["fields"].append({"name": "Đang chơi", "value": f"**{game}**", "inline": False})
        if universe:
            embed["fields"].append({
                "name":   "Link Game",
                "value":  f"[Bấm để vào](https://www.roblox.com/games/{universe})",
                "inline": False,
            })

    s = await get_session()
    for attempt in range(3):
        try:
            async with s.post(WEBHOOK_URL, json={"embeds": [embed]}) as r:
                if r.status == 429:
                    retry = (await r.json()).get("retry_after", 2)
                    log.warning(f"Rate limited → retry in {retry}s")
                    await asyncio.sleep(retry)
                    continue
                if r.status not in (200, 204):
                    log.error(f"Webhook failed: {r.status}")
                else:
                    with status_lock:
                        bot_status["webhooks_sent"] += 1
                    log.info(f"✅ Webhook → uid={uid} state={state} switch={switch}")
                break
        except Exception as e:
            log.error(f"Webhook error attempt {attempt+1}: {e}")
            await asyncio.sleep(2)

# ─────────────────────────────────────────
#  CORE MONITOR
# ─────────────────────────────────────────
async def monitor() -> None:
    log.info("Preloading avatars...")
    for uid in TARGET_IDS:
        avatar = await fetch_avatar(uid)
        user_cache[str(uid)] = {"avatar": avatar}
        log.info(f"  ✔ {uid}")

    log.info("🚀 Bot monitoring started!")

    while True:
        try:
            data = await safe_post(
                "https://presence.roblox.com/v1/presence/users",
                {"userIds": TARGET_IDS},
            )

            if not data:
                log.warning("Presence API returned no data, skipping.")
                await asyncio.sleep(10)
                continue

            events: list[dict] = []
            now = time.time()

            for p in data.get("userPresences", []):
                uid      = str(p["userId"])
                state    = p["userPresenceType"]
                universe = p.get("universeId")

                with state_lock:
                    sdata = user_state.setdefault(uid, {"state": -1, "universe": None})
                    in_cooldown = (now - last_sent.get(uid, 0)) < COOLDOWN

                    is_switch = (
                        state == 2
                        and sdata["state"] == 2
                        and sdata["universe"] != universe
                    )
                    is_change = (state != sdata["state"])

                    if (is_switch or is_change) and not in_cooldown:
                        events.append({
                            "uid":      uid,
                            "state":    state,
                            "universe": universe,
                            "switch":   is_switch,
                        })
                        sdata["state"]    = state
                        sdata["universe"] = universe
                        last_sent[uid]    = now
                    elif is_change:
                        sdata["state"]    = state
                        sdata["universe"] = universe

            for ev in events:
                game = await fetch_game_name(ev["universe"]) if ev["state"] == 2 else None
                asyncio.create_task(
                    send_webhook(ev["uid"], ev["state"], game, ev["universe"], ev["switch"])
                )

            if events:
                save_data()

            with status_lock:
                bot_status["last_check"]  = time.time()
                bot_status["checks_done"] += 1

            await asyncio.sleep(CHECK_TIME + random.uniform(0.5, 1.5))

        except Exception as e:
            log.error(f"Monitor error: {e}", exc_info=True)
            with status_lock:
                bot_status["errors"] += 1
            await asyncio.sleep(5)

# ─────────────────────────────────────────
#  AUTO-SAVE
# ─────────────────────────────────────────
async def auto_save_loop() -> None:
    while True:
        await asyncio.sleep(SAVE_EVERY)
        save_data()

# ─────────────────────────────────────────
#  KEEP-ALIVE
# ─────────────────────────────────────────
async def keep_alive_loop() -> None:
    if not KEEP_ALIVE or not RENDER_URL:
        log.info("Keep-alive disabled.")
        return
    log.info(f"Keep-alive → pinging {RENDER_URL}/ping every 10 min")
    while True:
        await asyncio.sleep(600)
        try:
            s = await get_session()
            async with s.get(f"{RENDER_URL}/ping") as r:
                log.debug(f"Keep-alive: {r.status}")
        except Exception as e:
            log.warning(f"Keep-alive failed: {e}")

# ─────────────────────────────────────────
#  ASYNCIO ENTRY
# ─────────────────────────────────────────
async def main() -> None:
    asyncio.create_task(monitor())
    asyncio.create_task(auto_save_loop())
    asyncio.create_task(keep_alive_loop())
    await asyncio.Event().wait()


def run_asyncio() -> None:
    asyncio.run(main())


# ─────────────────────────────────────────
#  KHỞI ĐỘNG ASYNCIO Ở MODULE LEVEL
#  → Chạy đúng cả khi dùng gunicorn lẫn python bot.py
# ─────────────────────────────────────────
_bot_thread = threading.Thread(target=run_asyncio, daemon=True, name="AsyncioLoop")
_bot_thread.start()
log.info("✅ AsyncioLoop started.")

# ─────────────────────────────────────────
#  DASHBOARD
# ─────────────────────────────────────────
DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="vi">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Roblox Tracker</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Rajdhani:wght@400;600;700&display=swap');
  :root {
    --bg:#0a0e17;--panel:#0f1623;--border:#1e2d45;
    --accent:#00e5ff;--green:#2ecc71;--red:#e74c3c;
    --blue:#3498db;--text:#c8d8e8;--muted:#4a6080;
  }
  *{box-sizing:border-box;margin:0;padding:0}
  body{background:var(--bg);color:var(--text);font-family:'Rajdhani',sans-serif;font-size:16px;min-height:100vh;padding:32px 24px}
  body::before{content:'';position:fixed;inset:0;background:repeating-linear-gradient(0deg,transparent,transparent 39px,var(--border) 40px),repeating-linear-gradient(90deg,transparent,transparent 39px,var(--border) 40px);opacity:.1;pointer-events:none;z-index:0}
  .wrap{max-width:860px;margin:0 auto;position:relative;z-index:1}
  header{display:flex;align-items:center;gap:14px;border-bottom:1px solid var(--border);padding-bottom:20px;margin-bottom:28px}
  .dot{width:11px;height:11px;border-radius:50%;background:var(--green);box-shadow:0 0 10px var(--green);animation:pulse 1.8s ease-in-out infinite}
  @keyframes pulse{0%,100%{opacity:1;transform:scale(1)}50%{opacity:.4;transform:scale(.75)}}
  h1{font-size:1.7rem;font-weight:700;letter-spacing:5px;text-transform:uppercase;color:var(--accent);text-shadow:0 0 24px rgba(0,229,255,.35)}
  .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(155px,1fr));gap:14px;margin-bottom:28px}
  .card{background:var(--panel);border:1px solid var(--border);border-top:2px solid var(--accent);padding:18px 20px}
  .card-label{font-size:.7rem;letter-spacing:2px;text-transform:uppercase;color:var(--muted);margin-bottom:6px}
  .card-value{font-family:'Share Tech Mono',monospace;font-size:1.45rem;color:var(--accent)}
  .green{color:var(--green)!important;text-shadow:0 0 12px var(--green)}
  .red{color:var(--red)!important;text-shadow:0 0 12px var(--red)}
  .sec{font-size:.7rem;letter-spacing:3px;text-transform:uppercase;color:var(--muted);margin-bottom:10px}
  table{width:100%;border-collapse:collapse;background:var(--panel);border:1px solid var(--border)}
  th{text-align:left;padding:10px 16px;font-size:.67rem;letter-spacing:2px;text-transform:uppercase;color:var(--muted);border-bottom:1px solid var(--border)}
  td{padding:12px 16px;font-family:'Share Tech Mono',monospace;font-size:.87rem;border-bottom:1px solid rgba(30,45,69,.5)}
  tr:last-child td{border-bottom:none}
  .badge{display:inline-block;padding:2px 10px;font-size:.7rem;font-weight:600;letter-spacing:1px}
  .b2{background:rgba(46,204,113,.12);color:var(--green);border:1px solid var(--green)}
  .b1{background:rgba(52,152,219,.12);color:var(--blue);border:1px solid var(--blue)}
  .b0{background:rgba(74,96,128,.12);color:var(--muted);border:1px solid var(--muted)}
  .bx{background:rgba(231,76,60,.12);color:var(--red);border:1px solid var(--red)}
  footer{margin-top:28px;text-align:right;font-size:.67rem;color:var(--muted);letter-spacing:1px}
</style>
</head>
<body>
<div class="wrap">
  <header><div class="dot"></div><h1>Roblox Tracker</h1></header>
  <div class="grid">
    <div class="card"><div class="card-label">Status</div><div class="card-value green">ONLINE</div></div>
    <div class="card"><div class="card-label">Uptime</div><div class="card-value">{{ uptime }}</div></div>
    <div class="card"><div class="card-label">Checks</div><div class="card-value">{{ checks }}</div></div>
    <div class="card"><div class="card-label">Webhooks</div><div class="card-value">{{ webhooks }}</div></div>
    <div class="card"><div class="card-label">Errors</div><div class="card-value {% if errors > 0 %}red{% endif %}">{{ errors }}</div></div>
    <div class="card"><div class="card-label">Last Check</div><div class="card-value" style="font-size:1rem">{{ last_check }}</div></div>
  </div>
  <div class="sec">Tracked Users</div>
  <table>
    <thead><tr><th>User ID</th><th>State</th><th>Universe</th><th>Last Notified</th></tr></thead>
    <tbody>
      {% for r in users %}
      <tr>
        <td>{{ r.uid }}</td>
        <td>
          {% if r.state == 2 %}<span class="badge b2">IN GAME</span>
          {% elif r.state == 1 %}<span class="badge b1">ONLINE</span>
          {% elif r.state == 0 %}<span class="badge b0">OFFLINE</span>
          {% else %}<span class="badge bx">UNKNOWN</span>{% endif %}
        </td>
        <td>{{ r.universe or '—' }}</td>
        <td>{{ r.last_sent }}</td>
      </tr>
      {% endfor %}
    </tbody>
  </table>
  <footer>AUTO-REFRESH 15s &nbsp;|&nbsp; ROBLOX PRESENCE TRACKER</footer>
</div>
<script>setTimeout(()=>location.reload(),15000);</script>
</body>
</html>
"""

@app.route("/")
def home():
    with status_lock:
        snap = dict(bot_status)
    s  = int(time.time() - snap["start_time"])
    ut = f"{s//3600:02d}:{(s%3600)//60:02d}:{s%60:02d}"
    with state_lock:
        users = []
        for uid in [str(i) for i in TARGET_IDS]:
            sd = user_state.get(uid, {})
            ls = last_sent.get(uid, 0)
            users.append({
                "uid":      uid,
                "state":    sd.get("state", -1),
                "universe": sd.get("universe"),
                "last_sent": datetime.fromtimestamp(ls).strftime("%H:%M:%S") if ls else "—",
            })
    return render_template_string(
        DASHBOARD_HTML,
        uptime=ut, checks=snap["checks_done"],
        webhooks=snap["webhooks_sent"], errors=snap["errors"],
        last_check=datetime.fromtimestamp(snap["last_check"]).strftime("%H:%M:%S"),
        users=users,
    )


@app.route("/status")
def api_status():
    with status_lock:
        snap = dict(bot_status)
    snap["uptime_seconds"] = round(time.time() - snap["start_time"], 1)
    return jsonify(snap)


@app.route("/ping")
def ping():
    return jsonify({"ok": True, "ts": time.time()})


# ─────────────────────────────────────────
#  CHẠY TRỰC TIẾP (python bot.py)
# ─────────────────────────────────────────
if __name__ == "__main__":
    log.info(f"🌐 Flask starting on port {PORT}")
    app.run(host="0.0.0.0", port=PORT, threaded=True)
