"""
Roblox Presence Tracker Bot — Dashboard + Test Webhook Support
==============================================================
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
#  LOGGING & CONFIG
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
#  STATE & FLASK
# ─────────────────────────────────────────
app = Flask(__name__)
state_lock, status_lock = threading.Lock(), threading.Lock()
user_cache, game_cache, user_state, last_sent = {}, {}, {}, {}
bot_status = {"start_time": time.time(), "last_check": time.time(), "errors": 0, "checks_done": 0, "webhooks_sent": 0}

# Lưu trữ event loop để Flask gọi vào
_main_loop = None

# ─────────────────────────────────────────
#  DATA PERSISTENCE
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
#  HTTP SESSION
# ─────────────────────────────────────────
_session = None
async def get_session():
    global _session
    if _session is None or _session.closed: _session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))
    return _session

async def safe_get(url):
    s = await get_session()
    for _ in range(3):
        try:
            async with s.get(url) as r:
                if r.status == 200: return await r.json()
        except: pass
        await asyncio.sleep(2)
    return None

async def safe_post(url, payload):
    s = await get_session()
    for _ in range(3):
        try:
            async with s.post(url, json=payload) as r:
                if r.status == 200: return await r.json()
        except: pass
        await asyncio.sleep(2)
    return None

# ─────────────────────────────────────────
#  ROBLOX & WEBHOOK
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
STATUS_COLOR = {0: 0x95A5A6,    1: 0x3498DB,    2: 0x2ECC71}

async def send_webhook(uid, state, game=None, universe=None, switch=False, is_test=False):
    if not WEBHOOK_URL: return
    label = "🧪 TEST CONNECTION" if is_test else STATUS_LABEL.get(state, "UNKNOWN")
    embed = {
        "title": "🔄 SWITCH GAME" if switch else label,
        "color": 0xFFFFFF if is_test else (0x9B59B6 if switch else STATUS_COLOR.get(state, 0x95A5A6)),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "fields": [{"name": "User ID", "value": str(uid), "inline": True}, {"name": "Trạng thái", "value": label, "inline": True}],
    }
    if is_test: embed["description"] = "Nếu bạn thấy tin nhắn này, Webhook vẫn đang hoạt động tốt! ✅"
    avatar = user_cache.get(str(uid), {}).get("avatar")
    if avatar: embed["thumbnail"] = {"url": avatar}
    if state == 2 and game:
        embed["fields"].append({"name": "Đang chơi", "value": f"**{game}**", "inline": False})
        if universe: embed["fields"].append({"name": "Link Game", "value": f"[Bấm để vào](https://www.roblox.com/games/{universe})", "inline": False})

    s = await get_session()
    for _ in range(3):
        try:
            async with s.post(WEBHOOK_URL, json={"embeds": [embed]}) as r:
                body = await r.text()
                if r.status == 429:
                    try: delay = float(json.loads(body).get("retry_after", 5))
                    except: delay = 5.0
                    await asyncio.sleep(delay); continue
                if r.status in (200, 204):
                    with status_lock: bot_status["webhooks_sent"] += 1
                    log.info(f"✅ Webhook sent (Test={is_test})")
                break
        except: await asyncio.sleep(2)

# ─────────────────────────────────────────
#  LOOPS
# ─────────────────────────────────────────
async def monitor():
    log.info("Preloading avatars...")
    for uid in TARGET_IDS:
        avatar = await fetch_avatar(uid)
        user_cache[str(uid)] = {"avatar": avatar}
    log.info("🚀 Monitor started!")
    while True:
        try:
            data = await safe_post("https://presence.roblox.com/v1/presence/users", {"userIds": TARGET_IDS})
            if data:
                now = time.time()
                for p in data.get("userPresences", []):
                    uid, state, universe = str(p["userId"]), p["userPresenceType"], p.get("universeId")
                    with state_lock:
                        s = user_state.setdefault(uid, {"state": -1, "universe": None})
                        if s["state"] == -1: s["state"], s["universe"] = state, universe; continue
                        
                        is_sw = (state == 2 and s["state"] == 2 and s["universe"] != universe)
                        is_ch = (state != s["state"])
                        if (is_sw or is_ch) and (now - last_sent.get(uid, 0) > COOLDOWN):
                            game = await fetch_game_name(universe) if state == 2 else None
                            asyncio.create_task(send_webhook(uid, state, game, universe, is_sw))
                            s["state"], s["universe"], last_sent[uid] = state, universe, now
                        elif is_ch: s["state"], s["universe"] = state, universe
                save_data()
            with status_lock: bot_status["last_check"], bot_status["checks_done"] = time.time(), bot_status["checks_done"]+1
        except Exception as e: log.error(f"Monitor error: {e}"); bot_status["errors"] += 1
        await asyncio.sleep(CHECK_TIME + random.uniform(0.5, 1.5))

async def main_async():
    global _main_loop
    _main_loop = asyncio.get_running_loop()
    asyncio.create_task(monitor())
    if KEEP_ALIVE and RENDER_URL:
        async def ka():
            while True:
                await asyncio.sleep(600)
                try: 
                    s = await get_session()
                    await s.get(f"{RENDER_URL}/ping")
                except: pass
        asyncio.create_task(ka())
    await asyncio.Event().wait()

def start_loop():
    asyncio.run(main_async())

threading.Thread(target=start_loop, daemon=True).start()

# ─────────────────────────────────────────
#  DASHBOARD HTML
# ─────────────────────────────────────────
DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="vi">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Roblox Tracker</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Rajdhani:wght@400;600;700&display=swap');
  :root { --bg:#0a0e17;--panel:#0f1623;--border:#1e2d45;--accent:#00e5ff;--green:#2ecc71;--red:#e74c3c;--text:#c8d8e8;--muted:#4a6080; }
  *{box-sizing:border-box;margin:0;padding:0}
  body{background:var(--bg);color:var(--text);font-family:'Rajdhani',sans-serif;padding:20px;min-height:100vh}
  .wrap{max-width:900px;margin:0 auto}
  header{display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid var(--border);padding-bottom:15px;margin-bottom:25px}
  h1{font-size:1.5rem;letter-spacing:4px;color:var(--accent);text-transform:uppercase}
  .btn-test{background:transparent;border:1px solid var(--accent);color:var(--accent);padding:8px 15px;font-family:inherit;font-weight:700;cursor:pointer;transition:0.3s;letter-spacing:1px}
  .btn-test:hover{background:var(--accent);color:var(--bg);box-shadow:0 0 15px var(--accent)}
  .btn-test:disabled{opacity:0.5;cursor:wait}
  .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:10px;margin-bottom:25px}
  .card{background:var(--panel);border:1px solid var(--border);padding:15px;border-top:2px solid var(--accent)}
  .card-label{font-size:0.65rem;color:var(--muted);text-transform:uppercase;margin-bottom:5px;letter-spacing:1px}
  .card-value{font-family:'Share Tech Mono',monospace;font-size:1.2rem;color:var(--accent)}
  table{width:100%;border-collapse:collapse;background:var(--panel);border:1px solid var(--border)}
  th{text-align:left;padding:12px;font-size:0.7rem;color:var(--muted);text-transform:uppercase;border-bottom:1px solid var(--border)}
  td{padding:12px;font-family:'Share Tech Mono',monospace;font-size:0.9rem;border-bottom:1px solid rgba(30,45,69,0.5)}
  .badge{padding:2px 8px;font-size:0.7rem;font-weight:700;border:1px solid}
  .b2{color:var(--green);border-color:var(--green);background:rgba(46,204,113,0.1)}
  .b1{color:#3498db;border-color:#3498db;background:rgba(52,152,219,0.1)}
  .b0{color:var(--muted);border-color:var(--muted)}
  #toast{position:fixed;bottom:20px;right:20px;padding:12px 20px;background:var(--panel);border:1px solid var(--accent);display:none;z-index:100}
</style>
</head>
<body>
<div class="wrap">
  <header>
    <h1>Roblox Monitor</h1>
    <button class="btn-test" id="testBtn" onclick="testWebhook()">Test Webhook</button>
  </header>
  <div class="grid">
    <div class="card"><div class="card-label">Uptime</div><div class="card-value">{{ uptime }}</div></div>
    <div class="card"><div class="card-label">Checks</div><div class="card-value">{{ checks }}</div></div>
    <div class="card"><div class="card-label">Webhooks</div><div class="card-value">{{ webhooks }}</div></div>
    <div class="card"><div class="card-label">Errors</div><div class="card-value">{{ errors }}</div></div>
  </div>
  <table>
    <thead><tr><th>User ID</th><th>State</th><th>Universe</th><th>Last Notified</th></tr></thead>
    <tbody>
      {% for r in users %}
      <tr>
        <td>{{ r.uid }}</td>
        <td><span class="badge b{{r.state}}">{% if r.state==2 %}IN GAME{% elif r.state==1 %}ONLINE{% else %}OFFLINE{% endif %}</span></td>
        <td>{{ r.universe or '—' }}</td>
        <td>{{ r.last_sent }}</td>
      </tr>
      {% endfor %}
    </tbody>
  </table>
</div>
<div id="toast"></div>
<script>
  async function testWebhook() {
    const btn = document.getElementById('testBtn');
    const toast = document.getElementById('toast');
    btn.disabled = true;
    toast.style.display = 'block';
    toast.innerText = '⏳ Đang gửi test...';
    try {
      const res = await fetch('/test-webhook', {method:'POST'});
      const data = await res.json();
      toast.innerText = data.msg;
      setTimeout(() => toast.style.display = 'none', 3000);
    } catch(e) { toast.innerText = '❌ Lỗi kết nối'; }
    btn.disabled = false;
  }
  setTimeout(()=>location.reload(), 20000);
</script>
</body>
</html>
"""

@app.route("/")
def home():
    with status_lock: snap = dict(bot_status)
    s = int(time.time() - snap["start_time"])
    ut = f"{s//3600:02d}:{(s%3600)//60:02d}:{s%60:02d}"
    with state_lock:
        users = []
        for uid in [str(i) for i in TARGET_IDS]:
            sd = user_state.get(uid, {})
            ls = last_sent.get(uid, 0)
            users.append({
                "uid": uid, "state": sd.get("state", -1), "universe": sd.get("universe"),
                "last_sent": datetime.fromtimestamp(ls).strftime("%H:%M:%S") if ls else "—"
            })
    return render_template_string(DASHBOARD_HTML, uptime=ut, checks=snap["checks_done"], webhooks=snap["webhooks_sent"], errors=snap["errors"], users=users)

@app.route("/test-webhook", methods=["POST"])
def test_webhook_route():
    if _main_loop and _main_loop.is_running():
        # Gọi hàm async từ Flask thread
        asyncio.run_coroutine_threadsafe(send_webhook(TARGET_IDS[0], 1, is_test=True), _main_loop)
        return jsonify({"ok": True, "msg": "✅ Đã gửi lệnh Test!"})
    return jsonify({"ok": False, "msg": "❌ Bot chưa sẵn sàng"})

@app.route("/ping")
def ping(): return jsonify({"ok": True})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
  
