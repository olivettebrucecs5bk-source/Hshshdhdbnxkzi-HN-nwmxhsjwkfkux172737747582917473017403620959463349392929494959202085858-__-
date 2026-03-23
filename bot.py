import asyncio, aiohttp, os, time, logging
from datetime import datetime, timezone
from collections import deque
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from tinydb import TinyDB
import uvicorn

# ================= CONFIG =================

logging.basicConfig(level=logging.INFO)
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")
PORT = int(os.getenv("PORT", 10000))
TARGET_IDS = [7475107931, 3606074031, 4256847258]
CHECK_INTERVAL = 10 
MAX_LOGS = 500
MAX_QUEUE = 100

# ================= STATE =================

db = TinyDB('data.json')
user_state = {}
activity_log = deque(maxlen=MAX_LOGS)
stats = {"checks": 0, "events": 0, "start": time.time()}
system_status = {"online_users": 0}
discord_queue = asyncio.Queue(maxsize=MAX_QUEUE)

# ================= UTILS (ROBLOX ASSETS) =================

async def get_roblox_thumb(session, universe_id):
    if not universe_id: return None
    try:
        url = f"https://thumbnails.roblox.com/v1/games/multiget/thumbnails?universeIds={universe_id}&countPerUniverse=1&sortOrder=Asc&size=768x432&format=Png"
        async with session.get(url, timeout=5) as r:
            if r.status == 200:
                data = await r.json()
                thumbs = data.get("data")
                if thumbs and thumbs[0].get("thumbnails"):
                    return thumbs[0]["thumbnails"][0].get("imageUrl")
    except: return None
    return None

# ================= WEBSOCKET MANAGER =================

class WSManager:
    def __init__(self): 
        self.clients = set()
    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.clients.add(ws)
        await ws.send_json({"type": "init", "stats": stats, "system": system_status, "logs": list(activity_log)})
    def disconnect(self, ws: WebSocket): 
        self.clients.discard(ws)  
    async def broadcast(self, data):  
        if not self.clients: return  
        clients = list(self.clients)  
        tasks = [ws.send_json(data) for ws in clients]  
        results = await asyncio.gather(*tasks, return_exceptions=True)  
        for i, res in enumerate(results):  
            if isinstance(res, Exception):  
                self.clients.discard(clients[i])

ws_manager = WSManager()

# ================= DISCORD WORKER =================

async def discord_worker(session):
    while True:
        try:
            try:
                embed = await asyncio.wait_for(discord_queue.get(), timeout=30)
            except asyncio.TimeoutError:
                continue
            if WEBHOOK_URL:
                async with session.post(WEBHOOK_URL, json={"embeds": [embed]}, timeout=10) as r:
                    await r.text()
            discord_queue.task_done()
        except Exception as e: 
            logging.error(f"Discord worker error: {e}")
        await asyncio.sleep(0.5)

# ================= MONITOR CORE =================

async def monitor(session):
    while True:
        try:
            async with session.post(
                "https://presence.roblox.com/v1/presence/users",
                json={"userIds": TARGET_IDS}, timeout=10
            ) as r:
                if r.status != 200:
                    await asyncio.sleep(CHECK_INTERVAL)
                    continue
                try: data = await r.json()  
                except: continue
                
                stats["checks"] += 1  
                presences = data.get("userPresences") or []  
                system_status["online_users"] = sum(1 for p in presences if p.get("userPresenceType") == 2)  

                for p in presences:  
                    uid = str(p["userId"])  
                    state, universe, game = p["userPresenceType"], p.get("universeId"), p.get("lastLocation") or "N/A"  
                    if uid not in user_state:  
                        user_state[uid] = {"state": state, "universe": universe, "start_time": time.time()}  
                        continue  

                    prev = user_state[uid]  
                    event = None  
                    if state == 2 and prev["state"] != 2: event = "JOIN"  
                    elif state == 0 and prev["state"] != 0: event = "OFFLINE"  
                    elif state == 1 and prev["state"] != 1: event = "ONLINE"  
                    elif state == 2 and prev["state"] == 2 and universe != prev.get("universe"): event = "SWITCH"  

                    if not event: continue

                    tags = []  
                    if state == 2:  
                        duration = time.time() - prev["start_time"]  
                        if duration > 7200: tags.append("⛏️ HARD GRIND")  
                        elif duration > 3600: tags.append("😴 AFK?")  

                    now = datetime.now().astimezone().strftime("%H:%M:%S")  
                    tag_str = f" ({' | '.join(tags)})" if tags else ""  
                    
                    # Log message với icon
                    icons = {"JOIN": "📥", "OFFLINE": "❌", "ONLINE": "👤", "SWITCH": "🔄"}
                    msg = f"{icons.get(event, '🔔')} {uid}: {event} | {game}{tag_str}"
                    
                    log_e = {"time": now, "msg": msg, "type": event, "uid": uid}  
                    activity_log.appendleft(log_e)  
                    stats["events"] += 1  
                    if len(db) >= MAX_LOGS: db.truncate()  
                    db.insert(log_e)  

                    await ws_manager.broadcast({"type": "update", "stats": stats, "system": system_status, "new_log": log_e})  

                    # Discord Embed logic giữ nguyên...
                    colors = {"JOIN": 0x3fb950, "OFFLINE": 0xf85149, "ONLINE": 0x58a6ff, "SWITCH": 0xd29922}
                    embed = {"title": f"👁️ Mắt Thần - {event}", "color": colors.get(event, 0x58a6ff), "timestamp": datetime.now(timezone.utc).isoformat()}
                    try: discord_queue.put_nowait(embed)
                    except: pass

                    user_state[uid] = {"state": state, "universe": universe, "start_time": prev["start_time"] if state == 2 and prev["state"] == 2 else time.time()}  
        except Exception as e: 
            logging.error(f"Monitor error: {e}")
            await asyncio.sleep(5)  
        await asyncio.sleep(CHECK_INTERVAL)

# ================= FASTAPI & UI =================

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with aiohttp.ClientSession() as session:
        asyncio.create_task(discord_worker(session))
        asyncio.create_task(monitor(session))
        yield

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def home():
    return HTMLResponse("""
<!DOCTYPE html>  <html lang="vi">  
<head>  
    <meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">  
    <title>MẮT THẦN V10 - PREMIUM CUSTOM</title>  
    <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;600;700&display=swap" rel="stylesheet">  
    <style>  
        :root { --bg: #04070a; --glass: rgba(22, 27, 34, 0.9); --b: rgba(48, 54, 61, 0.5); }  
        body { background: var(--bg); color: #c9d1d9; font-family: 'Plus Jakarta Sans', sans-serif; margin: 0; padding: 15px; height: 100vh; display: flex; flex-direction: column; overflow: hidden; }  
        .glass { background: var(--glass); backdrop-filter: blur(15px); border: 1px solid var(--b); border-radius: 12px; margin-bottom: 12px; }  
        .header { padding: 12px 20px; display: flex; justify-content: space-between; align-items: center; border-bottom: 2px solid #58a6ff; }  
        .stats-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; margin-bottom: 12px; }  
        .stat-card { padding: 15px; text-align: center; }  
        .stat-card div { font-size: 24px; font-weight: 700; color: #58a6ff; }
        .stat-card small { display: flex; align-items: center; justify-content: center; gap: 5px; opacity: 0.8; }
        #log { flex: 1; overflow-y: auto; padding: 12px; display: flex; flex-direction: column; gap: 8px; }  
        
        /* Màu riêng cho từng User */
        .u-7475107931 { border-left: 4px solid #111 !important; background: rgba(0,0,0,0.4) !important; color: #888; }
        .u-3606074031 { 
            border: 1px solid #00f2ff !important; 
            border-left: 5px solid #00f2ff !important; 
            background: rgba(0, 242, 255, 0.05) !important; 
            box-shadow: 0 0 15px rgba(0, 242, 255, 0.2); 
            color: #00f2ff;
            font-weight: 600;
        }
        .u-4256847258 { border-left: 4px solid #a371f7 !important; background: rgba(163, 113, 247, 0.05) !important; }

        .line { padding: 10px 15px; border-radius: 8px; background: rgba(255,255,255,0.03); font-size: 13px; display: flex; justify-content: space-between; align-items: center; border: 1px solid transparent; }
        .line span:first-child { opacity: 0.5; font-family: monospace; font-size: 11px; }
        
        .st-on { color: #3fb950; } .st-off { color: #f85149; } .st-wait { color: #d29922; }
    </style>  
</head>  
<body>  
    <div class="header glass">
        <div style="font-weight:700; font-size:1.1rem">👁️ MẮT THẦN <span style="color:#58a6ff">V10.0</span></div>
        <div id="st" class="st-wait">🟡 CONNECTING</div>
    </div>  
    <div class="stats-grid">  
        <div class="stat-card glass"><small>👁️ Scans</small><div id="ck">0</div></div>  
        <div class="stat-card glass"><small>🎮 In-Game</small><div id="on" style="color:#3fb950">0</div></div>  
        <div class="stat-card glass"><small>⚡ Events</small><div id="ev">0</div></div>  
    </div>  
    <div id="log" class="glass"></div>  
    <audio id="snd" src="https://assets.mixkit.co/active_storage/sfx/2358/2358-preview.mp3"></audio>  
    <script>  
        const ck=document.getElementById('ck'), ev=document.getElementById('ev'), on=document.getElementById('on'), log=document.getElementById('log'), st=document.getElementById('st'), snd=document.getElementById('snd');  
        let ws;  
        function connect() {  
            ws = new WebSocket((location.protocol==='https:'?'wss':'ws')+'://'+location.host+'/ws');  
            ws.onopen = () => { st.innerHTML='🟢 ONLINE'; st.className='st-on'; };  
            ws.onclose = () => { st.innerHTML='🔴 OFFLINE'; st.className='st-off'; setTimeout(connect, 3000); };  
            ws.onmessage = (e) => {  
                let d = JSON.parse(e.data);  
                if(d.type==='init' || d.type==='update') {   
                    ck.innerText=d.stats.checks; ev.innerText=d.stats.events; on.innerText=d.system.online_users;  
                    if(d.new_log) { add(d.new_log, true); if(['JOIN','SWITCH'].includes(d.new_log.type)) snd.play().catch(()=>{}); }  
                    if(d.logs && d.type==='init') d.logs.reverse().forEach(l => add(l));  
                }  
            };  
        }  
        function add(l, isNew=false) {  
            let div = document.createElement('div'); 
            div.className = `line u-${l.uid}`;  
            div.innerHTML = `<span>[${l.time}]</span> <span>${l.msg}</span>`;  
            if(isNew) { log.prepend(div); } else { log.appendChild(div); }  
        }  
        connect();  
    </script>  
</body>  
</html>  
""")  

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws_manager.connect(ws)
    try:
        while True: 
            try: await asyncio.wait_for(ws.receive_text(), timeout=60)
            except asyncio.TimeoutError: continue 
    except WebSocketDisconnect: ws_manager.disconnect(ws)
    except Exception as e: ws_manager.disconnect(ws)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
          
