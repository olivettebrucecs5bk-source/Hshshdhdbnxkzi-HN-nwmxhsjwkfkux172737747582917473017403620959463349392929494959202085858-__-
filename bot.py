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
                # 🔧 FIX 4: Tránh index lỗi khi list rỗng
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
            # 🔧 FIX 2: Thêm timeout để worker không bị treo ngầm
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

                # 🔧 FIX 3: Check response JSON an toàn
                try:
                    data = await r.json()  
                except:
                    continue
                    
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

                    # 🔧 FIX 5: Bỏ qua nếu không có event để nhẹ loop
                    if not event:
                        continue

                    # 🧠 AI Detect
                    tags = []  
                    if state == 2:  
                        duration = time.time() - prev["start_time"]  
                        if duration > 7200: tags.append("⛏️ HARD GRIND")  
                        elif duration > 3600: tags.append("😴 AFK?")  

                    now = datetime.now().astimezone().strftime("%H:%M:%S")  
                    tag_str = f" ({' | '.join(tags)})" if tags else ""  
                    msg = f"User {uid}: {event} | {game}{tag_str}"  
                    log_e = {"time": now, "msg": msg, "type": event}  
                      
                    activity_log.appendleft(log_e)  
                    stats["events"] += 1  
                      
                    # 🔧 FIX 1: Tối ưu kiểm tra DB (không dùng .all())
                    if len(db) >= MAX_LOGS: db.truncate()  
                    db.insert(log_e)  

                    await ws_manager.broadcast({"type": "update", "stats": stats, "system": system_status, "new_log": log_e})  

                    # --- RICH DISCORD EMBED ---  
                    colors = {"JOIN": 0x3fb950, "OFFLINE": 0xf85149, "ONLINE": 0x58a6ff, "SWITCH": 0xd29922}  
                    avatar_url = f"https://www.roblox.com/headshot-thumbnail/image?userId={uid}&width=150&height=150&format=png"  
                    game_thumb = await get_roblox_thumb(session, universe) if event in ["JOIN", "SWITCH"] else None  

                    embed = {  
                        "title": f"👁️ Mắt Thần - {event}",  
                        "description": f"👤 **User:** `{uid}`\n🎮 **Game:** `{game}`\n🕒 **Duration:** {int((time.time()-prev['start_time'])/60)}m",  
                        "color": colors.get(event, 0x58a6ff),  
                        "thumbnail": {"url": avatar_url},  
                        "footer": {"text": f"Mắt Thần V10 • {now}"},  
                        "timestamp": datetime.now(timezone.utc).isoformat()  
                    }  
                    if game_thumb: embed["image"] = {"url": game_thumb}  
                    
                    try: 
                        discord_queue.put_nowait(embed)  
                    except asyncio.QueueFull: 
                        logging.warning("Discord queue full")

                    # Cập nhật state (Giữ start_time nếu vẫn đang trong game)  
                    user_state[uid] = {  
                        "state": state,   
                        "universe": universe,   
                        "start_time": prev["start_time"] if state == 2 and prev["state"] == 2 else time.time()  
                    }  
        except Exception as e: 
            logging.error(f"Monitor error: {e}")
            await asyncio.sleep(5)  
            
        await asyncio.sleep(CHECK_INTERVAL)

# ================= FASTAPI & UI (GIỮ NGUYÊN) =================

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
    <title>MẮT THẦN V10 - ETERNAL</title>  
    <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;600;700&display=swap" rel="stylesheet">  
    <style>  
        :root { --p: #58a6ff; --s: #3fb950; --d: #f85149; --bg: #04070a; --glass: rgba(22, 27, 34, 0.85); --b: rgba(48, 54, 61, 0.8); }  
        body { background: var(--bg); color: #c9d1d9; font-family: 'Plus Jakarta Sans', sans-serif; margin: 0; padding: 15px; height: 100vh; display: flex; flex-direction: column; overflow: hidden; }  
        .glass { background: var(--glass); backdrop-filter: blur(12px); border: 1px solid var(--b); border-radius: 12px; margin-bottom: 12px; }  
        .header { padding: 12px 20px; display: flex; justify-content: space-between; align-items: center; }  
        .stats-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; margin-bottom: 12px; }  
        .stat-card { padding: 15px; text-align: center; }  
        .stat-card div { font-size: 24px; font-weight: 700; color: var(--p); }  
        #log { flex: 1; overflow-y: auto; padding: 12px; font-family: monospace; font-size: 12px; }  
        .line { padding: 8px 10px; margin-bottom: 5px; border-radius: 6px; background: rgba(255,255,255,0.02); display: flex; gap: 10px; border-left: 3px solid transparent; }  
        .JOIN { border-left-color: var(--s); } .OFFLINE { border-left-color: var(--d); }  
    </style>  
</head>  
<body>  
    <div class="header glass"><div style="font-weight:700">👁️ MẮT THẦN <span style="color:var(--p)">V10.0</span></div><div id="st">● CONNECTING</div></div>  
    <div class="stats-grid">  
        <div class="stat-card glass"><small>Scans</small><div id="ck">0</div></div>  
        <div class="stat-card glass"><small>In-Game</small><div id="on" style="color:var(--s)">0</div></div>  
        <div class="stat-card glass"><small>Events</small><div id="ev">0</div></div>  
    </div>  
    <div id="log" class="glass"></div>  
    <audio id="snd" src="https://assets.mixkit.co/active_storage/sfx/2358/2358-preview.mp3"></audio>  
    <script>  
        const ck=document.getElementById('ck'), ev=document.getElementById('ev'), on=document.getElementById('on'), log=document.getElementById('log'), st=document.getElementById('st'), snd=document.getElementById('snd');  
        let ws;  
        function connect() {  
            ws = new WebSocket((location.protocol==='https:'?'wss':'ws')+'://'+location.host+'/ws');  
            ws.onopen = () => { st.innerText='● ONLINE'; st.style.color='#3fb950'; };  
            ws.onclose = () => { st.innerText='○ OFFLINE'; st.style.color='#f85149'; setTimeout(connect, 3000); };  
            ws.onmessage = (e) => {  
                let d = JSON.parse(e.data);  
                if(d.type==='init' || d.type==='update') {   
                    ck.innerText=d.stats.checks; ev.innerText=d.stats.events; on.innerText=d.system.online_users;  
                    if(d.new_log) { add(d.new_log, true); if(['JOIN','SWITCH'].includes(d.new_log.type)) snd.play().catch(()=>{}); }  
                    if(d.logs && d.type==='init') d.logs.forEach(l => add(l));  
                }  
            };  
        }  
        function add(l, isNew=false) {  
            let div = document.createElement('div'); div.className = 'line ' + l.type;  
            div.innerHTML = `<span>[${l.time}]</span><span>${l.msg}</span>`;  
            if(isNew) { log.prepend(div); log.scrollTop = 0; } else { log.appendChild(div); }  
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
            try:
                await asyncio.wait_for(ws.receive_text(), timeout=60)
            except asyncio.TimeoutError:
                continue 
                
    except WebSocketDisconnect: 
        ws_manager.disconnect(ws)
    except Exception as e:
        logging.error(f"WebSocket error: {e}")
        ws_manager.disconnect(ws)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
  
