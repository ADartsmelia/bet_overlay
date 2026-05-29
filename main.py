import asyncio
import logging
import os
import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from pipeline import Pipeline
from generator import generate_overlay

# ── Logging ──────────────────────────────────────────────────────────────────

class WSLogHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.clients: list[WebSocket] = []

    def emit(self, record):
        msg = self.format(record)
        level = record.levelname
        asyncio.ensure_future(self._broadcast(msg, level))

    async def _broadcast(self, msg: str, level: str):
        dead = []
        for ws in self.clients:
            try:
                await ws.send_json({"msg": msg, "level": level})
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.clients.remove(ws)

ws_handler = WSLogHandler()
ws_handler.setFormatter(logging.Formatter("%(asctime)s [%(name)s] %(levelname)s — %(message)s", "%H:%M:%S"))

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(), ws_handler])
logging.getLogger("ffmpeg.encoder").setLevel(logging.INFO)
logging.getLogger("ffmpeg.ingest").setLevel(logging.WARNING)
log = logging.getLogger("main")

# ── App lifecycle ─────────────────────────────────────────────────────────────

pipeline: Pipeline = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global pipeline
    pipeline = Pipeline(ws_log_handler=ws_handler)
    await pipeline.start()
    log.info("Pipeline started")
    yield
    await pipeline.stop()
    log.info("Pipeline stopped")

app = FastAPI(lifespan=lifespan)

# ── Models ────────────────────────────────────────────────────────────────────

class GenerateRequest(BaseModel):
    team1: str = "PALACE"
    team2: str = "ვრე"
    team3: str = "RAYO VALLECANO"
    odd1: str = "1.50"
    odd2: str = "1.50"
    odd3: str = "1.50"

class PushRequest(BaseModel):
    duration_ms: int = 15000

# ── Routes ────────────────────────────────────────────────────────────────────

@app.post("/overlay/generate")
async def overlay_generate(req: GenerateRequest):
    log.info(f"Generating overlay: {req.team1}/{req.odd1}, {req.team2}/{req.odd2}, {req.team3}/{req.odd3}")
    try:
        path = await asyncio.to_thread(
            generate_overlay,
            teams=[req.team1, req.team2, req.team3],
            odds=[req.odd1, req.odd2, req.odd3],
        )
        log.info(f"Overlay generated → {path}")
        return {"ok": True, "path": str(path)}
    except Exception as e:
        log.error(f"Generate failed: {e}")
        return {"ok": False, "error": str(e)}

@app.post("/overlay/push")
async def overlay_push(req: PushRequest):
    overlay_path = Path("assets/overlay_out.mov")
    if not overlay_path.exists():
        log.warning("overlay_out.mov not found — generate first")
        return {"ok": False, "error": "Overlay not generated yet"}
    log.info(f"Pushing overlay for {req.duration_ms}ms")
    asyncio.create_task(pipeline.trigger_overlay(req.duration_ms))
    return {"ok": True, "duration_ms": req.duration_ms}

@app.post("/overlay/reset")
async def overlay_reset():
    log.info("Overlay RESET — forcing disable")
    pipeline._overlay_active = False
    ok = await pipeline._zmq("overlay", "enable", "0")
    return {"ok": ok}

@app.get("/status")
async def status():
    return pipeline.status()

@app.get("/stats")
async def stats():
    import asyncio, json
    async def probe(url):
        try:
            proc = await asyncio.create_subprocess_exec(
                "ffprobe", "-v", "quiet", "-print_format", "json",
                "-show_streams", "-show_format", url,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            out, _ = await asyncio.wait_for(proc.communicate(), timeout=5)
            data = json.loads(out)
            vs = next((s for s in data.get("streams", []) if s.get("codec_type") == "video"), {})
            return {
                "bitrate_kbps": int(data.get("format", {}).get("bit_rate", 0)) // 1000,
                "codec": vs.get("codec_name"),
                "fps": vs.get("r_frame_rate"),
                "resolution": f"{vs.get('width')}x{vs.get('height')}",
                "pix_fmt": vs.get("pix_fmt"),
            }
        except Exception as e:
            return {"error": str(e)}

    original, ours = await asyncio.gather(
        probe("srt://192.168.200.130:33511?mode=caller&latency=200"),
        probe("srt://127.0.0.1:33512?mode=caller&latency=200"),
    )
    return {"original": original, "ours": ours}

@app.websocket("/ws/logs")
async def ws_logs(websocket: WebSocket):
    await websocket.accept()
    ws_handler.clients.append(websocket)
    log.info("Log client connected")
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        ws_handler.clients.remove(websocket)
        log.info("Log client disconnected")

@app.get("/", response_class=HTMLResponse)
async def index():
    return Path("templates/index.html").read_text()