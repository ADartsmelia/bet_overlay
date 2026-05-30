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

from pipeline import Pipeline, SRT_INPUT
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
        # Write to inactive slot so stream keeps playing active slot
        target_path = pipeline.get_inactive_path()
        log.info(f"Writing to inactive slot: {target_path}")
        path = await asyncio.to_thread(
            generate_overlay,
            teams=[req.team1, req.team2, req.team3],
            odds=[req.odd1, req.odd2, req.odd3],
            output_path=target_path,
        )
        log.info(f"Overlay generated → {path}")
        # Swap slot — no restart needed
        await pipeline.reload_overlay()
        return {"ok": True, "path": str(path)}
    except Exception as e:
        log.error(f"Generate failed: {e}")
        return {"ok": False, "error": str(e)}

@app.post("/overlay/push")
async def overlay_push(req: PushRequest):
    active_path = Path(pipeline.get_active_path())
    if not active_path.exists():
        log.warning("No overlay file found — generate first")
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

@app.get("/latency")
async def latency():
    import asyncio, json, time

    async def get_pts(url):
        try:
            proc = await asyncio.create_subprocess_exec(
                "ffprobe", "-v", "quiet",
                "-analyzeduration", "2000000",
                "-probesize", "500000",
                "-print_format", "json",
                "-show_packets", "-select_streams", "v:0",
                "-read_intervals", "%+#2",
                url,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
            out, _ = await asyncio.wait_for(proc.communicate(), timeout=8)
            data = json.loads(out)
            pkts = data.get("packets", [])
            if pkts:
                pts = float(pkts[-1].get("pts_time", 0))
                wall = time.time()
                return {"pts": pts, "wall": wall}
            return {"error": "no packets"}
        except Exception as e:
            return {"error": str(e)}

    t_start = time.time()
    original, ours = await asyncio.gather(
        get_pts(f"{SRT_INPUT}"),
        get_pts("srt://127.0.0.1:33512?mode=caller&latency=200&streamid=read:live"),
    )
    t_end = time.time()

    if "pts" in original and "pts" in ours:
        diff = original["pts"] - ours["pts"]
        return {
            "original_pts": round(original["pts"], 3),
            "ours_pts": round(ours["pts"], 3),
            "diff_seconds": round(diff, 3),
            "note": "positive = ours is behind original"
        }
    return {"original": original, "ours": ours, "probe_took": round(t_end - t_start, 2)}
    import asyncio, json
    async def probe(url):
        try:
            proc = await asyncio.create_subprocess_exec(
                "ffprobe", "-v", "quiet",
                "-analyzeduration", "3000000",
                "-probesize", "1000000",
                "-print_format", "json",
                "-show_streams", "-show_format", url,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            out, _ = await asyncio.wait_for(proc.communicate(), timeout=10)
            data = json.loads(out)
            vs = next((s for s in data.get("streams", []) if s.get("codec_type") == "video"), {})
            fmt = data.get("format", {})
            return {
                "bitrate_kbps": int(fmt.get("bit_rate", 0)) // 1000,
                "codec": vs.get("codec_name"),
                "fps": vs.get("r_frame_rate"),
                "resolution": f"{vs.get('width')}x{vs.get('height')}",
                "pix_fmt": vs.get("pix_fmt"),
                "audio_streams": sum(1 for s in data.get("streams", []) if s.get("codec_type") == "audio"),
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