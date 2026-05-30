import asyncio
import logging
import sys
import time
from pathlib import Path

log = logging.getLogger("pipeline")

SRT_INPUT  = "srt://192.168.200.130:33511?mode=caller&latency=200"
SRT_OUTPUT = "srt://127.0.0.1:33512?mode=caller&latency=200&streamid=publish:live"
UDP_MAIN   = "udp://127.0.0.1:5000"
UDP_SCTE   = "udp://127.0.0.1:5001"
ZMQ_PORT   = 5556
OVERLAY_A  = str(Path(__file__).parent / "assets" / "overlay_out_a.mov")
OVERLAY_B  = str(Path(__file__).parent / "assets" / "overlay_out_b.mov")
OVERLAY    = OVERLAY_A  # legacy alias


class FFmpegProcess:
    def __init__(self, name: str, cmd_fn, restart_cb=None):
        self.name       = name
        self.cmd_fn     = cmd_fn
        self.restart_cb = restart_cb
        self.process    = None
        self._stop      = asyncio.Event()
        self._task      = None
        self._last_start = 0.0
        self._log       = logging.getLogger(f"ffmpeg.{name}")

    def build_cmd(self):
        return self.cmd_fn()

    async def start(self):
        self._stop.clear()
        self._task = asyncio.create_task(self._run())

    async def stop(self):
        self._stop.set()
        if self.process:
            try:
                self.process.terminate()
                await asyncio.wait_for(self.process.wait(), timeout=5)
            except Exception:
                self.process.kill()
        if self._task:
            self._task.cancel()

    @property
    def alive(self):
        return self.process is not None and self.process.returncode is None

    async def _run(self):
        backoff = 2.0
        while not self._stop.is_set():
            cmd = self.build_cmd()
            # Kill any lingering ffmpeg holding ZMQ port
            if self.name == "encoder":
                await asyncio.create_subprocess_exec(
                    "pkill", "-f", f"zmq=b='tcp", stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL
                )
                await asyncio.sleep(1.0)
            self._log.info(f"Starting: {' '.join(cmd)}")
            self._last_start = time.time()
            self.process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                limit=1024*1024,
            )
            asyncio.create_task(self._stream_logs(self.process.stderr))
            rc = await self.process.wait()
            if self._stop.is_set():
                break
            elapsed = time.time() - self._last_start
            backoff = 2.0 if elapsed > 60 else min(backoff * 2, 30.0)
            self._log.warning(f"Exited rc={rc}, restarting in {backoff:.1f}s")
            if self.restart_cb:
                self.restart_cb()
            await asyncio.sleep(backoff)

    async def _stream_logs(self, stream):
        if stream is None:
            return
        try:
            async for line in stream:
                txt = line.decode(errors="replace").rstrip()
                if not txt:
                    continue
                if "fps=" in txt and "bitrate=" in txt:
                    self._log.info(f"STATS: {txt.strip()}")
                elif txt.startswith("[") or "Error" in txt or "error" in txt or "Invalid" in txt:
                    self._log.debug(txt)
        except Exception:
            pass


class Pipeline:
    def __init__(self, ws_log_handler=None):
        self._overlay_active = False
        self._overlay_lock   = asyncio.Lock()
        self._watchdog_task  = None
        self._active_slot    = "a"  # which slot is ready to show

        self.ingest  = FFmpegProcess("ingest",  self._ingest_cmd)
        self.encoder = FFmpegProcess("encoder", self._encoder_cmd)

    def get_inactive_path(self):
        """Path to write new overlay to (the slot NOT currently active)."""
        return OVERLAY_B if self._active_slot == "a" else OVERLAY_A

    def get_active_path(self):
        return OVERLAY_A if self._active_slot == "a" else OVERLAY_B

    def swap_slot(self):
        """After generating new overlay, swap to new slot."""
        self._active_slot = "b" if self._active_slot == "a" else "a"
        log.info(f"Overlay slot swapped to '{self._active_slot}'")

    # ── Commands ──────────────────────────────────────────────────────────────

    def _ingest_cmd(self):
        return [
            "ffmpeg", "-y",
            "-i", SRT_INPUT,
            "-map", "0", "-c", "copy", "-copyts",
            "-f", "tee",
            f"[f=mpegts]{UDP_MAIN}?pkt_size=1316"
            f"|[f=mpegts:select=d]{UDP_SCTE}?pkt_size=1316",
        ]

    def _encoder_cmd(self):
        has_a = Path(OVERLAY_A).exists()
        has_b = Path(OVERLAY_B).exists()
        has_overlay = has_a or has_b

        # Use whichever file exists as fallback
        overlay_a = OVERLAY_A if has_a else OVERLAY_B
        overlay_b = OVERLAY_B if has_b else OVERLAY_A
        enable_a = "1" if (self._overlay_active and self._active_slot == "a") else "0"
        enable_b = "1" if (self._overlay_active and self._active_slot == "b") else "0"

        if has_overlay:
            log.info(f"Encoder: overlay slot='{self._active_slot}' enable_a='{enable_a}' enable_b='{enable_b}'")
            return [
                "ffmpeg", "-y",
                "-fflags", "+discardcorrupt+nobuffer",
                "-thread_queue_size", "512",
                "-i", f"{UDP_MAIN}?fifo_size=131072&overrun_nonfatal=1&timeout=60000000",
                "-thread_queue_size", "512",
                "-stream_loop", "-1", "-i", overlay_a,
                "-thread_queue_size", "512",
                "-stream_loop", "-1", "-i", overlay_b,
                "-filter_complex",
                f"[1:v]format=rgba,setpts=PTS-STARTPTS[ov_a];"
                f"[2:v]format=rgba,setpts=PTS-STARTPTS[ov_b];"
                f"[0:v][ov_a]overlay@oa=x=0:y=0:format=auto:eof_action=pass:enable='{enable_a}'[tmp];"
                f"[tmp][ov_b]overlay@ob=x=0:y=0:format=auto:eof_action=pass:enable='{enable_b}'[pre];"
                f"[pre]zmq=b='tcp\\://*\\:{ZMQ_PORT}'[vout]",
                "-map", "[vout]", "-map", "0:a?",
                "-c:v", "libx264", "-preset", "veryfast", "-tune", "zerolatency",
                "-pix_fmt", "yuv420p",
                "-threads", "16",
                "-b:v", "6500k", "-minrate", "6500k", "-maxrate", "6500k", "-bufsize", "3250k",
                "-x264-params", "nal-hrd=cbr:force-cfr=1:rc-lookahead=0",
                "-g", "50", "-bf", "0",
                "-c:a", "copy",
                "-muxrate", "7500k",
                "-f", "mpegts", SRT_OUTPUT,
            ]
        else:
            log.warning("Encoder: no overlay file, running passthrough")
            return [
                "ffmpeg", "-y",
                "-thread_queue_size", "512",
                "-i", f"{UDP_MAIN}?fifo_size=131072&overrun_nonfatal=1&timeout=60000000",
                "-filter_complex",
                f"[0:v]format=yuv420p,zmq=b='tcp\\://*\\:{ZMQ_PORT}'[vout]",
                "-map", "[vout]", "-map", "0:a?",
                "-c:v", "libx264", "-preset", "veryfast", "-tune", "zerolatency",
                "-pix_fmt", "yuv420p",
                "-threads", "16",
                "-b:v", "6500k", "-minrate", "6500k", "-maxrate", "6500k", "-bufsize", "3250k",
                "-x264-params", "nal-hrd=cbr:force-cfr=1:rc-lookahead=0",
                "-g", "50", "-bf", "0",
                "-c:a", "copy",
                "-muxrate", "7500k",
                "-f", "mpegts", SRT_OUTPUT,
            ]

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self):
        await self.ingest.start()
        await asyncio.sleep(1)
        await self.encoder.start()
        self._watchdog_task = asyncio.create_task(self._watchdog())

    async def stop(self):
        if self._watchdog_task:
            self._watchdog_task.cancel()
        await self.encoder.stop()
        await self.ingest.stop()

    # ── Overlay trigger ───────────────────────────────────────────────────────

    async def trigger_overlay(self, duration_ms: int):
        async with self._overlay_lock:
            log.info(f"Overlay ON slot='{self._active_slot}' for {duration_ms}ms")
            self._overlay_active = True
            if self._active_slot == "a":
                await self._zmq("overlay@oa", "enable", "1")
                await self._zmq("overlay@ob", "enable", "0")
            else:
                await self._zmq("overlay@oa", "enable", "0")
                await self._zmq("overlay@ob", "enable", "1")
            await asyncio.sleep(duration_ms / 1000)
            log.info("Overlay OFF")
            self._overlay_active = False
            await self._zmq("overlay@oa", "enable", "0")
            await self._zmq("overlay@ob", "enable", "0")

    async def reload_overlay(self):
        """Swap to newly generated overlay without restarting encoder."""
        self.swap_slot()
        log.info(f"Overlay slot swapped to '{self._active_slot}' — no restart needed")

    # ── ZMQ ──────────────────────────────────────────────────────────────────

    async def _zmq(self, *args) -> bool:
        cmd_str = " ".join(args)
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "zmq_helper.py", str(ZMQ_PORT), cmd_str,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            await asyncio.wait_for(proc.communicate(), timeout=3.0)
            return proc.returncode == 0
        except asyncio.TimeoutError:
            proc.kill()
            log.warning(f"ZMQ timeout for: {cmd_str}")
            return False

    # ── Watchdog ──────────────────────────────────────────────────────────────

    async def _watchdog(self):
        down_since = None
        while True:
            await asyncio.sleep(5)
            both_down = not self.ingest.alive and not self.encoder.alive
            if both_down:
                if down_since is None:
                    down_since = time.time()
                elif time.time() - down_since > 10:
                    log.error("Both processes down >10s — force restarting")
                    await self.start()
                    down_since = None
            else:
                down_since = None

    # ── Status ────────────────────────────────────────────────────────────────

    def status(self):
        return {
            "ingest":  "running" if self.ingest.alive  else "down",
            "encoder": "running" if self.encoder.alive else "down",
            "overlay": self._overlay_active,
        }