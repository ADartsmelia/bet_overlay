import asyncio
import logging
import sys
import time
from pathlib import Path

log = logging.getLogger("pipeline")

SRT_INPUT  = "srt://192.168.200.130:33511?mode=caller&latency=200"
SRT_OUTPUT = "srt://0.0.0.0:33512?mode=listener&latency=200"
UDP_MAIN   = "udp://127.0.0.1:5000"
UDP_SCTE   = "udp://127.0.0.1:5001"
ZMQ_PORT   = 5556
OVERLAY    = "assets/overlay_out.mov"


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
        async for line in stream:
            txt = line.decode(errors="replace").rstrip()
            if txt:
                self._log.debug(txt)


class Pipeline:
    def __init__(self, ws_log_handler=None):
        self._overlay_active = False
        self._overlay_lock   = asyncio.Lock()
        self._watchdog_task  = None

        self.ingest  = FFmpegProcess("ingest",  self._ingest_cmd)
        self.encoder = FFmpegProcess("encoder", self._encoder_cmd)

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
        has_overlay = Path(OVERLAY).exists()
        enable = "1" if self._overlay_active else "0"

        if has_overlay:
            log.info(f"Encoder: overlay file found, enable='{enable}'")
            return [
                "ffmpeg", "-y",
                "-thread_queue_size", "512",
                "-i", f"{UDP_MAIN}?fifo_size=10000000&overrun_nonfatal=1&timeout=60000000",
                "-thread_queue_size", "512",
                "-stream_loop", "-1",
                "-i", OVERLAY,
                "-filter_complex",
                f"[1:v]setpts=PTS-STARTPTS[ovin];"
                f"[0:v][ovin]overlay=x=0:y=0:format=auto:eof_action=pass:enable='{enable}'[pre];"
                f"[pre]zmq=b='tcp\\://*\\:{ZMQ_PORT}'[vout]",
                "-map", "[vout]", "-map", "0:a?",
                "-c:v", "libx264", "-preset", "veryfast", "-tune", "zerolatency",
                "-threads", "16",
                "-b:v", "7M", "-minrate", "7M", "-maxrate", "7M", "-bufsize", "7M",
                "-x264-params", "nal-hrd=cbr:force-cfr=1",
                "-g", "50", "-bf", "0",
                "-c:a", "copy",
                "-f", "mpegts", SRT_OUTPUT,
            ]
        else:
            log.warning("Encoder: no overlay file, running passthrough")
            return [
                "ffmpeg", "-y",
                "-thread_queue_size", "512",
                "-i", f"{UDP_MAIN}?fifo_size=10000000&overrun_nonfatal=1&timeout=60000000",
                "-filter_complex",
                f"[0:v]zmq=b='tcp\\://*\\:{ZMQ_PORT}'[vout]",
                "-map", "[vout]", "-map", "0:a?",
                "-c:v", "libx264", "-preset", "veryfast", "-tune", "zerolatency",
                "-threads", "16",
                "-b:v", "7M", "-minrate", "7M", "-maxrate", "7M", "-bufsize", "7M",
                "-x264-params", "nal-hrd=cbr:force-cfr=1",
                "-g", "50", "-bf", "0",
                "-c:a", "copy",
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
            log.info(f"Overlay ON for {duration_ms}ms")
            self._overlay_active = True
            # Reset overlay to frame 0 then enable
            await self._zmq("overlay", "enable", "0")
            await asyncio.sleep(0.1)
            await self._zmq("overlay", "enable", "1")

            await asyncio.sleep(duration_ms / 1000)

            log.info("Overlay OFF")
            self._overlay_active = False
            await self._zmq("overlay", "enable", "0")

    async def reload_overlay(self):
        """Restart encoder to pick up new overlay file."""
        log.info("Reloading encoder with new overlay file")
        await self.encoder.stop()
        await asyncio.sleep(0.5)
        await self.encoder.start()

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