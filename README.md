# bet_overlay

24/7 SRT relay with runtime bet widget overlay injection.

## Structure

```
bet_overlay/
├── main.py            # FastAPI app + WebSocket logs
├── pipeline.py        # FFmpeg process supervisors (ingest + encoder)
├── generator.py       # Overlay MOV generator
├── zmq_helper.py      # ZMQ subprocess helper (fork-safe)
├── requirements.txt
├── templates/
│   └── index.html     # Control UI
└── assets/
    ├── WidgetBet_FULL_HD.mov    ← place here
    ├── BebasNeue-Regular.ttf   ← place here
    └── overlay_out.mov         ← auto-generated
```

## Setup

```bash
pip install -r requirements.txt
```

Place `WidgetBet_FULL_HD.mov` and `BebasNeue-Regular.ttf` in `assets/`.

## Run

```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

Open http://localhost:8000

## Flow

1. Fill in teams + odds → **Generate MOV** → writes `assets/overlay_out.mov`
2. Set duration → **Push Overlay** → restarts encoder, shows overlay, hides after duration

## Ports

| Port | Purpose |
|------|---------|
| 5000 | UDP — ingest → encoder |
| 5001 | UDP — SCTE-35 data stream |
| 5555 | TCP — ZMQ control (FFmpeg) |
| 8000 | HTTP — control UI |
| 9003 | SRT — output to players |

## SRT

- Input:  `srt://5.178.129.17:12349?mode=caller&latency=200`
- Output: `srt://0.0.0.0:9003?mode=listener&latency=200`
