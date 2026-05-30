import subprocess
import numpy as np
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont
from concurrent.futures import ThreadPoolExecutor

ASSETS      = Path(__file__).parent / "assets"
TEMPLATE    = ASSETS / "WidgetBet_FULL_HD.mov"
FONT        = ASSETS / "BebasNeue-Regular.ttf"
OUTPUT      = ASSETS / "overlay_out.mov"

# Widget position in the 1920x1080 frame (from calibration)
WX, WY      = 117, 547
POSITIONS_Y = [123, 221, 319]   # relative y inside widget
W, H        = 1920, 1080
FPS         = 25               # must match source stream fps
FRAME_SIZE  = W * H * 4         # ARGB
PRE_ROLL    = 0                # no pre-roll needed


def _draw_frame(args):
    rgba, teams, odds, font_path = args
    font = ImageFont.truetype(str(font_path), 27)
    bboxes = [font.getbbox(t) for t in odds]

    # Only draw if widget is visible
    if rgba[WY + 123, WX + 338, 3] > 0:
        img  = Image.fromarray(rgba)
        draw = ImageDraw.Draw(img)
        for text, bbox, ty_rel in zip(odds, bboxes, POSITIONS_Y):
            abs_y  = WY + ty_rel
            draw_x = WX + 374 - bbox[2]
            draw.rectangle((WX + 326, abs_y - 4, WX + 386, abs_y + 24), fill=(24, 24, 24, 255))
            draw.text((draw_x, abs_y - bbox[1]), text, font=font, fill=(255, 255, 255, 255))
        rgba = np.array(img)

    # RGBA → ARGB
    argb = np.stack([rgba[:, :, 3], rgba[:, :, 0], rgba[:, :, 1], rgba[:, :, 2]], axis=2)
    return argb.tobytes()


def generate_overlay(
    teams: list[str] = None,
    odds:  list[str] = None,
    threads: int = 8,
    output_path: str = None,
) -> Path:
    teams = teams or ["PALACE", "ვრე", "RAYO VALLECANO"]
    odds  = odds  or ["1.50", "1.50", "1.50"]
    out   = Path(output_path) if output_path else OUTPUT

    if not TEMPLATE.exists():
        raise FileNotFoundError(f"Template not found: {TEMPLATE}")
    if not FONT.exists():
        raise FileNotFoundError(f"Font not found: {FONT}")

    # Read all frames from template
    reader = subprocess.Popen(
        ["ffmpeg", "-i", str(TEMPLATE), "-f", "rawvideo", "-pix_fmt", "argb", "pipe:1"],
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
    )

    writer = subprocess.Popen(
        [
            "ffmpeg",
            "-f", "rawvideo", "-vcodec", "rawvideo",
            "-s", f"{W}x{H}", "-pix_fmt", "argb",
            "-r", str(FPS), "-i", "pipe:0",
            "-c:v", "qtrle", "-pix_fmt", "argb",
            "-y", str(out),
        ],
        stdin=subprocess.PIPE, stderr=subprocess.DEVNULL,
    )

    raw_frames = []
    while True:
        raw = reader.stdout.read(FRAME_SIZE)
        if len(raw) < FRAME_SIZE:
            break
        arr  = np.frombuffer(raw, dtype=np.uint8).reshape((H, W, 4))
        rgba = np.stack([arr[:, :, 1], arr[:, :, 2], arr[:, :, 3], arr[:, :, 0]], axis=2)
        raw_frames.append(rgba)

    reader.stdout.close()
    reader.wait()

    with ThreadPoolExecutor(max_workers=threads) as ex:
        results = list(ex.map(_draw_frame, [(f, teams, odds, FONT) for f in raw_frames]))

    # Write transparent pre-roll frames (invisible padding before animation)
    transparent = np.zeros((H, W, 4), dtype=np.uint8).tobytes()
    print(f"Writing {PRE_ROLL} transparent pre-roll frames...")
    for _ in range(PRE_ROLL):
        writer.stdin.write(transparent)

    for r in results:
        writer.stdin.write(r)

    writer.stdin.close()
    writer.wait()

    return out