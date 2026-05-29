# Vintage Story WebMap downloader

Downloads every tile served by a [WebCartographer](https://gitlab.com/th3dilli_vintagestory/WebCartographer) instance and stores them locally, grouped by zoom level.

```
downloads/
  zoom_0/<x>_<y>.png
  zoom_1/<x>_<y>.png
  ...
  zoom_9/<x>_<y>.png
```

## Setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Usage

Default origin is `https://map.oldtops.vintagestory.at`:

```powershell
python download_tiles.py
```

Different server:

```powershell
python download_tiles.py --origin https://map.ap.aurafury.org
```

### Useful options

| Flag | Default | Notes |
| --- | --- | --- |
| `--origin` | `https://map.oldtops.vintagestory.at` | Base URL of the webmap. |
| `--path` | `/data/world` | Tile path prefix appended to the origin. |
| `--min-zoom` / `--max-zoom` | `0` / `9` | Zoom range to download. |
| `--probe-min` / `--probe-max` | `-16` / `64` | Initial zoom-0 search box on both axes (defaults cover a ~1M-block world). Widen if zoom 0 reports "no tiles found". |
| `--concurrency` | `32` | Concurrent HTTP requests. |
| `--output` | `downloads` | Output directory. |
| `--no-skip-existing` | off | Re-download tiles even if they already exist. |

## How discovery works

WebCartographer uses a standard tile pyramid where every tile at zoom `n` corresponds to up to four tiles `(2x..2x+1, 2y..2y+1)` at zoom `n+1`. The script:

1. Probes a `(2·probe_radius + 1)^2` grid at the lowest zoom level with HEAD requests.
2. Records the bounding box of tiles that returned `200 OK`.
3. For each subsequent zoom, only probes coordinates inside the doubled bounding box (plus a small padding), then downloads the hits.

This keeps the discovery cost proportional to the explored world area instead of `4^max_zoom`.
