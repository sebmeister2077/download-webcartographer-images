#!/usr/bin/env python3
"""Download every map tile served by a Vintage Story WebCartographer instance.

Tiles are categorised on disk by zoom level:

    downloads/
        zoom_1/<x>_<y>.png
        zoom_2/<x>_<y>.png
        ...
        zoom_9/<x>_<y>.png

Strategy:
  1. Probe the lowest zoom level over a configurable square box to find the
     bounding box of existing tiles.
  2. For every subsequent zoom level, derive the candidate child tiles by
     doubling the previous bounds (+ small padding) and just attempt the GET
     directly. 404s on the edges are cheap and expected.
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

import aiohttp
from tqdm import tqdm


DEFAULT_ORIGIN = "https://map.oldtops.vintagestory.at"
DEFAULT_PATH = "/data/world"
DEFAULT_MIN_ZOOM = 1   # WebCartographer's pyramid starts at zoom 1.
DEFAULT_MAX_ZOOM = 9
DEFAULT_CONCURRENCY = 64
# Default probe box wide enough for a ~1M-block world at zoom 1
# (≈ 1_000_000 / 256 / 2^8 ≈ 15 tiles per side, but tile origin can be offset).
DEFAULT_PROBE_MIN = -8
DEFAULT_PROBE_MAX = 40
EDGE_PAD = 2  # extra ring when propagating bounds to the next zoom level.

# Outcome codes for `fetch_tile`.
OK = "ok"
MISS = "miss"      # HTTP 404 — expected on edges.
FAIL = "fail"     # network error or other non-OK status.
CACHED = "cached"


def tile_url(base: str, zoom: int, x: int, y: int) -> str:
    return f"{base}/{zoom}/{x}_{y}.png"


def tile_path(out: Path, zoom: int, x: int, y: int) -> Path:
    return out / f"zoom_{zoom}" / f"{x}_{y}.png"


def local_tiles(out: Path, zoom: int) -> list[tuple[int, int]]:
    zoom_dir = out / f"zoom_{zoom}"
    if not zoom_dir.is_dir():
        return []

    tiles: list[tuple[int, int]] = []
    for path in zoom_dir.glob("*.png"):
        try:
            x_text, y_text = path.stem.split("_", 1)
            tiles.append((int(x_text), int(y_text)))
        except ValueError:
            continue
    return tiles


async def fetch_tile(
    session: aiohttp.ClientSession,
    url: str,
    dest: Path,
    sem: asyncio.Semaphore,
    skip_existing: bool,
    retries: int = 2,
) -> str:
    if skip_existing and dest.exists() and dest.stat().st_size > 0:
        return CACHED
    async with sem:
        for attempt in range(retries + 1):
            try:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.read()
                        dest.parent.mkdir(parents=True, exist_ok=True)
                        tmp = dest.with_suffix(dest.suffix + ".part")
                        tmp.write_bytes(data)
                        tmp.replace(dest)
                        return OK
                    if resp.status == 404:
                        return MISS
                    return FAIL
            except (aiohttp.ClientError, asyncio.TimeoutError):
                if attempt == retries:
                    return FAIL
                await asyncio.sleep(0.25 * (attempt + 1))
        return FAIL


async def _run_with_bar(
    tasks: list[asyncio.Task],
    desc: str,
) -> dict[str, int]:
    counts = {OK: 0, MISS: 0, FAIL: 0, CACHED: 0}
    bar = tqdm(total=len(tasks), desc=desc, unit="tile", dynamic_ncols=True, smoothing=0.05)
    try:
        for coro in asyncio.as_completed(tasks):
            res = await coro
            counts[res] = counts.get(res, 0) + 1
            bar.update(1)
            bar.set_postfix(ok=counts[OK], miss=counts[MISS], fail=counts[FAIL], cached=counts[CACHED])
    finally:
        bar.close()
    return counts


async def process_zoom(
    session: aiohttp.ClientSession,
    base: str,
    out: Path,
    zoom: int,
    box: tuple[int, int, int, int],
    sem: asyncio.Semaphore,
    skip_existing: bool,
    label: str,
) -> tuple[dict[str, int], list[tuple[int, int]]]:
    """Attempt to download every tile in `box` at `zoom`. Returns counts + hits."""
    xmin, xmax, ymin, ymax = box
    coords = [(x, y) for x in range(xmin, xmax + 1) for y in range(ymin, ymax + 1)]
    hits: list[tuple[int, int]] = []
    tasks: list[asyncio.Task] = []

    async def run(x: int, y: int) -> str:
        res = await fetch_tile(
            session, tile_url(base, zoom, x, y), tile_path(out, zoom, x, y),
            sem, skip_existing,
        )
        if res in (OK, CACHED):
            hits.append((x, y))
        return res

    for x, y in coords:
        tasks.append(asyncio.create_task(run(x, y)))

    desc = f"{label} z{zoom} ({len(coords):,} tiles, x[{xmin}..{xmax}] y[{ymin}..{ymax}])"
    counts = await _run_with_bar(tasks, desc)
    return counts, hits


def bounds_of(tiles: list[tuple[int, int]]) -> tuple[int, int, int, int]:
    xs = [x for x, _ in tiles]
    ys = [y for _, y in tiles]
    return min(xs), max(xs), min(ys), max(ys)


def doubled(box: tuple[int, int, int, int], pad: int) -> tuple[int, int, int, int]:
    xmin, xmax, ymin, ymax = box
    return (xmin * 2 - pad, xmax * 2 + 1 + pad, ymin * 2 - pad, ymax * 2 + 1 + pad)


def infer_zoom_box_from_local(out: Path, zoom: int) -> tuple[int, int, int, int] | None:
    hits = local_tiles(out, zoom)
    if hits:
        return bounds_of(hits)

    for source_zoom in range(zoom - 1, 0, -1):
        source_hits = local_tiles(out, source_zoom)
        if not source_hits:
            continue

        box = bounds_of(source_hits)
        for _ in range(source_zoom, zoom):
            box = doubled(box, EDGE_PAD)
        return box

    return None


async def main_async(args: argparse.Namespace) -> int:
    base = f"{args.origin.rstrip('/')}{args.path}"
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)

    sem = asyncio.Semaphore(args.concurrency)
    timeout = aiohttp.ClientTimeout(total=60, connect=20)
    connector = aiohttp.TCPConnector(
        limit=args.concurrency * 2,
        limit_per_host=args.concurrency * 2,
        ttl_dns_cache=300,
    )
    headers = {"User-Agent": "vs-webmap-downloader/1.0"}

    print(f"origin: {args.origin}")
    print(f"base:   {base}")
    print(f"zoom:   {args.min_zoom}..{args.max_zoom}")
    print(f"probe:  x,y in [{args.probe_min}..{args.probe_max}] at zoom {args.min_zoom}")
    print(f"out:    {out.resolve()}\n")

    async with aiohttp.ClientSession(timeout=timeout, connector=connector, headers=headers) as session:
        if args.redo_zoom is not None:
            box = infer_zoom_box_from_local(out, args.redo_zoom)
            if box is None:
                print(
                    f"redo zoom {args.redo_zoom}: no local tiles found for zoom {args.redo_zoom} "
                    "or any lower zoom to infer bounds from."
                )
                return 1

            counts, hits = await process_zoom(
                session,
                base,
                out,
                args.redo_zoom,
                box,
                sem,
                False,
                "redo",
            )

            if not hits:
                print(f"redo zoom {args.redo_zoom}: no tiles found in inferred bounds; nothing refreshed.")
                return 1

            xmin, xmax, ymin, ymax = bounds_of(hits)
            print(
                f"  zoom {args.redo_zoom}: ok={counts[OK]} cached={counts[CACHED]} "
                f"miss={counts[MISS]} fail={counts[FAIL]}  "
                f"bounds x[{xmin}..{xmax}] y[{ymin}..{ymax}]\n"
            )
            print(
                f"Done. ok={counts[OK]:,} cached={counts[CACHED]:,} "
                f"miss={counts[MISS]:,} fail={counts[FAIL]:,}"
            )
            return 0 if counts[FAIL] == 0 else 2

        totals = {OK: 0, MISS: 0, FAIL: 0, CACHED: 0}
        probe_box = (args.probe_min, args.probe_max, args.probe_min, args.probe_max)
        last_hits: list[tuple[int, int]] | None = None

        for zoom in range(args.min_zoom, args.max_zoom + 1):
            if last_hits is None:
                # Discovery + download in one pass for the first zoom level.
                box = probe_box
                label = "scan"
            else:
                box = doubled(bounds_of(last_hits), EDGE_PAD)
                label = "fetch"

            counts, hits = await process_zoom(
                session, base, out, zoom, box, sem, args.skip_existing, label,
            )
            for k, v in counts.items():
                totals[k] = totals.get(k, 0) + v

            if not hits:
                if last_hits is None:
                    print(
                        f"  zoom {zoom}: no tiles found. Widen --probe-min/--probe-max "
                        "or check --origin/--path."
                    )
                    return 1
                print(f"  zoom {zoom}: no tiles found; stopping.")
                break

            xmin, xmax, ymin, ymax = bounds_of(hits)
            print(
                f"  zoom {zoom}: ok={counts[OK]} cached={counts[CACHED]} "
                f"miss={counts[MISS]} fail={counts[FAIL]}  "
                f"bounds x[{xmin}..{xmax}] y[{ymin}..{ymax}]\n"
            )
            last_hits = hits

        print(
            f"Done. ok={totals[OK]:,} cached={totals[CACHED]:,} "
            f"miss={totals[MISS]:,} fail={totals[FAIL]:,}"
        )
        return 0 if totals[FAIL] == 0 else 2


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--origin", default=DEFAULT_ORIGIN, help="Map origin (default: %(default)s)")
    p.add_argument("--path", default=DEFAULT_PATH, help="Tile path prefix relative to origin (default: %(default)s)")
    p.add_argument("--min-zoom", type=int, default=DEFAULT_MIN_ZOOM, help="Lowest zoom level (default: %(default)s)")
    p.add_argument("--max-zoom", type=int, default=DEFAULT_MAX_ZOOM, help="Highest zoom level (default: %(default)s)")
    p.add_argument(
        "--redo-zoom",
        type=int,
        help="Re-download only one zoom level, inferring bounds from existing local tiles.",
    )
    p.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY, help="Concurrent HTTP requests (default: %(default)s)")
    p.add_argument(
        "--probe-min", type=int, default=DEFAULT_PROBE_MIN,
        help="Lower bound of the initial probe box on both axes (default: %(default)s).",
    )
    p.add_argument(
        "--probe-max", type=int, default=DEFAULT_PROBE_MAX,
        help="Upper bound of the initial probe box on both axes (default: %(default)s). "
             "Default covers a ~1M-block world; widen for larger worlds.",
    )
    p.add_argument("--output", default="downloads", help="Output directory (default: %(default)s)")
    p.add_argument(
        "--no-skip-existing", dest="skip_existing", action="store_false",
        help="Re-download tiles even if they already exist locally.",
    )
    p.set_defaults(skip_existing=True)
    args = p.parse_args()
    if args.redo_zoom is not None and args.redo_zoom < 1:
        p.error("--redo-zoom must be 1 or higher")
    return args


def main() -> None:
    args = parse_args()
    try:
        rc = asyncio.run(main_async(args))
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        sys.exit(130)
    sys.exit(rc)


if __name__ == "__main__":
    main()
