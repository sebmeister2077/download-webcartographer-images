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
import os
import sys
from pathlib import Path
from typing import Iterator

import aiohttp
from tqdm import tqdm


DEFAULT_ORIGIN = "https://map.tops.vintagestory.at"
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
    with os.scandir(zoom_dir) as it:
        for entry in it:
            if not entry.name.endswith(".png") or not entry.is_file():
                continue
            stem = entry.name[:-4]
            try:
                x_text, y_text = stem.split("_", 1)
                tiles.append((int(x_text), int(y_text)))
            except ValueError:
                continue
    return tiles


def existing_nonempty_tiles(out: Path, zoom: int) -> set[tuple[int, int]]:
    """Return the set of (x, y) tiles already present on disk with non-zero size.

    Uses a single os.scandir pass — far faster than millions of Path.exists()
    calls at high zoom levels.
    """
    zoom_dir = out / f"zoom_{zoom}"
    if not zoom_dir.is_dir():
        return set()

    found: set[tuple[int, int]] = set()
    with os.scandir(zoom_dir) as it:
        for entry in it:
            if not entry.name.endswith(".png"):
                continue
            try:
                if entry.stat().st_size <= 0:
                    continue
            except OSError:
                continue
            stem = entry.name[:-4]
            try:
                x_text, y_text = stem.split("_", 1)
                found.add((int(x_text), int(y_text)))
            except ValueError:
                continue
    return found


async def fetch_tile(
    session: aiohttp.ClientSession,
    url: str,
    dest: Path,
    retries: int = 2,
) -> str:
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


def _iter_box(box: tuple[int, int, int, int]) -> Iterator[tuple[int, int]]:
    xmin, xmax, ymin, ymax = box
    for x in range(xmin, xmax + 1):
        for y in range(ymin, ymax + 1):
            yield x, y


async def process_zoom(
    session: aiohttp.ClientSession,
    base: str,
    out: Path,
    zoom: int,
    box: tuple[int, int, int, int],
    concurrency: int,
    skip_existing: bool,
    label: str,
) -> tuple[dict[str, int], list[tuple[int, int]]]:
    """Attempt to download every tile in `box` at `zoom` using a bounded worker
    pool so memory stays O(concurrency) regardless of tile count."""
    xmin, xmax, ymin, ymax = box
    total = (xmax - xmin + 1) * (ymax - ymin + 1)

    existing = existing_nonempty_tiles(out, zoom) if skip_existing else set()
    (out / f"zoom_{zoom}").mkdir(parents=True, exist_ok=True)

    counts = {OK: 0, MISS: 0, FAIL: 0, CACHED: 0}
    hits: list[tuple[int, int]] = []

    desc = f"{label} z{zoom} ({total:,} tiles, x[{xmin}..{xmax}] y[{ymin}..{ymax}])"
    bar = tqdm(total=total, desc=desc, unit="tile", dynamic_ncols=True, smoothing=0.05)

    # Small bounded queue — just enough to keep workers fed without buffering
    # the entire coordinate space in memory.
    queue: asyncio.Queue[tuple[int, int] | None] = asyncio.Queue(maxsize=concurrency * 4)

    async def producer() -> None:
        for coord in _iter_box(box):
            await queue.put(coord)
        for _ in range(concurrency):
            await queue.put(None)

    async def worker() -> None:
        while True:
            item = await queue.get()
            if item is None:
                return
            x, y = item
            if (x, y) in existing:
                res = CACHED
            else:
                res = await fetch_tile(
                    session, tile_url(base, zoom, x, y), tile_path(out, zoom, x, y),
                )
            if res in (OK, CACHED):
                hits.append((x, y))
            counts[res] = counts.get(res, 0) + 1
            bar.update(1)
            # Update postfix only every so often to avoid tqdm overhead at high rates.
            if (counts[OK] + counts[MISS] + counts[FAIL] + counts[CACHED]) % 256 == 0:
                bar.set_postfix(ok=counts[OK], miss=counts[MISS], fail=counts[FAIL], cached=counts[CACHED])

    try:
        workers = [asyncio.create_task(worker()) for _ in range(concurrency)]
        prod = asyncio.create_task(producer())
        await asyncio.gather(prod, *workers)
    finally:
        bar.set_postfix(ok=counts[OK], miss=counts[MISS], fail=counts[FAIL], cached=counts[CACHED])
        bar.close()

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
                args.concurrency,
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
                session, base, out, zoom, box, args.concurrency, args.skip_existing, label,
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
