#!/usr/bin/env python3
"""Download every map tile served by a Vintage Story WebCartographer instance.

Tiles are categorised on disk by zoom level:

    downloads/
        zoom_0/<x>_<y>.png
        zoom_1/<x>_<y>.png
        ...
        zoom_9/<x>_<y>.png
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path
from typing import Iterable

import aiohttp
from tqdm import tqdm


DEFAULT_ORIGIN = "https://map.oldtops.vintagestory.at"
DEFAULT_PATH = "/data/world"
DEFAULT_MAX_ZOOM = 9
DEFAULT_CONCURRENCY = 32
# Default probe box wide enough for a ~1M-block world at zoom 0
# (≈ 1_000_000 / 256 / 2^9 ≈ 8 tiles per side, but tile origin can be offset).
DEFAULT_PROBE_MIN = -16
DEFAULT_PROBE_MAX = 64


def tile_url(base: str, zoom: int, x: int, y: int) -> str:
    return f"{base}/{zoom}/{x}_{y}.png"


def tile_path(out: Path, zoom: int, x: int, y: int) -> Path:
    return out / f"zoom_{zoom}" / f"{x}_{y}.png"


async def probe(session: aiohttp.ClientSession, url: str, sem: asyncio.Semaphore) -> bool:
    async with sem:
        try:
            async with session.head(url, allow_redirects=False) as resp:
                if resp.status == 200:
                    return True
                if resp.status in (405, 501):
                    async with session.get(url) as r2:
                        return r2.status == 200
                return False
        except aiohttp.ClientError:
            return False


async def fetch_tile(
    session: aiohttp.ClientSession,
    url: str,
    dest: Path,
    sem: asyncio.Semaphore,
) -> bool:
    async with sem:
        try:
            async with session.get(url) as resp:
                if resp.status != 200:
                    return False
                data = await resp.read()
                dest.parent.mkdir(parents=True, exist_ok=True)
                tmp = dest.with_suffix(dest.suffix + ".part")
                tmp.write_bytes(data)
                tmp.replace(dest)
                return True
        except aiohttp.ClientError:
            return False


async def _gather_with_bar(
    tasks: list[asyncio.Task],
    desc: str,
    on_result=None,
) -> list:
    results: list = [None] * len(tasks)
    bar = tqdm(total=len(tasks), desc=desc, unit="tile", dynamic_ncols=True, smoothing=0.1)
    try:
        for i, task in enumerate(tasks):
            res = await task
            results[i] = res
            if on_result is not None:
                on_result(i, res, bar)
            bar.update(1)
    finally:
        bar.close()
    return results


async def discover_zoom(
    session: aiohttp.ClientSession,
    base: str,
    zoom: int,
    search_box: tuple[int, int, int, int],
    sem: asyncio.Semaphore,
) -> set[tuple[int, int]]:
    xmin, xmax, ymin, ymax = search_box
    coords = [(x, y) for x in range(xmin, xmax + 1) for y in range(ymin, ymax + 1)]
    desc = f"probe z{zoom} x[{xmin}..{xmax}] y[{ymin}..{ymax}]"
    tasks = [
        asyncio.create_task(probe(session, tile_url(base, zoom, x, y), sem))
        for x, y in coords
    ]
    hits: set[tuple[int, int]] = set()

    def on_result(i, ok, bar):
        if ok:
            hits.add(coords[i])
            bar.set_postfix(hits=len(hits))

    await _gather_with_bar(tasks, desc, on_result)
    return hits


async def download_tiles(
    session: aiohttp.ClientSession,
    base: str,
    out: Path,
    zoom: int,
    tiles: Iterable[tuple[int, int]],
    sem: asyncio.Semaphore,
    skip_existing: bool,
) -> tuple[int, int, int]:
    todo: list[tuple[int, int, Path]] = []
    cached = 0
    for x, y in tiles:
        dest = tile_path(out, zoom, x, y)
        if skip_existing and dest.exists() and dest.stat().st_size > 0:
            cached += 1
            continue
        todo.append((x, y, dest))
    if not todo:
        return 0, 0, cached
    tasks = [
        asyncio.create_task(fetch_tile(session, tile_url(base, zoom, x, y), dest, sem))
        for x, y, dest in todo
    ]
    counters = {"ok": 0, "fail": 0}

    def on_result(_i, ok, bar):
        counters["ok" if ok else "fail"] += 1
        bar.set_postfix(ok=counters["ok"], fail=counters["fail"])

    await _gather_with_bar(tasks, f"download z{zoom}", on_result)
    return counters["ok"], counters["fail"], cached


def bounds_of(tiles: set[tuple[int, int]]) -> tuple[int, int, int, int]:
    xs = [x for x, _ in tiles]
    ys = [y for _, y in tiles]
    return min(xs), max(xs), min(ys), max(ys)


async def discover_with_expand(
    session: aiohttp.ClientSession,
    base: str,
    zoom: int,
    initial_box: tuple[int, int, int, int],
    sem: asyncio.Semaphore,
    max_attempts: int = 3,
) -> set[tuple[int, int]]:
    """Probe `zoom`; if nothing is found, widen the box and retry a couple times."""
    box = initial_box
    for attempt in range(max_attempts):
        tiles = await discover_zoom(session, base, zoom, box, sem)
        if tiles:
            return tiles
        if attempt + 1 == max_attempts:
            break
        xmin, xmax, ymin, ymax = box
        w = xmax - xmin + 1
        h = ymax - ymin + 1
        cx = (xmin + xmax) // 2
        cy = (ymin + ymax) // 2
        box = (cx - w, cx + w, cy - h, cy + h)
        print(f"  no hits at zoom {zoom}; widening probe box and retrying...")
    return set()


async def main_async(args: argparse.Namespace) -> int:
    base = f"{args.origin.rstrip('/')}{args.path}"
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)

    sem = asyncio.Semaphore(args.concurrency)
    timeout = aiohttp.ClientTimeout(total=60)
    connector = aiohttp.TCPConnector(limit=args.concurrency * 2)
    headers = {"User-Agent": "vs-webmap-downloader/1.0"}

    print(f"origin: {args.origin}")
    print(f"base:   {base}")
    print(f"zoom:   {args.min_zoom}..{args.max_zoom}")
    print(f"probe:  x,y in [{args.probe_min}..{args.probe_max}]")
    print(f"out:    {out.resolve()}\n")

    async with aiohttp.ClientSession(timeout=timeout, connector=connector, headers=headers) as session:
        search = (args.probe_min, args.probe_max, args.probe_min, args.probe_max)
        total_ok = total_fail = total_cached = 0
        last_bounds: tuple[int, int, int, int] | None = None

        for zoom in range(args.min_zoom, args.max_zoom + 1):
            if last_bounds is None:
                tiles = await discover_with_expand(session, base, zoom, search, sem)
            else:
                tiles = await discover_zoom(session, base, zoom, search, sem)

            if not tiles:
                if last_bounds is None:
                    print(
                        f"  zoom {zoom}: no tiles found. "
                        "Try wider --probe-min/--probe-max or check --origin/--path."
                    )
                    return 1
                print(f"  zoom {zoom}: no tiles found in propagated box; continuing.")
                xmin, xmax, ymin, ymax = last_bounds
                last_bounds = (xmin * 2, xmax * 2 + 1, ymin * 2, ymax * 2 + 1)
                pad = 2
                search = (
                    last_bounds[0] - pad, last_bounds[1] + pad,
                    last_bounds[2] - pad, last_bounds[3] + pad,
                )
                continue

            xmin, xmax, ymin, ymax = bounds_of(tiles)
            print(
                f"  zoom {zoom}: found {len(tiles):,} tiles, "
                f"bounds x[{xmin}..{xmax}] y[{ymin}..{ymax}]"
            )
            ok, fail, cached = await download_tiles(
                session, base, out, zoom, tiles, sem, args.skip_existing
            )
            total_ok += ok
            total_fail += fail
            total_cached += cached
            print(f"  zoom {zoom}: downloaded {ok}, failed {fail}, cached {cached}\n")

            last_bounds = (xmin, xmax, ymin, ymax)
            pad = 2
            search = (
                xmin * 2 - pad, xmax * 2 + 1 + pad,
                ymin * 2 - pad, ymax * 2 + 1 + pad,
            )

        print(
            f"\nDone. downloaded={total_ok:,}  failed={total_fail:,}  "
            f"already-cached={total_cached:,}"
        )
        return 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--origin", default=DEFAULT_ORIGIN, help="Map origin (default: %(default)s)")
    p.add_argument("--path", default=DEFAULT_PATH, help="Tile path prefix relative to origin (default: %(default)s)")
    p.add_argument("--min-zoom", type=int, default=0, help="Lowest zoom level to download (default: %(default)s)")
    p.add_argument("--max-zoom", type=int, default=DEFAULT_MAX_ZOOM, help="Highest zoom level to download (default: %(default)s)")
    p.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY, help="Concurrent HTTP requests (default: %(default)s)")
    p.add_argument(
        "--probe-min",
        type=int,
        default=DEFAULT_PROBE_MIN,
        help="Lower bound of the initial probe box on both axes (default: %(default)s).",
    )
    p.add_argument(
        "--probe-max",
        type=int,
        default=DEFAULT_PROBE_MAX,
        help="Upper bound of the initial probe box on both axes (default: %(default)s). "
             "Default covers a ~1M-block world; widen for larger worlds.",
    )
    p.add_argument("--output", default="downloads", help="Output directory (default: %(default)s)")
    p.add_argument(
        "--no-skip-existing",
        dest="skip_existing",
        action="store_false",
        help="Re-download tiles even if they already exist locally.",
    )
    p.set_defaults(skip_existing=True)
    return p.parse_args()


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
