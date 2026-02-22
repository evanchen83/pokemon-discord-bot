#!/usr/bin/env python3
"""Split a CSV into shards under a byte limit, preserving header."""
from __future__ import annotations

import argparse
import csv
from io import StringIO
from pathlib import Path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description='Split CSV by max output file size')
    p.add_argument('--input', required=True, help='Input CSV path')
    p.add_argument('--output-dir', required=True, help='Output directory for shards')
    p.add_argument('--prefix', default='part', help='Shard file prefix')
    p.add_argument('--max-bytes', type=int, default=4_800_000, help='Max bytes per shard (including header)')
    p.add_argument('--encoding', default='utf-8', help='Input/output encoding')
    return p.parse_args()


def main() -> int:
    args = parse_args()
    in_path = Path(args.input)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Clear existing shard files for deterministic reruns.
    for old in sorted(out_dir.glob(f"{args.prefix}-*.csv")):
        old.unlink()

    with in_path.open('r', newline='', encoding=args.encoding) as inf:
        reader = csv.reader(inf)
        try:
            header = next(reader)
        except StopIteration:
            return 0

        shard_idx = 0
        out_f = None
        writer = None
        header_size = 0
        current_size = 0
        current_rows = 0

        def start_new_shard() -> tuple[object, csv.writer, int, int]:
            nonlocal shard_idx
            shard_idx += 1
            out_path = out_dir / f"{args.prefix}-{shard_idx:03d}.csv"
            fh = out_path.open('w', newline='', encoding=args.encoding)
            w = csv.writer(fh)
            w.writerow(header)
            fh.flush()
            size = out_path.stat().st_size
            return fh, w, size, 0

        out_f, writer, current_size, current_rows = start_new_shard()
        header_size = current_size

        for row in reader:
            # Measure serialized row bytes once using csv module semantics.
            probe = StringIO()
            probe_writer = csv.writer(probe)
            probe_writer.writerow(row)
            row_bytes = len(probe.getvalue().encode(args.encoding))

            # Rotate if row would exceed cap and current shard already has data rows.
            if current_rows > 0 and current_size + row_bytes > args.max_bytes:
                out_f.close()
                out_f, writer, current_size, current_rows = start_new_shard()
                header_size = min(header_size, current_size)

            writer.writerow(row)
            out_f.flush()
            # Use stat for exact size after writer's escaping/newline handling.
            current_size = (out_dir / f"{args.prefix}-{shard_idx:03d}.csv").stat().st_size
            current_rows += 1

        out_f.close()

    print(f'shard_count={shard_idx}')
    print(f'header_bytes~{header_size}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
