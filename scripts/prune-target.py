#!/usr/bin/env python3
"""Keep a Cargo target at 5 completed oag builds before the next one starts.

One fresh workspace build is about 6G, almost all of it full-debug oag artifacts.
Third-party deps already have several live hashes per crate, so those are never
touched. Only superseded oag / liboag generations are removed.

Before a build this leaves 5 generations. The build may add a 6th. The next
start drops the oldest again, so the tree stays at or below 6 builds.
"""

from __future__ import annotations

import argparse
import fcntl
import os
import re
import sys
from pathlib import Path

KEEP = 5
HASHED = re.compile(r"^(?P<stem>.+)-(?P<hash>[0-9a-f]{16})(?P<rest>\..*)?$")
INCREMENTAL = re.compile(r"^(?P<stem>.+)-(?P<hash>[0-9A-Za-z]+)$")


def is_oag(stem: str) -> bool:
    return stem == "oag" or stem.startswith(("oag-", "oag_", "liboag"))


def looks_like_target(path: Path) -> bool:
    """Whether `path` carries the files cargo leaves at the root of every target dir."""
    return (path / "CACHEDIR.TAG").is_file() or (path / ".rustc_info.json").is_file()


def inside(path: Path, root: Path) -> bool:
    """Whether `path` is `root` or one of its descendants, by real path."""
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def find_target(out_dir: str | None, fallback: Path) -> Path | None:
    if out_dir:
        current = Path(out_dir).resolve()
        for parent in [current, *current.parents]:
            if (parent / "CACHEDIR.TAG").is_file() or (parent / ".rustc_info.json").is_file():
                return parent
            if parent.name in {"debug", "release"} and parent.parent.name == "target":
                return parent.parent
    return fallback if fallback.is_dir() else None


def live_inodes(profile: Path) -> set[tuple[int, int]]:
    live: set[tuple[int, int]] = set()
    if not profile.is_dir():
        return live
    for entry in profile.iterdir():
        if not entry.is_file() or HASHED.match(entry.name):
            continue
        try:
            st = entry.stat()
        except OSError:
            continue
        live.add((st.st_dev, st.st_ino))
    return live


def prune_hashed_dir(directory: Path, live: set[tuple[int, int]], keep: int, dry_run: bool) -> tuple[int, int]:
    if not directory.is_dir():
        return 0, 0
    groups: dict[str, dict[str, list[Path]]] = {}
    mtimes: dict[tuple[str, str], float] = {}
    forced: dict[str, set[str]] = {}
    for entry in directory.iterdir():
        if not entry.is_file():
            continue
        match = HASHED.match(entry.name)
        if not match or not is_oag(match.group("stem")):
            continue
        stem, digest = match.group("stem"), match.group("hash")
        groups.setdefault(stem, {}).setdefault(digest, []).append(entry)
        try:
            st = entry.stat()
        except OSError:
            continue
        key = (stem, digest)
        mtimes[key] = max(mtimes.get(key, 0.0), st.st_mtime)
        if (st.st_dev, st.st_ino) in live:
            forced.setdefault(stem, set()).add(digest)

    removed = 0
    freed = 0
    for stem, hashes in groups.items():
        keep_hashes = set(forced.get(stem, set()))
        ordered = sorted(hashes, key=lambda digest: mtimes.get((stem, digest), 0.0), reverse=True)
        for digest in ordered:
            if len(keep_hashes) >= keep:
                break
            keep_hashes.add(digest)
        for digest, paths in hashes.items():
            if digest in keep_hashes:
                continue
            for path in paths:
                try:
                    size = path.stat().st_size
                except OSError:
                    size = 0
                if dry_run:
                    print(f"would remove {path}", file=sys.stderr)
                else:
                    try:
                        path.unlink()
                    except OSError as exc:
                        print(f"prune-target: skip {path}: {exc}", file=sys.stderr)
                        continue
                removed += 1
                freed += size
    return removed, freed


def prune_generation_dirs(
    directory: Path,
    keep: int,
    dry_run: bool,
    force_keep: set[str] | None = None,
) -> tuple[int, int]:
    if not directory.is_dir():
        return 0, 0
    pattern = INCREMENTAL if directory.name == "incremental" else HASHED
    groups: dict[str, list[tuple[float, str, Path]]] = {}
    for entry in directory.iterdir():
        if not entry.is_dir():
            continue
        match = pattern.match(entry.name)
        if not match or not is_oag(match.group("stem")):
            continue
        try:
            mtime = entry.stat().st_mtime
        except OSError:
            continue
        groups.setdefault(match.group("stem"), []).append((mtime, match.group("hash"), entry))

    removed = 0
    freed = 0
    forced = force_keep or set()
    for entries in groups.values():
        entries.sort(key=lambda item: item[0], reverse=True)
        kept = 0
        drop: list[Path] = []
        for _mtime, digest, path in entries:
            if digest in forced or kept < keep:
                kept += 1
                continue
            drop.append(path)
        for path in drop:
            size = dir_size(path)
            if dry_run:
                print(f"would remove {path}", file=sys.stderr)
            else:
                freed += remove_tree(path)
                removed += 1
                continue
            removed += 1
            freed += size
    return removed, freed


def dir_size(path: Path) -> int:
    total = 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            try:
                total += (Path(root) / name).stat().st_size
            except OSError:
                pass
    return total


def remove_tree(path: Path) -> int:
    freed = 0
    for root, dirs, files in os.walk(path, topdown=False):
        for name in files:
            file_path = Path(root) / name
            try:
                freed += file_path.stat().st_size
                file_path.unlink()
            except OSError:
                pass
        for name in dirs:
            try:
                (Path(root) / name).rmdir()
            except OSError:
                pass
    try:
        path.rmdir()
    except OSError:
        pass
    return freed


def prune_tmp(profile: Path, dry_run: bool) -> tuple[int, int]:
    deps = profile / "deps"
    if not deps.is_dir():
        return 0, 0
    removed = 0
    freed = 0
    for entry in deps.iterdir():
        if ".tmp" not in entry.name:
            continue
        stem = entry.name[1:] if entry.name.startswith(".") else entry.name
        stem = stem.split("-", 1)[0]
        if not is_oag(stem):
            continue
        try:
            size = entry.stat().st_size if entry.is_file() else 0
        except OSError:
            continue
        if dry_run:
            print(f"would remove {entry}", file=sys.stderr)
        else:
            try:
                entry.unlink()
            except OSError as exc:
                print(f"prune-target: skip {entry}: {exc}", file=sys.stderr)
                continue
        removed += 1
        freed += size
    return removed, freed


def prune_target(target: Path, keep: int, dry_run: bool) -> tuple[int, int]:
    removed = 0
    freed = 0
    for profile_name in ("debug", "release"):
        profile = target / profile_name
        if not profile.is_dir():
            continue
        live = live_inodes(profile)
        for count, size in (
            prune_hashed_dir(profile / "deps", live, keep, dry_run),
            prune_generation_dirs(profile / "incremental", keep, dry_run),
            prune_generation_dirs(profile / ".fingerprint", keep, dry_run),
            prune_generation_dirs(profile / "build", keep, dry_run),
            prune_tmp(profile, dry_run),
        ):
            removed += count
            freed += size
    return removed, freed


def once_guard(target: Path, cargo_pid: str) -> bool:
    """Return True if this cargo process already pruned."""
    if not cargo_pid:
        return False
    stamp = target / ".prune-cargo-pid"
    try:
        if stamp.read_text().strip() == cargo_pid and os.path.isdir(f"/proc/{cargo_pid}"):
            return True
    except OSError:
        pass
    return False


def write_stamp(target: Path, cargo_pid: str) -> None:
    if not cargo_pid:
        return
    try:
        (target / ".prune-cargo-pid").write_text(cargo_pid + "\n")
    except OSError as exc:
        print(f"prune-target: stamp: {exc}", file=sys.stderr)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", type=Path)
    parser.add_argument("--out-dir")
    parser.add_argument("--keep", type=int, default=KEEP)
    parser.add_argument("--cargo-pid", default=os.environ.get("PRUNE_CARGO_PID", ""))
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if args.keep < 1:
        print("prune-target: --keep must be >= 1", file=sys.stderr)
        return 2

    fallback = args.target or Path(__file__).resolve().parents[1] / "target"
    target = find_target(args.out_dir or os.environ.get("PRUNE_OUT_DIR"), fallback)
    if target is None or not target.is_dir():
        return 0

    # Only ever prune a target that belongs to this repository. Cargo may be pointed at a
    # shared or external --target-dir, and deleting another checkout's builds is never
    # acceptable. A caller that names --target explicitly is asking about that tree, so it
    # is in scope -- but it must still contain the markers of a cargo target, or a typo
    # would happily empty an arbitrary directory.
    repo_root = Path(__file__).resolve().parents[1]
    resolved = target.resolve()
    if not inside(resolved, repo_root) and not looks_like_target(resolved):
        print(f"prune-target: refusing {resolved}, not a cargo target", file=sys.stderr)
        return 0


    lock_path = target / ".prune.lock"
    lock_path.touch(exist_ok=True)
    with lock_path.open("a") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        if args.once and once_guard(target, args.cargo_pid):
            return 0
        removed, freed = prune_target(target, args.keep, args.dry_run)
        if not args.dry_run:
            write_stamp(target, args.cargo_pid)
        gib = freed / 1024 / 1024 / 1024
        action = "would remove" if args.dry_run else "removed"
        print(
            f"prune-target: keeping {args.keep} oag builds in {target}, {action} {removed} items ({gib:.2f}G)",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
