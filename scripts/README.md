# scripts/ — build-artifact pruning

Keeps `target/` at **5 completed oag builds** so debug artifacts (≈6 GB per
workspace build) stop filling the disk. One fresh build is almost entirely
full-debug `oag*` binaries, incremental dirs, and fingerprints; third-party
dependency artifacts are comparatively small and are **never touched**.

The same three files exist, byte-identical, in all four repositories:
`rust-ai` (open-agent-graph), `oag-mesh`, `oag-collisions`,
`oag-imagination-random-data-compute`. If you change one, copy the change to
the other three.

## Files

| File | Role |
|---|---|
| `prune-target.py` | The pruner. Python 3 stdlib only, no dependencies. |
| `rustc-wrapper.sh` | Cargo `rustc-wrapper` hook. Prunes once per cargo process, then `exec`s the real rustc. |
| `test_prune_target.py` | Standalone test suite. 14 tests, < 1 s. |

Wired up via `.cargo/config.toml` in the repo root:

```toml
[build]
rustc-wrapper = "scripts/rustc-wrapper.sh"
```

## The trim policy, in numbers

- **Keep: 5 prior oag build generations.** A build may add a 6th; the next
  build's first rustc drops the oldest, so the tree oscillates between 5 and 6.
- Applied **per profile**: `debug/` and `release/` each keep their own 5.
- At ~6 GB per dev build, this caps a repo's `target/` at roughly **36 GB**
  worst case (the unpruned `rust-ai/target` had reached 124 GB).
- **The knob is the `--keep 5` argument in `scripts/rustc-wrapper.sh`.** To
  change the policy, edit it there — in all four repositories, since the
  copies must stay identical. (`KEEP = 5` in `prune-target.py` is only the
  default when no `--keep` is passed.)

## Running the tests

```bash
python3 scripts/test_prune_target.py
```

Expected: `Ran 14 tests ... OK` in under a second. The suite builds synthetic
target trees in a tempdir; two tests additionally touch the real
`target/debug/deps` using only files named `oag_wrapper_probe-*` (created,
asserted on, deleted — never anything cargo made). It is safe to run at any
time, including while nothing is building.

**Run this suite after any change to `prune-target.py` or
`rustc-wrapper.sh`.** It is NOT run automatically — see "History" below for
why it must never be.

## How the pruner decides what to delete

- Only files/dirs whose *stem* matches the project: exactly `oag`, or starting
  with `oag-`, `oag_`, `liboag`. Everything else (serde, getrandom, …) is
  never touched, because third-party crates legitimately keep several live
  hashes and deleting them forces rebuilds.
- Within each matching stem, hashed generations (`name-<16 hex>`) are sorted
  by mtime; the newest `--keep` (default 5) survive, older ones are deleted
  together with their sidecars (`.d`, `.rlib`, …).
- **Hardlink safety:** `target/debug/oag` and `target/debug/deps/oag-<hash>`
  share an inode. Any hashed file whose inode is linked from a live top-level
  binary is kept regardless of age.
- Also pruned per profile (`debug/`, `release/`): `incremental/`,
  `.fingerprint/`, `build/` dirs with matching stems, and leftover
  `*.tmp*` files with matching stems.
- Result: ≤ 5 prior generations before a build; the build may add a 6th; the
  next build's first rustc drops the oldest again.

## Safety guards (do not remove)

1. **Repo confinement:** a target resolved from cargo's `--out-dir` is refused
   unless it is inside this repository (`prune-target: refusing ...`). An
   explicitly passed `--target` outside the repo is accepted only if it
   carries cargo's own markers (`CACHEDIR.TAG` or `.rustc_info.json`), so a
   typo cannot empty an arbitrary directory.
2. **Locking:** `flock` on `target/.prune.lock` — parallel rustc invocations
   serialize.
3. **Once per cargo process:** the wrapper walks the PPID chain to find the
   cargo PID; `target/.prune-cargo-pid` records it so only the first
   compilation unit of a cargo run prunes.

## CLI

```
prune-target.py [--target DIR] [--out-dir DIR] [--keep N] [--once]
                [--cargo-pid PID] [--dry-run]
```

- `--dry-run` prints `would remove ...` and deletes nothing.
- `--keep` must be ≥ 1 (exit 2 otherwise).
- Missing target dir is a silent no-op (exit 0).
- Manual use: `python3 scripts/prune-target.py --target target --keep 5 --dry-run`

## Troubleshooting

- **Cargo errors with `rustc-wrapper.sh ... exit status 1` even though the
  wrapper works when run by hand:** cargo cached a failed probe in
  `target/.rustc_info.json`. Delete that file and rebuild. (This happened
  once; see History.)
- **Wrapper changed → full rebuild:** changing the wrapper or its mtime
  invalidates cargo's fingerprints; the first build after a change recompiles.
  Expected, not a bug.
- Stamp/lock files in `target/` (`.prune.lock`, `.prune-cargo-pid`) are
  harmless and recreated as needed.

## History — the one rule learned the hard way

An earlier version ran this test suite *inside* the pruner, before every
prune, during every cargo build. The suite spawns the pruner and the wrapper
as subprocesses, which re-ran the suite: recursive, timing-dependent, and it
hung a real build for six hours. The machinery was removed entirely.

**Rule: the build-time hook must stay dumb and fast. Never make
`prune-target.py` or `rustc-wrapper.sh` execute tests, builds, or any
subprocess other than the pruner/rustc themselves. Validation is this suite,
run by hand (or by CI), never inline in the build path.**
