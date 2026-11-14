#!/usr/bin/env python3
"""Tests for prune-target.py and rustc-wrapper.sh.

Run:      python3 scripts/test_prune_target.py
Expected: Ran 14 tests ... OK, in under a second.

Scope and safety:
- Most tests build a synthetic cargo target tree in a tempdir and never touch
  the real checkout.
- Two wrapper tests use the real target/debug/deps, but only via files named
  oag_wrapper_probe-* which they create and delete themselves.
- The suite is standalone. It is NOT invoked by the pruner or the wrapper, and
  must never be: an earlier design that ran it inside the build path recursed
  (the suite spawns the pruner/wrapper, which ran the suite) and hung a real
  cargo build. Keep the build hook dumb; run this by hand after any change.

See scripts/README.md for the full contract of the pruner.
"""

from __future__ import annotations

import importlib.util
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent / "prune-target.py"
WRAPPER = Path(__file__).resolve().parent / "rustc-wrapper.sh"


def script_env() -> dict[str, str]:
    return os.environ.copy()


def load_prune():
    spec = importlib.util.spec_from_file_location("prune_target", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class PruneTargetTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.target = Path(self.tmp.name) / "target"
        self.debug = self.target / "debug"
        self.deps = self.debug / "deps"
        self.deps.mkdir(parents=True)
        (self.target / "CACHEDIR.TAG").write_text("Signature: 8a477f597d28d172789f06886806bc55\n")
        self.prune = load_prune()

    def tearDown(self):
        self.tmp.cleanup()

    def write_hash(self, stem: str, n: int, payload: bytes = b"x", ext: str = "") -> Path:
        path = self.deps / f"{stem}-{n:016x}{ext}"
        path.write_bytes(payload)
        os.utime(path, (n, n))
        return path

    def names(self) -> set[str]:
        return {path.name for path in self.deps.iterdir()}

    def test_keeps_five_newest_oag_hashes_and_their_sidecars(self):
        for n in range(1, 9):
            self.write_hash("oag", n, f"bin{n}".encode())
            self.write_hash("liboag_api", n, f"rlib{n}".encode(), ".rlib")
            self.write_hash("liboag_api", n, b"d", ".d")
        removed, _freed = self.prune.prune_target(self.target, keep=5, dry_run=False)
        left = self.names()
        self.assertEqual(removed, 3 + 3 + 3)
        for n in range(4, 9):
            self.assertIn(f"oag-{n:016x}", left)
            self.assertIn(f"liboag_api-{n:016x}.rlib", left)
            self.assertIn(f"liboag_api-{n:016x}.d", left)
        for n in range(1, 4):
            self.assertNotIn(f"oag-{n:016x}", left)
            self.assertNotIn(f"liboag_api-{n:016x}.rlib", left)

    def test_live_hardlink_is_kept_even_when_it_is_the_oldest(self):
        for n in range(1, 8):
            self.write_hash("oag", n, f"bin{n}".encode())
        oldest = self.deps / f"oag-{1:016x}"
        os.link(oldest, self.debug / "oag")
        self.prune.prune_target(self.target, keep=5, dry_run=False)
        left = self.names()
        self.assertIn(f"oag-{1:016x}", left)
        self.assertTrue((self.debug / "oag").is_file())
        self.assertEqual((self.debug / "oag").stat().st_ino, oldest.stat().st_ino)
        for n in range(4, 8):
            self.assertIn(f"oag-{n:016x}", left)
        self.assertNotIn(f"oag-{2:016x}", left)
        self.assertNotIn(f"oag-{3:016x}", left)

    def test_more_live_hashes_than_keep_are_all_preserved(self):
        for n in range(1, 8):
            path = self.write_hash("liboag_core", n, f"v{n}".encode(), ".rlib")
            os.link(path, self.debug / f"liboag_core_{n}.rlib")
        self.prune.prune_target(self.target, keep=5, dry_run=False)
        left = self.names()
        for n in range(1, 8):
            self.assertIn(f"liboag_core-{n:016x}.rlib", left)

    def test_third_party_crates_are_not_pruned(self):
        for n in range(8):
            self.write_hash("libgetrandom", n, b"g", ".rlib")
            self.write_hash("serde_json", n, b"s", ".rmeta")
        self.prune.prune_target(self.target, keep=5, dry_run=False)
        self.assertEqual(len(list(self.deps.iterdir())), 16)

    def test_incremental_and_fingerprint_keep_five_oag_dirs_only(self):
        incremental = self.debug / "incremental"
        fingerprint = self.debug / ".fingerprint"
        incremental.mkdir()
        fingerprint.mkdir()
        for n in range(7):
            (incremental / f"oag_mesh-{n:04d}").mkdir()
            os.utime(incremental / f"oag_mesh-{n:04d}", (n, n))
            (incremental / f"aberration-{n:04d}").mkdir()
            (fingerprint / f"oag-cli-{n:016x}").mkdir()
            os.utime(fingerprint / f"oag-cli-{n:016x}", (n, n))
            (fingerprint / f"serde-{n:016x}").mkdir()
        self.prune.prune_target(self.target, keep=5, dry_run=False)
        self.assertEqual(sorted(path.name for path in incremental.iterdir() if path.name.startswith("oag_")), [f"oag_mesh-{n:04d}" for n in range(2, 7)])
        self.assertEqual(len(list(incremental.glob("aberration-*"))), 7)
        self.assertEqual(len(list(fingerprint.glob("oag-cli-*"))), 5)
        self.assertEqual(len(list(fingerprint.glob("serde-*"))), 7)
        self.assertFalse((fingerprint / f"oag-cli-{0:016x}").exists())

    def test_protect_after_skips_generations_cargo_is_using(self):
        """A generation at or after the cargo process start is never pruned.

        Cargo creates the incremental/.fingerprint directories for the units it is about to
        compile *before* the first rustc, then writes into them. Deleting one mid-build makes
        cargo fail with "failed to write .../invoked.timestamp: No such file or directory" --
        reproduced by `cargo build --examples` after clearing the fingerprint directory.
        """
        incremental = self.debug / "incremental"
        incremental.mkdir()
        now = 10_000.0
        for n in range(10):
            path = incremental / f"oag_mesh-{n:04d}"
            path.mkdir()
            # n=0 and n=1 carry the cargo process's own start time: this build is using them.
            stamp = now if n < 2 else float(n)
            os.utime(path, (stamp, stamp))
        self.prune.prune_target(self.target, keep=5, dry_run=False, protect_after=now)
        left = {path.name for path in incremental.iterdir()}
        # The two in-use dirs survive despite being the oldest; of the rest, the five newest
        # are kept and the three oldest go.
        self.assertEqual(
            left,
            {"oag_mesh-0000", "oag_mesh-0001"}
            | {f"oag_mesh-{n:04d}" for n in range(5, 10)},
        )

    def test_protect_after_skips_dependency_outputs_cargo_is_writing(self):
        """The same horizon applies to deps/ outputs, which cargo also creates up front."""
        now = 10_000.0
        for n in range(8):
            path = self.deps / f"oag-{n:016x}"
            path.write_bytes(b"x")
            stamp = now if n < 2 else float(n)
            os.utime(path, (stamp, stamp))
        self.prune.prune_target(self.target, keep=5, dry_run=False, protect_after=now)
        left = {path.name for path in self.deps.iterdir()}
        # The two in-use outputs survive; they count toward the keep, so of the rest the three
        # newest are kept and the three oldest go.
        self.assertEqual(
            left,
            {"oag-{:016x}".format(0), "oag-{:016x}".format(1)}
            | {f"oag-{n:016x}" for n in range(5, 8)},
        )

    def test_oag_tmp_files_are_removed_and_foreign_tmp_files_stay(self):
        (self.deps / "oag-0000000000000001.tmpabc").write_bytes(b"t")
        (self.deps / ".oag-0000000000000001.tmpabc").write_bytes(b"t")
        (self.deps / "libgetrandom-0000000000000001.tmpabc").write_bytes(b"t")
        removed, freed = self.prune.prune_target(self.target, keep=5, dry_run=False)
        self.assertEqual(removed, 2)
        self.assertGreater(freed, 0)
        self.assertTrue((self.deps / "libgetrandom-0000000000000001.tmpabc").is_file())
        self.assertFalse((self.deps / "oag-0000000000000001.tmpabc").exists())

    def test_dry_run_deletes_nothing(self):
        for n in range(8):
            self.write_hash("oag", n)
        removed, _freed = self.prune.prune_target(self.target, keep=5, dry_run=True)
        self.assertEqual(removed, 3)
        self.assertEqual(len(list(self.deps.iterdir())), 8)

    def test_release_profile_is_pruned_too(self):
        release = self.target / "release" / "deps"
        release.mkdir(parents=True)
        for n in range(7):
            path = release / f"oag-{n:016x}"
            path.write_bytes(b"r")
            os.utime(path, (n, n))
        self.prune.prune_target(self.target, keep=5, dry_run=False)
        left = {path.name for path in release.iterdir()}
        self.assertEqual(left, {f"oag-{n:016x}" for n in range(2, 7)})

    def test_out_dir_resolves_to_the_target_root(self):
        found = self.prune.find_target(str(self.deps), Path("/no/such/fallback"))
        self.assertEqual(found, self.target.resolve())

    def test_once_skips_a_second_prune_in_the_same_cargo_process(self):
        for n in range(8):
            self.write_hash("oag", n)
        pid = str(os.getpid())
        first = subprocess.run(
            ["python3", str(SCRIPT), "--target", str(self.target), "--keep", "5", "--once", "--cargo-pid", pid],
            check=True,
            capture_output=True,
            text=True,
            env=script_env(),
        )
        self.assertIn("removed 3", first.stderr)
        extra = self.write_hash("oag", 0)
        second = subprocess.run(
            ["python3", str(SCRIPT), "--target", str(self.target), "--keep", "5", "--once", "--cargo-pid", pid],
            check=True,
            capture_output=True,
            text=True,
            env=script_env(),
        )
        self.assertEqual(second.stderr, "")
        self.assertTrue(extra.is_file())

    def test_cli_rejects_keep_below_one_and_missing_target_is_a_noop(self):
        bad = subprocess.run(["python3", str(SCRIPT), "--target", str(self.target), "--keep", "0"], capture_output=True, text=True, env=script_env())
        self.assertEqual(bad.returncode, 2)
        missing = subprocess.run(["python3", str(SCRIPT), "--target", str(self.target / "missing")], capture_output=True, text=True, env=script_env())
        self.assertEqual(missing.returncode, 0)

    def test_wrapper_prunes_its_own_target_then_execs_rustc(self):
        """End to end: the wrapper prunes the target it was pointed at, then execs."""
        for n in range(9):
            path = self.deps / f"oag_wrapper_probe-{n:016x}"
            path.write_bytes(b"probe")
            os.utime(path, (n, n))
        completed = subprocess.run(
            [str(WRAPPER), "/bin/echo", "rustc",
             "--out-dir", str(self.deps), "--crate-name", "oag_wrapper_probe"],
            check=True,
            capture_output=True,
            text=True,
            env=script_env(),
        )
        self.assertIn("rustc --out-dir", completed.stdout)
        left = {path.name for path in self.deps.glob("oag_wrapper_probe-*")}
        self.assertEqual(left, {f"oag_wrapper_probe-{n:016x}" for n in range(4, 9)})

    def test_wrapper_prunes_the_repository_target_when_pointed_at_it(self):
        """The checkout's real target is the one cargo's wrapper reaches."""
        repo_target = SCRIPT.resolve().parents[1] / "target"
        if not (repo_target / "debug" / "deps").is_dir():
            self.skipTest("no repository target/debug/deps")
        deps = repo_target / "debug" / "deps"
        probes = sorted(deps.glob("oag_wrapper_probe-*"))
        for path in probes:
            path.unlink()
        for n in range(9):
            path = deps / f"oag_wrapper_probe-{n:016x}"
            path.write_bytes(b"probe")
            os.utime(path, (n, n))
        try:
            completed = subprocess.run(
                [str(WRAPPER), "/bin/echo", "--out-dir", str(deps)],
                check=True, capture_output=True, text=True, env=script_env(),
            )
            left = {path.name for path in deps.glob("oag_wrapper_probe-*")}
            self.assertEqual(left, {f"oag_wrapper_probe-{n:016x}" for n in range(4, 9)})
        finally:
            for path in deps.glob("oag_wrapper_probe-*"):
                path.unlink()

    def test_wrapper_refuses_a_target_outside_the_repository(self):
        """A cargo --out-dir outside this checkout must never be pruned."""
        for n in range(8):
            self.write_hash("oag_mesh", n)
        outside = Path(self.tmp.name) / "elsewhere" / "target" / "debug" / "deps"
        outside.mkdir(parents=True)
        for n in range(8):
            path = outside / f"oag_mesh-{n:016x}"
            path.write_bytes(b"o")
            os.utime(path, (n, n))
        completed = subprocess.run(
            [str(WRAPPER), "/bin/echo", "--out-dir", str(outside)],
            check=True,
            capture_output=True,
            text=True,
            env=script_env(),
        )
        self.assertIn("refusing", completed.stderr)
        self.assertEqual(len(list(outside.iterdir())), 8)


if __name__ == "__main__":
    unittest.main(verbosity=2)
