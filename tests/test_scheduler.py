#!/usr/bin/env python3
import difflib
import subprocess
import sys
from pathlib import Path


def main() -> int:
    binary = sys.argv[1] if len(sys.argv) > 1 else "./scheduler"
    fixtures = Path(__file__).parent / "fixtures"
    interaction = (fixtures / "public_example.in").read_text()
    expected = (fixtures / "public_example.out").read_text()

    task_done = "TDN E D POST -1 1 0 1.000000000\n"
    finish = "FIN 0\n"
    final_events = task_done + finish
    if interaction.count(final_events) != 1:
        raise AssertionError("public fixture no longer has the expected final frame")
    interaction = interaction.replace(final_events, finish + task_done)

    result = subprocess.run(
        [binary], input=interaction, text=True, capture_output=True,
        timeout=5, check=False,
    )
    if result.returncode != 0:
        raise AssertionError(
            f"exited {result.returncode}\nstdout:\n{result.stdout}"
            f"stderr:\n{result.stderr}"
        )
    if result.stdout != expected:
        diff = "".join(difflib.unified_diff(
            expected.splitlines(keepends=True),
            result.stdout.splitlines(keepends=True),
            fromfile="expected", tofile="actual",
        ))
        raise AssertionError(f"FIN-before-TDN output mismatch\n{diff}")

    print("1 scheduler regression case passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
