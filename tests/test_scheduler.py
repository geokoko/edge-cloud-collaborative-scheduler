#!/usr/bin/env python3
import difflib
import subprocess
import sys
import textwrap
from pathlib import Path


def run_case(binary: str, name: str, interaction: str, expected: str) -> None:
    result = subprocess.run(
        [binary],
        input=interaction,
        text=True,
        capture_output=True,
        timeout=5,
        check=False,
    )
    if result.returncode != 0:
        raise AssertionError(
            f"{name}: exited {result.returncode}\n"
            f"stdout before exit:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    if result.stdout != expected:
        diff = "".join(
            difflib.unified_diff(
                expected.splitlines(keepends=True),
                result.stdout.splitlines(keepends=True),
                fromfile="expected",
                tofile="actual",
            )
        )
        raise AssertionError(f"{name}: protocol output mismatch\n{diff}")


def clean(value: str) -> str:
    return textwrap.dedent(value).lstrip()


def main() -> int:
    binary = sys.argv[1] if len(sys.argv) > 1 else "./scheduler"
    fixtures = Path(__file__).parent / "fixtures"
    run_case(
        binary,
        "public example",
        (fixtures / "public_example.in").read_text(),
        (fixtures / "public_example.out").read_text(),
    )

    run_case(
        binary,
        "decode cycle and FIN override",
        clean(
            """
            1 1.000000000 2.000000000 1.000000000 100 1
            30.000000000 15.000000000 1.000000000 0.000000000 0.000000000 0.500000000 0.500000000
            2
            1 1.000000000 1.000000000 1.000000000 1.000000000 1.000000000 1.000000000
            2 2.000000000 2.000000000 2.000000000 2.000000000 2.000000000 2.000000000
            0.000000000
            1
            ARR 0 1
            1.000000000
            1
            TDN E P PRE 0 0 1.000000000
            2.000000000
            1
            XDN UP 0 100 PRE 1 0
            3.000000000
            1
            TDN C0 P PROC 0 1 0 0 1.000000000
            4.000000000
            1
            XDN DOWN 0 100 PRE 1 0
            5.000000000
            1
            TDN E P POST 0 0 1.000000000
            6.000000000
            1
            TDN E D PRE -1 1 0 1.000000000
            7.000000000
            1
            XDN UP 0 100 DEC 1 0
            8.000000000
            1
            TDN C0 D PROC 0 1 0 1.000000000
            9.000000000
            1
            XDN DOWN 0 100 DEC 1 0
            10.000000000
            1
            TDN E D POST -1 1 0 1.000000000
            11.000000000
            1
            TDN E D PRE -1 1 0 1.000000000
            12.000000000
            1
            XDN UP 0 100 DEC 1 0
            13.000000000
            1
            TDN C0 D PROC 0 1 0 1.000000000
            14.000000000
            1
            XDN DOWN 0 100 DEC 1 0
            15.000000000
            2
            FIN 0
            TDN E D POST -1 1 0 1.000000000
            END
            """
        ),
        clean(
            """
            1
            E P PRE 0 0
            0
            1
            C0 P PROC 0 1 0 0
            0
            1
            E P POST 0 0
            1
            E D PRE -1 1 0
            0
            1
            C0 D PROC 0 1 0
            0
            1
            E D POST -1 1 0
            1
            E D PRE -1 1 0
            0
            1
            C0 D PROC 0 1 0
            0
            1
            E D POST -1 1 0
            0
            """
        ),
    )

    run_case(
        binary,
        "timing-aware decode batching across independent remotes",
        clean(
            """
            2 1.000000000 2.000000000 1.000000000 100 1
            30.000000000 15.000000000 1.000000000 0.000000000 0.000000000 0.500000000 0.500000000
            2
            1 1.000000000 1.000000000 1.000000000 1.000000000 1.000000000 1.000000000
            4 2.000000000 2.000000000 2.000000000 2.000000000 2.000000000 2.000000000
            0.000000000
            3
            ARR 0 1
            ARR 1 1
            ARR 2 1
            1.000000000
            1
            TDN E P PRE 0 0 1.000000000
            2.000000000
            1
            XDN UP 0 100 PRE 1 0
            3.000000000
            1
            TDN E P PRE 1 1 1.000000000
            4.000000000
            1
            XDN UP 1 100 PRE 1 1
            5.000000000
            1
            TDN E P PRE 0 2 1.000000000
            6.000000000
            1
            XDN UP 0 100 PRE 1 2
            7.000000000
            1
            TDN C0 P PROC 0 1 0 0 1.000000000
            8.000000000
            1
            XDN DOWN 0 100 PRE 1 0
            9.000000000
            1
            TDN C1 P PROC 0 1 1 1 1.000000000
            10.000000000
            3
            XDN DOWN 1 100 PRE 1 1
            TDN C0 P PROC 0 1 0 2 1.000000000
            TDN E P POST 0 0 1.000000000
            11.000000000
            1
            TDN E P POST 1 1 1.000000000
            12.000000000
            1
            TDN E D PRE -1 2 0 1 1.333333333
            13.000000000
            1
            XDN UP 0 100 DEC 1 0
            14.000000000
            1
            XDN UP 1 100 DEC 1 1
            15.000000000
            1
            XDN DOWN 0 100 PRE 1 2
            16.000000000
            1
            TDN E P POST 0 2 1.000000000
            17.000000000
            1
            TDN C0 D PROC 0 1 0 1.000000000
            18.000000000
            1
            TDN C1 D PROC 1 1 1 1.000000000
            18.500000000
            1
            TDN E D PRE -1 1 2 1.000000000
            18.600000000
            1
            XDN UP 0 100 DEC 1 2
            18.700000000
            1
            TDN C0 D PROC 0 1 2 1.000000000
            19.000000000
            1
            XDN DOWN 0 100 DEC 1 0
            20.000000000
            1
            XDN DOWN 1 100 DEC 1 1
            22.000000000
            3
            FIN 0
            TDN E D POST -1 2 0 1 1.333333333
            FIN 1
            23.000000000
            1
            XDN DOWN 0 100 DEC 1 2
            24.000000000
            2
            TDN E D POST -1 1 2 1.000000000
            FIN 2
            END
            """
        ),
        clean(
            """
            1
            E P PRE 0 0
            1
            E P PRE 1 1
            1
            C0 P PROC 0 1 0 0
            1
            E P PRE 0 2
            1
            C1 P PROC 0 1 1 1
            0
            0
            1
            C0 P PROC 0 1 0 2
            1
            E P POST 0 0
            0
            1
            E P POST 1 1
            1
            E D PRE -1 2 0 1
            0
            1
            C0 D PROC 0 1 0
            1
            C1 D PROC 1 1 1
            1
            E P POST 0 2
            1
            E D PRE -1 1 2
            0
            0
            0
            1
            C0 D PROC 0 1 2
            0
            0
            1
            E D POST -1 2 0 1
            0
            1
            E D POST -1 1 2
            0
            """
        ),
    )

    run_case(
        binary,
        "one-event transfer-aware remote decode coalescing",
        clean(
            """
            1 1.000000000 2.000000000 1.000000000 100 1
            1000.000000000 1000.000000000 1.000000000 0.000000000 0.000000000 0.500000000 0.500000000
            2
            1 1.000000000 1.000000000 1.000000000 1.000000000 1.000000000 1.000000000
            2 1.000000000 1.000000000 1.000000000 1.000000000 3.500000000 1.000000000
            0.000000000
            2
            ARR 0 1
            ARR 1 1
            1.000000000
            1
            TDN E P PRE 0 0 1.000000000
            2.000000000
            1
            XDN UP 0 100 PRE 1 0
            3.000000000
            1
            TDN E P PRE 0 1 1.000000000
            4.000000000
            2
            TDN C0 P PROC 0 1 0 0 1.000000000
            XDN UP 0 100 PRE 1 1
            5.000000000
            1
            XDN DOWN 0 100 PRE 1 0
            6.000000000
            1
            TDN E P POST 0 0 1.000000000
            7.000000000
            1
            TDN E D PRE -1 1 0 1.000000000
            8.000000000
            1
            TDN C0 P PROC 0 1 0 1 1.000000000
            9.000000000
            1
            XDN DOWN 0 100 PRE 1 1
            10.000000000
            1
            TDN E P POST 0 1 1.000000000
            11.000000000
            1
            TDN E D PRE -1 1 1 1.000000000
            12.000000000
            1
            XDN UP 0 100 DEC 1 0
            13.000000000
            1
            XDN UP 0 100 DEC 1 1
            END
            """
        ),
        clean(
            """
            1
            E P PRE 0 0
            1
            E P PRE 0 1
            1
            C0 P PROC 0 1 0 0
            0
            1
            C0 P PROC 0 1 0 1
            1
            E P POST 0 0
            1
            E D PRE -1 1 0
            0
            0
            1
            E P POST 0 1
            1
            E D PRE -1 1 1
            0
            0
            1
            C0 D PROC 0 2 0 1
            """
        ),
    )

    run_case(
        binary,
        "timing-aware remote decode batch and singleton remainder",
        clean(
            """
            1 1.000000000 2.000000000 1.000000000 100 1
            30.000000000 15.000000000 1.000000000 0.000000000 0.000000000 0.500000000 0.500000000
            3
            1 1.000000000 1.000000000 1.000000000 1.000000000 1.000000000 1.000000000
            2 1.000000000 1.000000000 1.000000000 1.000000000 1.000000000 1.000000000
            3 1.000000000 1.000000000 1.000000000 1.000000000 100.000000000 1.000000000
            0.000000000
            3
            ARR 0 1
            ARR 1 1
            ARR 2 1
            1.000000000
            1
            TDN E P PRE 0 0 1.000000000
            2.000000000
            1
            XDN UP 0 100 PRE 1 0
            3.000000000
            1
            TDN E P PRE 0 1 1.000000000
            4.000000000
            2
            TDN C0 P PROC 0 1 0 0 1.000000000
            XDN UP 0 100 PRE 1 1
            5.000000000
            1
            TDN E P PRE 0 2 1.000000000
            6.000000000
            3
            XDN DOWN 0 100 PRE 1 0
            TDN C0 P PROC 0 1 0 1 1.000000000
            XDN UP 0 100 PRE 1 2
            7.000000000
            2
            XDN DOWN 0 100 PRE 1 1
            TDN E P POST 0 0 1.000000000
            8.000000000
            1
            TDN C0 P PROC 0 1 0 2 1.000000000
            9.000000000
            2
            XDN DOWN 0 100 PRE 1 2
            TDN E P POST 0 1 1.000000000
            10.000000000
            1
            TDN E P POST 0 2 1.000000000
            11.000000000
            1
            TDN E D PRE -1 3 0 1 2 1.000000000
            12.000000000
            1
            XDN UP 0 300 DEC 3 0 1 2
            13.000000000
            1
            TDN C0 D PROC 0 2 0 1 1.000000000
            14.000000000
            1
            XDN DOWN 0 200 DEC 2 0 1
            15.000000000
            1
            TDN C0 D PROC 0 1 2 1.000000000
            16.000000000
            1
            XDN DOWN 0 100 DEC 1 2
            17.000000000
            3
            FIN 0
            TDN E D POST -1 2 0 1 1.000000000
            FIN 1
            18.000000000
            2
            TDN E D POST -1 1 2 1.000000000
            FIN 2
            END
            """
        ),
        clean(
            """
            1
            E P PRE 0 0
            1
            E P PRE 0 1
            1
            C0 P PROC 0 1 0 0
            1
            E P PRE 0 2
            1
            C0 P PROC 0 1 0 1
            0
            2
            C0 P PROC 0 1 0 2
            E P POST 0 0
            1
            E P POST 0 1
            0
            1
            E P POST 0 2
            1
            E D PRE -1 3 0 1 2
            0
            1
            C0 D PROC 0 2 0 1
            1
            C0 D PROC 0 1 2
            1
            E D POST -1 2 0 1
            0
            0
            1
            E D POST -1 1 2
            0
            """
        ),
    )

    run_case(
        binary,
        "least-loaded remote assignment with round-robin ties",
        clean(
            """
            2 1.000000000 0.100000000 100.000000000 100 1
            1000.000000000 1000.000000000 1.000000000 0.000000000 0.000000000 0.500000000 0.500000000
            2
            1 1.000000000 1.000000000 3.000000000 1.000000000 1.000000000 1.000000000
            2 1.000000000 100.000000000 3.000000000 1.000000000 1.000000000 1.000000000
            0.000000000
            3
            ARR 0 1
            ARR 1 2
            ARR 2 1
            2.000000000
            1
            TDN E P PRE 0 0 1.000000000
            2.100000000
            1
            XDN UP 0 100 PRE 1 0
            4.000000000
            1
            TDN E P PRE 1 1 1.000000000
            4.100000000
            2
            TDN C0 P PROC 0 1 0 0 1.000000000
            XDN UP 1 200 PRE 1 1
            4.200000000
            1
            XDN DOWN 0 100 PRE 1 0
            6.000000000
            1
            TDN E P PRE 0 2 1.000000000
            6.100000000
            1
            XDN UP 0 100 PRE 1 2
            8.100000000
            1
            TDN C0 P PROC 0 1 0 2 1.000000000
            8.200000000
            1
            XDN DOWN 0 100 PRE 1 2
            10.000000000
            1
            TDN E P POST 0 0 3.000000000
            14.000000000
            1
            TDN E P POST 0 2 3.000000000
            16.000000000
            1
            TDN E D PRE -1 2 0 2 1.000000000
            16.100000000
            1
            XDN UP 0 200 DEC 2 0 2
            18.100000000
            1
            TDN C0 D PROC 0 2 0 2 1.000000000
            18.200000000
            1
            XDN DOWN 0 200 DEC 2 0 2
            20.200000000
            4
            TDN E D POST -1 2 0 2 1.000000000
            FIN 0
            FIN 2
            ARR 3 1
            """
        ),
        clean(
            """
            1
            E P PRE 0 0
            1
            E P PRE 1 1
            1
            C0 P PROC 0 1 0 0
            1
            E P PRE 0 2
            1
            C1 P PROC 0 1 1 1
            0
            1
            E P POST 0 0
            1
            C0 P PROC 0 1 0 2
            0
            0
            1
            E P POST 0 2
            1
            E D PRE -1 2 0 2
            0
            1
            C0 D PROC 0 2 0 2
            0
            1
            E D POST -1 2 0 2
            1
            E P PRE 0 3
            """
        ),
    )

    run_case(
        binary,
        "contended prefill splits once and yields to decode",
        clean(
            """
            1 1.000000000 2.000000000 1.000000000 100 4
            5.000000000 5.000000000 1.000000000 0.000000000 0.000000000 0.500000000 0.500000000
            2
            1 1.000000000 5.000000000 1.000000000 1.000000000 1.000000000 1.000000000
            2 1.000000000 5.000000000 1.000000000 1.000000000 1.000000000 1.000000000
            0.000000000
            2
            ARR 0 1
            ARR 1 1
            1.000000000
            1
            TDN E P PRE 0 0 1.000000000
            2.000000000
            1
            XDN UP 0 100 PRE 1 0
            3.000000000
            1
            TDN E P PRE 0 1 1.000000000
            4.000000000
            2
            TDN C0 P PROC 0 4 0 0 5.000000000
            XDN UP 0 100 PRE 1 1
            5.000000000
            1
            XDN DOWN 0 100 PRE 1 0
            10.000000000
            1
            ARR 2 1
            20.000000000
            1
            TDN E P POST 0 0 1.000000000
            21.000000000
            1
            TDN E P PRE 0 2 1.000000000
            22.000000000
            1
            TDN E D PRE -1 1 0 1.000000000
            23.000000000
            1
            XDN UP 0 100 PRE 1 2
            24.000000000
            1
            XDN UP 0 100 DEC 1 0
            30.000000000
            1
            TDN C0 P PROC 0 4 0 1 5.000000000
            31.500000000
            1
            TDN C0 P PROC 0 2 0 2 2.500000000
            32.000800000
            1
            XDN DOWN 0 100 PRE 1 1
            33.500000000
            1
            TDN C0 D PROC 0 1 0 1.000000000
            34.000800000
            1
            TDN E P POST 0 1 1.000000000
            35.000000000
            1
            TDN C0 P PROC 2 4 0 2 2.500000000
            36.000800000
            1
            TDN E D PRE -1 1 1 1.000000000
            36.500000000
            1
            XDN DOWN 0 100 DEC 1 0
            37.000800000
            1
            XDN DOWN 0 100 PRE 1 2
            38.500000000
            2
            TDN E D POST -1 1 0 1.000000000
            FIN 0
            """
        ),
        clean(
            """
            1
            E P PRE 0 0
            1
            E P PRE 0 1
            1
            C0 P PROC 0 4 0 0
            0
            1
            C0 P PROC 0 4 0 1
            1
            E P POST 0 0
            0
            1
            E P PRE 0 2
            1
            E D PRE -1 1 0
            0
            0
            0
            1
            C0 P PROC 0 2 0 2
            1
            C0 D PROC 0 1 0
            1
            E P POST 0 1
            1
            C0 P PROC 2 4 0 2
            1
            E D PRE -1 1 1
            0
            0
            1
            E D POST -1 1 0
            0
            1
            E P POST 0 2
            """
        ),
    )

    run_case(
        binary,
        "deadline aging prevents local and remote prefill starvation",
        clean(
            """
            1 1.000000000 2.000000000 1.000000000 100 1
            5.000000000 5.000000000 1.000000000 0.000000000 0.000000000 0.500000000 0.500000000
            2
            1 1.000000000 1.000000000 1.000000000 1.000000000 1.000000000 1.000000000
            2 1.000000000 1.000000000 1.000000000 1.000000000 1.000000000 1.000000000
            0.000000000
            2
            ARR 0 1
            ARR 1 1
            1.000000000
            1
            TDN E P PRE 0 0 1.000000000
            2.000000000
            1
            XDN UP 0 100 PRE 1 0
            3.000000000
            1
            TDN E P PRE 0 1 1.000000000
            4.000000000
            2
            TDN C0 P PROC 0 1 0 0 1.000000000
            XDN UP 0 100 PRE 1 1
            5.000000000
            1
            XDN DOWN 0 100 PRE 1 0
            10.000000000
            1
            ARR 2 1
            20.000000000
            1
            TDN E P POST 0 0 1.000000000
            21.000000000
            1
            TDN E P PRE 0 2 1.000000000
            22.000000000
            1
            TDN E D PRE -1 1 0 1.000000000
            23.000000000
            1
            XDN UP 0 100 PRE 1 2
            24.000000000
            1
            XDN UP 0 100 DEC 1 0
            30.000000000
            1
            TDN C0 P PROC 0 1 0 1 1.000000000
            31.000000000
            1
            XDN DOWN 0 100 PRE 1 1
            32.000000000
            1
            TDN E P POST 0 1 1.000000000
            33.000000000
            1
            TDN E D PRE -1 1 1 1.000000000
            34.000000000
            1
            XDN UP 0 100 DEC 1 1
            40.000000000
            1
            TDN C0 P PROC 0 1 0 2 1.000000000
            41.000000000
            1
            XDN DOWN 0 100 PRE 1 2
            42.000000000
            1
            TDN E P POST 0 2 1.000000000
            43.000000000
            2
            TDN C0 D PROC 0 2 0 1 1.000000000
            TDN E D PRE -1 1 2 1.000000000
            44.000000000
            1
            XDN UP 0 100 DEC 1 2
            45.000000000
            1
            XDN DOWN 0 200 DEC 2 0 1
            46.000000000
            1
            TDN C0 D PROC 0 1 2 1.000000000
            47.000000000
            1
            XDN DOWN 0 100 DEC 1 2
            48.000000000
            3
            FIN 0
            TDN E D POST -1 2 0 1 1.000000000
            FIN 1
            49.000000000
            2
            TDN E D POST -1 1 2 1.000000000
            FIN 2
            END
            """
        ),
        clean(
            """
            1
            E P PRE 0 0
            1
            E P PRE 0 1
            1
            C0 P PROC 0 1 0 0
            0
            1
            C0 P PROC 0 1 0 1
            1
            E P POST 0 0
            0
            1
            E P PRE 0 2
            1
            E D PRE -1 1 0
            0
            0
            0
            1
            C0 P PROC 0 1 0 2
            1
            E P POST 0 1
            1
            E D PRE -1 1 1
            0
            0
            1
            C0 D PROC 0 2 0 1
            1
            E P POST 0 2
            1
            E D PRE -1 1 2
            0
            1
            C0 D PROC 0 1 2
            1
            E D POST -1 2 0 1
            0
            0
            1
            E D POST -1 1 2
            0
            """
        ),
    )

    run_case(
        binary,
        "prefill admission remains work-conserving",
        clean(
            """
            1 1.000000000 2.000000000 1.000000000 1000000 1
            100000.000000000 10.000000000 1.000000000 0.000000000 0.000000000 0.000000000 1.000000000
            2
            1 1.000000000 1.000000000 1.000000000 1.000000000 1.000000000 1.000000000
            4096 1.000000000 1.000000000 1.000000000 1.000000000 1.000000000 1.000000000
            0.000000000
            1
            ARR 0 1
            1.000000000
            1
            TDN E P PRE 0 0 1.000000000
            2.000000000
            1
            XDN UP 0 1000000 PRE 1 0
            3.000000000
            1
            TDN C0 P PROC 0 1 0 0 1.000000000
            4.000000000
            1
            XDN DOWN 0 1000000 PRE 1 0
            5.000000000
            1
            TDN E P POST 0 0 1.000000000
            6.000000000
            1
            TDN E D PRE -1 1 0 1.000000000
            7.000000000
            2
            XDN UP 0 1000000 DEC 1 0
            ARR 1 4096
            END
            """
        ),
        clean(
            """
            1
            E P PRE 0 0
            0
            1
            C0 P PROC 0 1 0 0
            0
            1
            E P POST 0 0
            1
            E D PRE -1 1 0
            0
            2
            C0 D PROC 0 1 0
            E P PRE 0 1
            """
        ),
    )

    run_case(
        binary,
        "prefill process completes without final-layer hold",
        clean(
            """
            2 1.000000000 2.000000000 1.000000000 100 2
            1000000000.000000000 10.000000000 1.000000000 0.000000000 0.000000000 0.000000000 1.000000000
            2
            1 1.000000000 1.000000000 1.000000000 1.000000000 1.000000000 1.000000000
            2 1.000000000 1.000000000 1.000000000 1.000000000 1.000000000 1.000000000
            0.000000000
            2
            ARR 0 1
            ARR 1 1
            1.000000000
            1
            TDN E P PRE 0 0 1.000000000
            2.000000000
            1
            XDN UP 0 100 PRE 1 0
            3.000000000
            2
            TDN E P PRE 1 1 1.000000000
            TDN C0 P PROC 0 2 0 0 1.000000000
            4.000000000
            1
            XDN DOWN 0 100 PRE 1 0
            5.000000000
            1
            TDN E P POST 0 0 1.000000000
            6.000000000
            1
            TDN E D PRE -1 1 0 1.000000000
            7.000000000
            1
            XDN UP 1 100 PRE 1 1
            END
            """
        ),
        clean(
            """
            1
            E P PRE 0 0
            1
            E P PRE 1 1
            1
            C0 P PROC 0 2 0 0
            0
            1
            E P POST 0 0
            1
            E D PRE -1 1 0
            0
            1
            C1 P PROC 0 2 1 1
            """
        ),
    )

    run_case(
        binary,
        "hard deadlines ignore projected remaining work",
        clean(
            """
            1 1.000000000 2.000000000 1.000000000 1000000 1
            50000.000000000 1000000000.000000000 1.000000000 0.000000000 0.000000000 0.000000000 1.000000000
            2
            1 1.000000000 1.000000000 1.000000000 1.000000000 1.000000000 1.000000000
            4096 1.000000000 1.000000000 1.000000000 1.000000000 1.000000000 1.000000000
            0.000000000
            1
            ARR 0 1
            1.000000000
            1
            TDN E P PRE 0 0 1.000000000
            2.000000000
            1
            XDN UP 0 1000000 PRE 1 0
            3.000000000
            1
            TDN C0 P PROC 0 1 0 0 1.000000000
            4.000000000
            1
            XDN DOWN 0 1000000 PRE 1 0
            5.000000000
            2
            TDN E P POST 0 0 1.000000000
            ARR 1 4096
            END
            """
        ),
        clean(
            """
            1
            E P PRE 0 0
            0
            1
            C0 P PROC 0 1 0 0
            0
            1
            E P POST 0 0
            1
            E D PRE -1 1 0
            """
        ),
    )

    print("11 scheduler regression cases passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
