#!/usr/bin/env python3
"""Deterministic train/holdout A/B corpus for scheduler policy changes."""
import math
import random
import statistics
import sys
from dataclasses import replace

from simulator import Scenario, simulate


def log_uniform(rng, low, high):
    return math.exp(rng.uniform(math.log(low), math.log(high)))


def raw_cases(seed, count):
    rng = random.Random(seed)
    sizes = (1, 2, 4, 8, 16, 32, 64, 128, 256, 1024)
    for _ in range(count):
        remotes = rng.choice((1, 2, 4, 8))
        requests = rng.randint(4, 48)
        schedule = log_uniform(rng, 0.05, 8.0)
        latency = log_uniform(rng, 0.005, 50.0)
        bandwidth = log_uniform(rng, 0.02, 100.0)
        bytes_per_token = rng.choice((1, 256, 4096, 125000, 1000000))
        layers = rng.choice((1, 4, 16, 64))
        pre_scale = log_uniform(rng, 0.05, 100.0)
        dec_scale = log_uniform(rng, 0.05, 40.0)
        edge_alpha = rng.uniform(0.4, 1.1)
        proc_alpha = rng.uniform(0.45, 1.1)
        rows = []
        for size in sizes:
            rows.append((
                size,
                0.1 + 0.04 * pre_scale * size**0.6,
                0.2 + pre_scale * size**0.68,
                0.1 + 0.03 * pre_scale * size**0.58,
                0.1 + dec_scale * size**edge_alpha,
                0.2 + 2.0 * dec_scale * size**proc_alpha,
                0.1 + 0.8 * dec_scale * size**edge_alpha,
            ))
        spacing = rng.choice((0.0, 0.0, 0.1, 2.0, 10.0))
        now = 0.0
        arrivals = []
        for rid in range(requests):
            if spacing:
                now += rng.uniform(0.0, 2.0 * spacing)
            arrivals.append((now, rng.choice(sizes), rng.randint(1, 48)))
        yield Scenario(
            remotes, schedule, latency, bandwidth, bytes_per_token, layers,
            1.0, 1.0, 1.0, 0.0, 1.0, 0.75, 0.25, rows, arrivals)


def calibrated_cases(binary, seed, count):
    for cfg in raw_cases(seed, count):
        baseline = simulate(cfg, [binary])
        if not baseline.ok:
            yield cfg
            continue
        # Keep both score components away from trivial clamp regions. The
        # calibration depends only on the frozen baseline, never the candidate.
        tp_ub = max(baseline.tp * 1.35, 1e-9)
        tp_base = baseline.tp * 0.15
        dist_base = max(baseline.dist * 1.8, 0.05)
        slo1 = baseline.tdr / rng_factor(seed, len(baseline.per_request), 0)
        slo2 = max(baseline.tpot, 1e-6) / rng_factor(seed, len(baseline.per_request), 1)
        weight = (0.5, 0.75, 0.9, 1.0)[(seed + len(baseline.per_request)) % 4]
        yield replace(cfg, tp_ub=tp_ub, tp_base=tp_base,
                      dist_base=dist_base, slo1=max(slo1, 1e-6),
                      slo2=max(slo2, 1e-6), w_tp=weight, w_c=1.0-weight)


def rng_factor(seed, count, salt):
    rng = random.Random((seed << 20) ^ (count << 4) ^ salt)
    return rng.choice((0.7, 1.0, 1.4))


def main():
    if len(sys.argv) not in (3, 4):
        raise SystemExit("usage: benchmark_scheduler.py BASELINE CANDIDATE [train|holdout]")
    baseline, candidate = sys.argv[1:3]
    suite = sys.argv[3] if len(sys.argv) == 4 else "train"
    if suite not in ("train", "holdout"):
        raise SystemExit("suite must be train or holdout")
    seed, count = ((2251001, 96) if suite == "train" else (9918273, 96))
    rows = []
    invalid = []
    for index, cfg in enumerate(calibrated_cases(baseline, seed, count)):
        old = simulate(cfg, [baseline])
        new = simulate(cfg, [candidate])
        if not old.ok or not new.ok:
            invalid.append((index, old.error, new.error))
            continue
        rows.append((new.score-old.score, new.elapsed/old.elapsed,
                     index, old, new))
    deltas = [row[0] for row in rows]
    ratios = [row[1] for row in rows]
    losses = sorted(deltas)[:max(1, len(deltas)//10)]
    print(f"suite={suite} cases={len(rows)} invalid={len(invalid)} "
          f"wins={sum(x>1e-8 for x in deltas)} "
          f"losses={sum(x<-1e-8 for x in deltas)} "
          f"ties={sum(abs(x)<=1e-8 for x in deltas)}")
    if rows:
        print(f"score mean_delta={statistics.mean(deltas):.6f} "
              f"total_delta={sum(deltas):.6f} "
              f"worst10_mean={statistics.mean(losses):.6f}")
        print(f"elapsed mean_ratio={statistics.mean(ratios):.6f}")
        for delta, ratio, index, old, new in sorted(rows)[:8]:
            print(f"worst case={index:03d} delta={delta:+.6f} "
                  f"elapsed_ratio={ratio:.6f} "
                  f"old=({old.summary()}) new=({new.summary()})")
    for failure in invalid:
        print("INVALID", failure)
    if invalid:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
