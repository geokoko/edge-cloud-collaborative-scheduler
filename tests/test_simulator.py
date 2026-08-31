#!/usr/bin/env python3
"""Regression tests for the local interactor and scorer in simulator.py."""
import math
import sys
from collections import Counter
from dataclasses import replace
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from simulator import (D_PRE, D_PROC, P_PRE, Scenario, lookup, score, simulate)

# One row per listed group size; -1.0 marks a step the table omits.
EXAMPLE_ROWS = [(1, 3.0, 10.0, 2.0, 1.0, 4.0, 1.0),
                (4, 3.0, 10.0, 2.0, 1.0, 4.0, 1.0)]
FLAT_ROWS = [(1, 3.0, 10.0, 2.0, 1.0, 4.0, 1.0)]


def scenario(**overrides):
    """The public example's system, with per-test overrides."""
    base = dict(remote_count=1, schedule_cost=1.0, latency_ms=2.0,
                bandwidth_gbps=1.0, bytes_per_token=125000, num_layers=4,
                slo1=30.0, slo2=15.0, tp_ub=0.0625, tp_base=0.022222222,
                dist_base=0.0, w_tp=0.5, w_c=0.5, rows=EXAMPLE_ROWS,
                arrivals=[(0.0, 4, 1)])
    base.update(overrides)
    return Scenario(**base)


# --- a scriptable stand-in scheduler, so simulator tests never depend on main.cpp
PRELUDE = """
import sys
def rd(): return sys.stdin.readline()
rd(); rd()
for _ in range(int(rd())): rd()
def frames():
    while True:
        head = rd()
        if not head or head.strip() == 'END': return
        yield [rd() for _ in range(int(rd()))]
def say(text):
    sys.stdout.write(text)
    sys.stdout.flush()
"""


def fake(body):
    return [sys.executable, "-c", PRELUDE + body]


def frames(result):
    """Parse a transcript into [(timestamp, [event lines])], skipping startup."""
    lines = result.transcript.splitlines()
    cursor = 3 + int(lines[2])
    parsed = []
    while cursor < len(lines) and lines[cursor] != "END":
        count = int(lines[cursor + 1])
        parsed.append((float(lines[cursor]), lines[cursor + 2:cursor + 2 + count]))
        cursor += 2 + count
    return parsed


def check_invariants(result):
    """Protocol properties every successful run must satisfy."""
    cfg = result.scenario
    times = [time for time, _ in frames(result)]
    assert all(b >= a for a, b in zip(times, times[1:])), \
        f"printed frame timestamps must be nondecreasing: {times}"

    finish = {}          # direction -> last completion, to prove one-at-a-time FIFO
    tokens = {}
    for time, events in frames(result):
        for line in events:
            fields = line.split()
            if fields[0] == "ARR":
                assert len(fields) == 3, f"ARR must not leak the output length: {line}"
            elif fields[0] == "XDN":
                direction, size = fields[1], int(fields[3])
                duration = cfg.transfer_ms(size // cfg.bytes_per_token)
                previous = finish.get(direction, -math.inf)
                assert time >= previous + duration - 1e-9, \
                    f"{direction} transfers overlap: {previous} then {line} at {time}"
                finish[direction] = time
            elif fields[0] == "TDN" and fields[2:4] == ["D", "POST"]:
                for rid in fields[6:6 + int(fields[5])]:
                    tokens[int(rid)] = tokens.get(int(rid), 0) + 1
            elif fields[0] == "FIN":
                rid = int(fields[1])
                assert tokens.get(rid) == cfg.arrivals[rid][2], \
                    f"FIN for {rid} must accompany its final D POST"
    for rid, (_, _, lout) in enumerate(cfg.arrivals):
        assert tokens.get(rid) == lout, f"request {rid} produced {tokens.get(rid)}/{lout}"


def check(condition, message):
    if not condition:
        raise AssertionError(message)


# --- pure helpers -----------------------------------------------------------
def test_lookup():
    check(lookup([(4, 7.0)], 1) == 7.0, "a single listed size is flat below it")
    check(lookup([(4, 7.0)], 99) == 7.0, "a single listed size is flat above it")
    curve = [(1, 10.0), (5, 30.0)]
    check(lookup(curve, 1) == 10.0, "exact hit on the first size")
    check(lookup(curve, 5) == 30.0, "exact hit on the last size")
    check(lookup(curve, 3) == 20.0, "midpoint interpolates linearly")
    check(lookup(curve, 2) == 15.0, "quarter point interpolates linearly")
    check(lookup(curve, 0) == 10.0, "below the table clamps to the first time")
    check(lookup(curve, 9) == 30.0, "above the table clamps to the last time")


def test_missing_table_entries():
    rows = [(1, 3.0, 10.0, 2.0, -1.0, 4.0, 1.0),
            (8, -1.0, -1.0, -1.0, 5.0, 12.0, -1.0)]
    cfg = scenario(rows=rows)
    check(cfg.curve(D_PRE) == [(8, 5.0)], "-1 entries are skipped")
    check(cfg.task_time(D_PRE, 1) == 5.0, "a lone entry applies at every size")
    check(cfg.task_time(P_PRE, 8) == 3.0, "prefill columns clamp above the table")
    check(cfg.task_time(D_PROC, 4) == 4.0 + 3 / 7 * 8.0, "decode_proc interpolates")
    try:
        scenario(rows=[(1, 3.0, 10.0, 2.0, -1.0, 4.0, 1.0)]).curve(D_PRE)
    except ValueError:
        pass
    else:
        raise AssertionError("an empty column must be rejected")


def test_transfer_time():
    cfg = scenario()
    check(cfg.transfer_ms(4) == 6.0, "4 tokens: 2ms latency + 8*500000/1e6 bits")
    check(cfg.transfer_ms(1) == 3.0, "1 token: 2ms latency + 8*125000/1e6 bits")


def test_score():
    cfg = scenario(slo1=100.0, slo2=50.0, tp_ub=2.0, tp_base=1.0, dist_base=0.0)
    check(score(cfg, 1.5, 100.0, 50.0) == (0.0, 0.5, 1.0, 750.0),
          "meeting both targets with dist_base=0 scores a full waiting component")
    check(score(cfg, 1.5, 100.0, 50.001)[2] == 0.0,
          "dist_base=0 is all-or-nothing")
    check(score(cfg, 0.4, 10.0, 10.0)[1] == 0.0, "throughput clamps at the base")
    check(score(cfg, 99.0, 10.0, 10.0)[1] == 1.0, "throughput clamps at the bound")

    graded = scenario(slo1=100.0, slo2=50.0, tp_ub=2.0, tp_base=1.0,
                      dist_base=2.0, w_tp=0.0, w_c=1.0)
    # excess_tdr = 0.6, excess_tpot = 0.8 -> dist = 1.0 -> norm_c = 1 - 1/2.
    dist, _, norm_c, points = score(graded, 0.0, 160.0, 90.0)
    check(abs(dist - 1.0) < 1e-12, f"3-4-5 excesses give dist 1.0, got {dist}")
    check(abs(norm_c - 0.5) < 1e-12, f"norm_c is 1 - dist/dist_base, got {norm_c}")
    check(abs(points - 500.0) < 1e-9, f"points, got {points}")
    check(score(graded, 0.0, 1000.0, 1000.0)[2] == 0.0, "norm_c clamps at zero")


# --- end-to-end against the real scheduler ----------------------------------
def test_public_example(binary):
    fixtures = Path(__file__).parent / "fixtures"
    result = simulate(scenario(), [binary])
    check(result.ok, f"public example failed: {result.error}")
    check(result.transcript == (fixtures / "public_example.in").read_text(),
          "the generated transcript must match the published example byte for byte")
    check(result.responses == (fixtures / "public_example.out").read_text(),
          "the scheduler's replies must match the published example")
    # The judge reported points=500.0000027586 for this exact configuration.
    check(abs(result.score - 500.0000027586) < 1e-9,
          f"expected the judge's score, got {result.score!r}")
    check(result.tdr == 30.0 and result.tpot == 0.0 and result.elapsed == 45.0,
          f"hand-computed metrics, got tdr={result.tdr} tpot={result.tpot}")
    check(frames(result)[-1][1] == ["TDN E D POST -1 1 0 1.000000000", "FIN 0"],
          "events sharing a timestamp are coalesced into one frame, FIN last")
    check_invariants(result)


def test_single_layer_and_repeated_tokens(binary):
    # One request, three tokens, num_layers=1. Every decode iteration costs
    # (S + d_pre) + up + (S + d_proc) + down + (S + d_post) = 2+3+5+3+2 = 15ms.
    result = simulate(scenario(num_layers=1, rows=FLAT_ROWS,
                               arrivals=[(0.0, 1, 3)]), [binary])
    check(result.ok, f"single-layer run failed: {result.error}")
    check_invariants(result)
    check(result.tdr == 24.0, f"hand-computed TDR 24.0, got {result.tdr}")
    check(result.tpot == 15.0, f"hand-computed TPOT 15.0, got {result.tpot}")
    check(result.elapsed == 69.0, f"hand-computed makespan 69.0, got {result.elapsed}")
    check(abs(result.tp - 3 / 69.0) < 1e-12, f"tp is tokens/elapsed, got {result.tp}")


def test_one_token_request_has_no_gap(binary):
    result = simulate(scenario(num_layers=1, rows=FLAT_ROWS,
                               arrivals=[(0.0, 1, 1), (0.0, 1, 1)]), [binary])
    check(result.ok, f"one-token run failed: {result.error}")
    check(result.tpot == 0.0, f"single-token requests contribute no gap, got {result.tpot}")
    check_invariants(result)


def test_hidden_output_length_cannot_change_visible_prefix(binary):
    short = simulate(scenario(num_layers=1, rows=FLAT_ROWS,
                              arrivals=[(0.0, 1, 1)]), [binary])
    long = simulate(scenario(num_layers=1, rows=FLAT_ROWS,
                             arrivals=[(0.0, 1, 3)]), [binary])
    check(short.ok and long.ok,
          f"hidden-length runs failed: {short.error}, {long.error}")
    visible = short.responses.splitlines()[:-1]
    check(visible == long.responses.splitlines()[:len(visible)],
          "different hidden output lengths changed the schedule before FIN")


def test_shortest_prefill_is_admitted_first(binary):
    rows = [(1, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0),
            (8, 8.0, 80.0, 8.0, 1.0, 1.0, 1.0)]
    result = simulate(scenario(num_layers=1, rows=rows,
                               bytes_per_token=1, slo1=1000.0, slo2=1000.0,
                               arrivals=[(0.0, 8, 1), (0.0, 1, 1)]), [binary])
    check(result.ok, f"shortest-prefill run failed: {result.error}")
    starts = [line for line in result.responses.splitlines()
              if line.startswith("E P PRE")]
    check(starts[0] == "E P PRE 0 1",
          f"the shorter prefill must be admitted first, got {starts}")
    check(result.tdr < 60.0,
          f"short-first should keep this workload's mean TDR below 60ms, got {result.tdr}")
    check_invariants(result)


def test_shortest_prefill_does_not_depend_on_score_weight(binary):
    rows = [(1, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0),
            (8, 1.02, 1.02, 1.02, 1.0, 1.0, 1.0)]
    starts = {}
    for weight in (0.75, 0.9):
        result = simulate(
            scenario(num_layers=1, rows=rows, bytes_per_token=1,
                     slo1=1000.0, slo2=1000.0,
                     w_tp=weight, w_c=1.0 - weight,
                     arrivals=[(0.0, 8, 1), (0.0, 1, 1)]), [binary])
        check(result.ok, f"prefill tradeoff run failed: {result.error}")
        starts[weight] = next(line for line in result.responses.splitlines()
                              if line.startswith("E P PRE"))
        check_invariants(result)
    check(starts == {0.75: "E P PRE 0 1", 0.9: "E P PRE 0 1"},
          f"score weights must not create test-specific admission: {starts}")

    boundary = simulate(
        scenario(num_layers=1, bytes_per_token=1,
                 rows=[(1, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0),
                       (8, 2.5, 2.5, 2.5, 1.0, 1.0, 1.0)],
                 slo1=1000.0, slo2=1000.0, w_tp=0.75, w_c=0.25,
                 arrivals=[(0.0, 8, 1), (0.0, 1, 1)]), [binary])
    check(boundary.ok, f"prefill boundary run failed: {boundary.error}")
    first = next(line for line in boundary.responses.splitlines()
                 if line.startswith("E P PRE"))
    check(first == "E P PRE 0 1",
          f"the shorter prefill should remain first: {first}")
    check_invariants(boundary)


def test_overdue_prefill_beats_shorter_new_arrival(binary):
    rows = [(1, 20.0, 1.0, 1.0, 1.0, 1.0, 1.0),
            (8, 20.0, 80.0, 8.0, 1.0, 1.0, 1.0)]
    result = simulate(scenario(num_layers=1, rows=rows,
                               bytes_per_token=1, slo1=6.0, slo2=1000.0,
                               arrivals=[(0.0, 1, 1), (1.0, 8, 1),
                                         (18.0, 1, 1)]), [binary])
    check(result.ok, f"aged-prefill run failed: {result.error}")
    starts = [line for line in result.responses.splitlines()
              if line.startswith("E P PRE")]
    check(starts[:2] == ["E P PRE 0 0", "E P PRE 0 1"],
          f"the overdue prefill must retain priority, got {starts}")
    check_invariants(result)


def test_score_scaled_aging_keeps_shortest_prefill(binary):
    rows = [(1, 20.0, 1.0, 1.0, 1.0, 1.0, 1.0),
            (8, 20.0, 80.0, 8.0, 1.0, 1.0, 1.0)]
    result = simulate(scenario(num_layers=1, rows=rows,
                               bytes_per_token=1, slo1=10.0, slo2=1000.0,
                               dist_base=100.0,
                               arrivals=[(0.0, 1, 1), (1.0, 8, 1),
                                         (18.0, 1, 1)]), [binary])
    check(result.ok, f"score-scaled aging run failed: {result.error}")
    starts = [line for line in result.responses.splitlines()
              if line.startswith("E P PRE")]
    check(starts == ["E P PRE 0 0", "E P PRE 0 2", "E P PRE 0 1"],
          f"a large distance budget should retain shortest-first order: {starts}")
    check(result.tdr < 90.0,
          f"short-first should keep mean TDR below 90ms, got {result.tdr}")
    check_invariants(result)


def test_prefill_aging_does_not_preempt_ready_post(binary):
    rows = [(1, 10.0, 1.0, 1.0, 10.0, 2.0, 1.0),
            (16, 200.0, 4.0, 2.0, 40.0, 8.0, 8.0)]
    result = simulate(scenario(remote_count=2, schedule_cost=1.0,
                               latency_ms=0.1, bandwidth_gbps=10.0,
                               bytes_per_token=1000, num_layers=16,
                               slo1=20.0, slo2=5.0, tp_ub=1.0,
                               tp_base=0.0, dist_base=100.0,
                               w_tp=0.5, w_c=0.5, rows=rows,
                               arrivals=[(0.0, 16, 1), (0.0, 1, 1),
                                         (1.1, 1, 1)]), [binary])
    check(result.ok, f"P PRE aging run failed: {result.error}")
    edge_prefill = [line for line in result.responses.splitlines()
                    if line.startswith("E P PRE") or
                    line.startswith("E P POST")]
    check(edge_prefill[:4] == ["E P PRE 0 1", "E P PRE 1 2",
                               "E P POST 0 1", "E P PRE 0 0"],
          f"ready P POST should precede P PRE within its aging budget: {edge_prefill}")
    check(result.tdr < 200.0,
          f"consistent P PRE aging should keep mean TDR below 200ms: {result.tdr}")
    check(result.elapsed <= 259.2016 + 1e-6,
          f"the ordering change must not regress makespan: {result.elapsed}")
    check_invariants(result)

    throughput_only = simulate(
        replace(result.scenario, w_tp=1.0, w_c=0.0), [binary])
    check(throughput_only.ok,
          f"throughput-only P PRE aging failed: {throughput_only.error}")
    edge_prefill = [line for line in throughput_only.responses.splitlines()
                    if line.startswith("E P PRE") or
                    line.startswith("E P POST")]
    check(edge_prefill[2] == "E P PRE 0 0",
          f"throughput-only arbitration should remain unchanged: {edge_prefill}")
    check_invariants(throughput_only)


def test_uplink_queues_behind_a_transfer_in_flight(binary):
    # 1 token = 10ms latency + 8ms of bits = 18ms on the wire, while P PRE
    # takes only S+1 = 2ms, so the second uplink must queue behind the first.
    rows = [(1, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)]
    result = simulate(scenario(remote_count=2, latency_ms=10.0, bytes_per_token=10**6,
                               num_layers=1, rows=rows, slo1=1e6, slo2=1e6,
                               arrivals=[(0.0, 1, 1), (0.0, 1, 1)]), [binary])
    check(result.ok, f"contended-uplink run failed: {result.error}")
    check_invariants(result)
    starts = [time for time, events in frames(result)
              for line in events if line.startswith("XDN UP") and "PRE" in line]
    check(starts == [20.0, 38.0],
          f"hand-computed uplink completions [20.0, 38.0], got {starts}")


def test_decode_group_splits_by_remote(binary):
    result = simulate(scenario(remote_count=4, num_layers=1, rows=FLAT_ROWS,
                               slo1=1e6, slo2=1e6,
                               arrivals=[(0.0, 1, 2)] * 4), [binary])
    check(result.ok, f"multi-remote run failed: {result.error}")
    check_invariants(result)

    placement = {}       # rid -> remote, as fixed by each P PRE
    uplinks = 0
    for _, events in frames(result):
        for line in events:
            fields = line.split()
            if fields[0] == "TDN" and fields[2:4] == ["P", "PRE"]:
                placement[int(fields[5])] = int(fields[4])
            elif fields[0] == "XDN" and fields[1] == "UP" and fields[4] == "DEC":
                uplinks += 1
                remote, members = int(fields[2]), [int(r) for r in fields[6:]]
                check(len(members) == int(fields[5]), f"member count matches: {line}")
                check([placement[rid] for rid in members] == [remote] * len(members),
                      f"a decode uplink carries only C{remote}'s members: {line}")
    check(len(set(placement.values())) > 1, "the run must span more than one remote")
    check(uplinks >= 2, f"expected several decode uplinks, saw {uplinks}")


def test_prefill_placement_balances_remaining_compute(binary):
    rows = [(1, 0.1, 1.0, 0.1, 0.1, 0.1, 0.1),
            (60, 0.1, 60.0, 0.1, 0.1, 0.1, 0.1),
            (100, 0.1, 100.0, 0.1, 0.1, 0.1, 0.1)]
    result = simulate(
        scenario(remote_count=2, schedule_cost=0.1, latency_ms=0.001,
                 bandwidth_gbps=1000.0, bytes_per_token=1, num_layers=1,
                 rows=rows, slo1=1000.0, slo2=1000.0,
                 dist_base=100.0, w_tp=0.5, w_c=0.5,
                 arrivals=[(0.0, 100, 1), (1.0, 1, 1), (1.0, 60, 1)]),
        [binary])
    check(result.ok, f"weighted-placement run failed: {result.error}")
    placements = [line.split()[3] for line in result.responses.splitlines()
                  if line.startswith("E P PRE")]
    check(placements == ["0", "1", "1"],
          f"the medium prefill should avoid the busy remote: {placements}")
    check(result.elapsed < 102.0,
          f"balanced remote work should finish near 101.1ms: {result.elapsed}")
    check_invariants(result)


def test_ready_decode_runs_before_late_uplink(binary):
    rows = [(1, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0),
            (2, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)]
    result = simulate(scenario(schedule_cost=0.5, latency_ms=5.3,
                               bandwidth_gbps=1000.0, bytes_per_token=1,
                               num_layers=1, rows=rows,
                               slo1=1000.0, slo2=1000.0,
                               dist_base=100.0, w_tp=1.0, w_c=0.0,
                               arrivals=[(0.0, 1, 2), (0.1, 1, 1)]),
                      [binary])
    check(result.ok, f"late-uplink run failed: {result.error}")
    decode_proc = [line for line in result.responses.splitlines()
                   if line.startswith("C0 D PROC")]
    check(decode_proc[0] == "C0 D PROC 0 1 0",
          f"ready decode should run before the late uplink: {decode_proc}")
    check(abs(result.elapsed - 45.300000048) < 1e-9,
          f"overlapping compute with the uplink should finish at 45.3ms: "
          f"{result.elapsed}")
    check_invariants(result)


def test_decode_post_waits_only_when_batch_reduces_drain_time(binary):
    def run(decode_post_for_two):
        rows = [(1, 0.1, 20.0, 0.1, 0.1, 1.0, 10.0),
                (2, 0.1, 20.0, 0.1, 0.1, 1.0, decode_post_for_two)]
        return simulate(
            scenario(remote_count=2, schedule_cost=0.1, latency_ms=0.1,
                     bandwidth_gbps=1.0, bytes_per_token=500000,
                     num_layers=1, rows=rows, slo1=1e6, slo2=1e6,
                     tp_ub=1.0, tp_base=0.0, dist_base=100.0,
                     w_tp=1.0, w_c=0.0,
                     arrivals=[(0.0, 1, 1), (0.0, 1, 1)]),
            [binary])

    slow_batch = run(19.0)
    check(slow_batch.ok, f"slow-batch decode run failed: {slow_batch.error}")
    slow_posts = [line for line in slow_batch.responses.splitlines()
                  if line.startswith("E D POST")]
    check(slow_posts[0] == "E D POST -1 1 0",
          f"a slower combined drain should start immediately: {slow_posts}")
    check(abs(slow_batch.elapsed - 58.4) < 1e-9,
          f"two immediate singletons should finish at 58.4ms: "
          f"{slow_batch.elapsed}")
    check_invariants(slow_batch)

    fast_batch = run(15.0)
    check(fast_batch.ok, f"fast-batch decode run failed: {fast_batch.error}")
    fast_posts = [line for line in fast_batch.responses.splitlines()
                  if line.startswith("E D POST")]
    check(fast_posts == ["E D POST -1 2 0 1"],
          f"a faster combined drain should wait for its second member: "
          f"{fast_posts}")
    check(abs(fast_batch.elapsed - 57.4) < 1e-9,
          f"the beneficial size-2 batch should finish at 57.4ms: "
          f"{fast_batch.elapsed}")
    check_invariants(fast_batch)


def test_decode_plan_trades_link_latency_for_remote_parallelism(binary):
    link_bound_rows = [(1, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0),
                       (4, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)]
    link_bound = simulate(
        scenario(remote_count=4, schedule_cost=1.0, latency_ms=20.0,
                 bandwidth_gbps=1000.0, bytes_per_token=1, num_layers=1,
                 rows=link_bound_rows, slo1=1000.0, slo2=1000.0,
                 dist_base=100.0, w_tp=1.0, w_c=0.0,
                 arrivals=[(0.0, 1, 2)] * 4),
        [binary])
    check(link_bound.ok, f"link-bound decode run failed: {link_bound.error}")
    placements = [line.split()[3] for line in link_bound.responses.splitlines()
                  if line.startswith("E P PRE")]
    check(placements == ["0"] * 4,
          f"fixed transfer latency should concentrate placement: {placements}")
    first_decode_pre = next(line for line in link_bound.responses.splitlines()
                            if line.startswith("E D PRE"))
    check(first_decode_pre.startswith("E D PRE -1 2 "),
          f"prefill overlap should retain two-request decode waves: "
          f"{first_decode_pre}")
    check(link_bound.tasks == 24 and link_bound.elapsed < 198.001,
          f"two waves should use 24 tasks without extending the drain: "
          f"tasks={link_bound.tasks}, elapsed={link_bound.elapsed}")
    check_invariants(link_bound)

    compute_bound_rows = [(1, 0.1, 0.1, 0.1, 0.1, 20.0, 0.1),
                          (4, 0.1, 0.1, 0.1, 0.1, 80.0, 0.1)]
    compute_bound = simulate(
        scenario(remote_count=4, schedule_cost=0.1, latency_ms=0.001,
                 bandwidth_gbps=1000.0, bytes_per_token=1, num_layers=1,
                 rows=compute_bound_rows, slo1=1000.0, slo2=1000.0,
                 dist_base=100.0, w_tp=1.0, w_c=0.0,
                 arrivals=[(0.0, 1, 2), (1.0, 1, 2),
                           (2.1, 1, 2), (4.3, 1, 2)]),
        [binary])
    check(compute_bound.ok,
          f"compute-bound decode run failed: {compute_bound.error}")
    placements = [line.split()[3] for line in compute_bound.responses.splitlines()
                  if line.startswith("E P PRE")]
    check(len(set(placements)) >= 2,
          f"cheap links should retain remote parallelism: {placements}")
    check_invariants(compute_bound)


def throughput_regression_rows(decode_scale):
    return [(size,
             0.1 + size**0.55,
             0.2 + 20.0 * size**0.62,
             0.1 + 0.8 * size**0.55,
             0.1 + decode_scale * size**0.58,
             0.2 + 2.0 * decode_scale * size**0.55,
             0.1 + decode_scale * size**0.58)
            for size in (1, 4, 16, 64, 256, 1024)]


def test_placement_preserves_known_prefill_work_balance(binary):
    # Exact request-count balance discards known input costs in favor of
    # unknown future output work.  The load-aware placement intentionally uses
    # skewed counts here and finishes 3.5 seconds sooner.
    result = simulate(
        scenario(remote_count=4, schedule_cost=5.0, latency_ms=40.0,
                 bandwidth_gbps=0.05, bytes_per_token=1, num_layers=64,
                 rows=throughput_regression_rows(2.0),
                 slo1=10.0, slo2=0.5,
                 tp_ub=0.04931732736796676, tp_base=0.0,
                 dist_base=1683.9345249583582,
                 w_tp=1.0, w_c=0.0,
                 arrivals=[(10.0, 1, 74), (20.0, 4, 57),
                           (30.0, 1, 56), (40.0, 4, 61),
                           (50.0, 16, 27), (60.0, 1, 63),
                           (70.0, 64, 22), (80.0, 16, 67),
                           (90.0, 64, 36), (100.0, 1024, 62),
                           (110.0, 1024, 79), (120.0, 256, 28),
                           (130.0, 256, 22), (140.0, 1, 63),
                           (150.0, 256, 67), (160.0, 1024, 11),
                           (170.0, 64, 13), (180.0, 1024, 80),
                           (190.0, 1024, 74), (200.0, 256, 40),
                           (210.0, 16, 65)]),
        [binary])
    check(result.ok, f"prefill-work placement run failed: {result.error}")
    check_invariants(result)

    placements = [int(line.split()[3]) for line in result.responses.splitlines()
                  if line.startswith("E P PRE")]
    check(sorted(Counter(placements).values()) == [2, 4, 6, 9],
          f"known prefill work should justify count skew: {placements}")
    check(result.tasks <= 1710 and result.elapsed < 25200.0,
          f"load-aware placement or decode-cohort fill regressed: "
          f"tasks={result.tasks}, "
          f"elapsed={result.elapsed}")


def test_single_remote_avoids_fixed_waves_for_hidden_lengths(binary):
    # Fixed K=1 waves look excellent when output lengths happen to match, but
    # hidden heterogeneous lengths make the cohorts collapse into singleton
    # tasks.  Dynamic per-step groups remain compact as requests finish.
    result = simulate(
        scenario(remote_count=1, schedule_cost=0.2, latency_ms=10.0,
                 bandwidth_gbps=1.0, bytes_per_token=1, num_layers=1,
                 rows=throughput_regression_rows(0.2),
                 slo1=100.0, slo2=0.5,
                 tp_ub=0.13037640519856733, tp_base=0.0,
                 dist_base=105.35709238964952, w_tp=0.8, w_c=0.2,
                 arrivals=[(0.0, 256, 14), (0.0, 4, 66),
                           (0.0, 1, 51), (0.0, 1, 27),
                           (0.0, 64, 67), (0.0, 1024, 20),
                           (0.0, 16, 33), (0.0, 16, 72),
                           (0.0, 64, 51), (0.0, 1024, 3),
                           (0.0, 16, 59), (0.0, 64, 80),
                           (0.0, 1, 25), (0.0, 64, 35)]),
        [binary])
    check(result.ok, f"heterogeneous-length K=1 run failed: {result.error}")
    check_invariants(result)

    check(result.tasks <= 400 and result.elapsed < 7000.0,
          f"fixed-wave collapse: tasks={result.tasks}, elapsed={result.elapsed}")
    check(result.score > 630.0,
          f"dynamic groups should retain throughput score: {result.score}")


def test_ready_final_steps_fill_active_decode_batch(binary):
    prefill_scenario = scenario(
        remote_count=1, schedule_cost=1.0, latency_ms=0.01,
        bandwidth_gbps=1.0, bytes_per_token=100, num_layers=1,
        rows=[(1, 0.2, 2.2, 0.18, 2.1, 4.2, 2.1)],
        slo1=100.0, slo2=50.0,
        tp_ub=0.17256567356491095, tp_base=0.0, dist_base=0.1,
        arrivals=[(0.0, 1, 4), (10.0, 1, 2), (30.0, 1, 1)])
    for weight in (0.75, 0.9):
        prefill_fill = simulate(
            replace(prefill_scenario, w_tp=weight, w_c=1.0 - weight),
            [binary])
        check(prefill_fill.ok,
              f"P POST decode-fill run failed: {prefill_fill.error}")
        check_invariants(prefill_fill)
        check(prefill_fill.tasks <= 27 and prefill_fill.tdr < 10.1 and
              prefill_fill.elapsed < 55.0,
              f"ready P POST should refill a blocked active batch: tasks="
              f"{prefill_fill.tasks}, tdr={prefill_fill.tdr}, "
              f"elapsed={prefill_fill.elapsed}, weight={weight}")

    sizes = (1, 4, 16, 64)
    decode_fill_rows = [
        (size, 0.1 + size**0.55, 0.2 + 10.0 * size**0.62,
         0.1 + 0.8 * size**0.55, 0.1 + size**0.58,
         0.2 + 2.0 * size**0.55, 0.1 + size**0.58)
        for size in sizes]
    decode_fill = simulate(
        scenario(remote_count=2, schedule_cost=5.0, latency_ms=20.0,
                 bandwidth_gbps=1.0, bytes_per_token=500000, num_layers=8,
                 rows=decode_fill_rows, slo1=100.0, slo2=50.0,
                 tp_ub=1.0, tp_base=0.0, dist_base=100.0,
                 w_tp=0.9, w_c=0.1,
                 arrivals=[(0.0, 16, 13), (0.0, 16, 2),
                           (0.0, 1, 8), (0.0, 4, 2)]),
        [binary])
    check(decode_fill.ok,
          f"D POST decode-fill run failed: {decode_fill.error}")
    check_invariants(decode_fill)
    post_sizes = [int(line.split()[4])
                  for line in decode_fill.responses.splitlines()
                  if line.startswith("E D POST")]
    check(2 in post_sizes and decode_fill.tasks <= 83 and
          decode_fill.elapsed < 1270.0 and decode_fill.tpot < 71.0,
          f"ready D POST should safely merge decode waves: sizes="
          f"{post_sizes}, tasks={decode_fill.tasks}, "
          f"elapsed={decode_fill.elapsed}, tpot={decode_fill.tpot}")


def test_cheap_prefill_uses_active_decode_gap(binary):
    rows = [(1, 1.0, 1.0, 1.0, 1.0, 2.0, 1.0),
            (4, 1.0, 2.0, 1.0, 2.0, 4.0, 2.0)]
    result = simulate(
        scenario(schedule_cost=1.0, latency_ms=1.0,
                 bandwidth_gbps=100.0, bytes_per_token=1, num_layers=1,
                 rows=rows, slo1=50.0, slo2=50.0,
                 tp_ub=1.0, tp_base=0.0, dist_base=100.0,
                 w_tp=0.75, w_c=0.25,
                 arrivals=[(0.0, 1, 2), (0.0, 1, 3),
                           (30.0, 1, 1)]),
        [binary])
    check(result.ok, f"cheap-prefill fill run failed: {result.error}")
    check_invariants(result)

    edge_tasks = [line for line in result.responses.splitlines()
                  if line.startswith("E ")]
    check(edge_tasks.index("E P PRE 0 2") <
          edge_tasks.index("E D PRE -1 1 1"),
          f"cheap prefill should use the decode-fill gap: {edge_tasks}")
    check(result.tasks == 21 and result.elapsed < 48.0 and result.tdr < 9.0,
          f"cheap admission should overlap work without adding tasks: "
          f"tasks={result.tasks}, elapsed={result.elapsed}, tdr={result.tdr}")


def test_current_workload_can_enable_joint_decode_plan(binary):
    # At the maximum supported workload, linear D PROC work needs both
    # remotes.  Four live requests are link-bound instead, so their current
    # plan should concentrate placement and form two size-2 pipeline waves.
    rows = [(1, 0.1, 0.1, 0.1, 0.1, 10.0, 0.1),
            (2000, 0.1, 0.1, 0.1, 0.1, 20000.0, 0.1)]
    result = simulate(
        scenario(remote_count=2, schedule_cost=1.0, latency_ms=50.0,
                 bandwidth_gbps=1000.0, bytes_per_token=1, num_layers=1,
                 rows=rows, slo1=1e9, slo2=1e9,
                 tp_ub=1.0, tp_base=0.0, dist_base=100.0,
                 w_tp=1.0, w_c=0.0,
                 arrivals=[(0.0, 1, 2)] * 4),
        [binary])
    check(result.ok, f"current-workload joint plan failed: {result.error}")
    check_invariants(result)

    placements = [int(line.split()[3]) for line in result.responses.splitlines()
                  if line.startswith("E P PRE")]
    decode_tasks = [line.split() for line in result.responses.splitlines()
                    if line.startswith("E D ") or line.startswith("C0 D ")]
    check(placements == [0] * 4,
          f"the current link-bound workload should use C0 only: {placements}")
    check(len(decode_tasks) == 12 and
          {int(fields[4]) for fields in decode_tasks} == {2},
          f"expected twelve size-2 decode tasks, got {decode_tasks}")
    # The old max-workload mode gate spreads work over both remotes and takes
    # 663.2ms because every wave pays two transfer latencies.
    check(result.elapsed < 510.0,
          f"current-workload planning should finish before 510ms: "
          f"{result.elapsed}")


def test_finite_decode_drain_merges_ready_waves(binary):
    # Keep the smaller plan while prefill feeds decode, then use finite drain
    # cost to merge ready waves once all six requests have reached decode.
    rows = [(size, 0.4 * size**0.72, 3.0 * size**0.7,
             0.4 * size**0.72, 0.7 * size**0.68,
             2.5 * size**0.64, 0.7 * size**0.68)
            for size in (1, 2, 4, 8, 16, 32, 64, 128)]
    inputs = (1, 8, 64, 256, 1024, 1)
    result = simulate(
        scenario(remote_count=2, schedule_cost=1.0, latency_ms=12.0,
                 bandwidth_gbps=100.0, bytes_per_token=64, num_layers=16,
                 rows=rows, slo1=1e9, slo2=1e9,
                 tp_ub=1.0, tp_base=0.0, dist_base=1.0,
                 w_tp=0.9, w_c=0.1,
                 arrivals=[(0.0, length, 2) for length in inputs]),
        [binary])
    check(result.ok, f"finite decode-drain run failed: {result.error}")
    check_invariants(result)
    check(result.tasks <= 37 and result.elapsed < 303.0 and result.tpot < 38.0,
          f"ready decode waves should merge after pipeline fill: "
          f"tasks={result.tasks}, elapsed={result.elapsed}, tpot={result.tpot}")


def test_finite_decode_drain_uses_faster_plan(binary):
    rows = [(1, 1.5, 13.0, 1.0, 0.7, 4.0, 5.0),
            (4, 4.6, 38.0, 3.0, 2.1, 7.6, 12.3),
            (16, 13.3, 111.0, 8.9, 6.0, 15.1, 30.9)]
    arrivals = [(0.0, length, output) for length, output in
                [(4, 1), (1, 2), (4, 1), (1, 1), (1, 1), (2, 2),
                 (1, 1), (4, 2), (4, 1), (2, 1), (2, 1), (2, 1), (4, 1)]]
    result = simulate(
        scenario(remote_count=2, schedule_cost=0.1, latency_ms=10.0,
                 bandwidth_gbps=2.0, bytes_per_token=125000, num_layers=4,
                 rows=rows, slo1=105.0, slo2=23.0,
                 tp_ub=1.0, tp_base=0.0, dist_base=1.0,
                 w_tp=0.75, w_c=0.25, arrivals=arrivals), [binary])
    check(result.ok, f"finite-drain plan failed: {result.error}")
    check_invariants(result)
    check(result.tasks <= 62 and result.elapsed < 305.0 and result.tpot < 40.0,
          f"finite drain should use the faster full plan: "
          f"tasks={result.tasks}, elapsed={result.elapsed}, tpot={result.tpot}")


def test_completed_decode_waves_are_not_discounted_twice(binary):
    rows = [(size, 0.1 + size**0.55, 0.2 + 10 * size**0.62,
             0.1 + 0.8 * size**0.55, 0.1 + size**0.58,
             0.2 + 2 * size**0.55, 0.1 + size**0.58)
            for size in (1, 4, 16, 64)]
    result = simulate(
        scenario(remote_count=2, schedule_cost=0.1, latency_ms=5.0,
                 bandwidth_gbps=1.0, bytes_per_token=500000, num_layers=8,
                 rows=rows, slo1=100.0, slo2=50.0,
                 tp_ub=1.0, tp_base=0.0, dist_base=100.0,
                 w_tp=0.9, w_c=0.1,
                 arrivals=[(0.0, 1, 2), (0.0, 4, 2),
                           (0.0, 4, 13), (0.0, 16, 3)]), [binary])
    check(result.ok, f"completed-wave run failed: {result.error}")
    check_invariants(result)
    check(result.tasks <= 66 and result.elapsed < 557.0,
          f"ready D POST members should use the full ready workload: "
          f"tasks={result.tasks}, elapsed={result.elapsed}")


def test_costly_prefill_waits_for_active_decode(binary):
    # A long prefill admitted between active decode iterations blocks both
    # FIFO links for about 3.28s each. Drain token-producing requests first.
    rows = [(1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1),
            (4096, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1)]
    result = simulate(
        scenario(remote_count=2, schedule_cost=0.1, latency_ms=1.0,
                 bandwidth_gbps=0.01, bytes_per_token=1000, num_layers=1,
                 rows=rows, slo1=10000.0, slo2=10.0,
                 tp_ub=1.0, tp_base=0.0, dist_base=100.0,
                 w_tp=0.5, w_c=0.5,
                 arrivals=[(0.0, 1, 3), (0.0, 1, 3),
                           (10.0, 4096, 1)]),
        [binary])
    check(result.ok, f"costly-prefill probe run failed: {result.error}")
    check_invariants(result)

    active_tokens = [tokens for _, tokens in result.per_request[:2]]
    active_gaps = [later - earlier for tokens in active_tokens
                   for earlier, later in zip(tokens, tokens[1:])]
    check(max(active_gaps) < 10.0,
          f"costly prefill must not block active decode: {active_gaps}")
    check(result.tpot < 6.0,
          f"decode probing should keep mean TPOT below 6ms: {result.tpot}")
    # The unavoidable prefill transfers dominate the makespan; admission
    # control should preserve it while avoiding the old 3281ms mean TPOT.
    check(result.elapsed < 6600.0,
          f"deferred prefill should still finish before 6600ms: "
          f"{result.elapsed}")


def test_prefill_proc_chunks_to_decode_gap_budget(binary):
    # Request 1 arrives after request 0 has produced a token. Its full P PROC
    # takes 640ms, so cap pieces to the weighted TPOT budget until active
    # decode drains, then finish the remaining layers in one piece.
    rows = [(1, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0),
            (64, 1.0, 640.0, 1.0, 1.0, 1.0, 1.0)]
    result = simulate(
        scenario(remote_count=1, schedule_cost=1.0, latency_ms=5.0,
                 bandwidth_gbps=1000.0, bytes_per_token=1, num_layers=64,
                 rows=rows, slo1=100.0, slo2=10.0,
                 tp_ub=1.0, tp_base=0.0, dist_base=100.0,
                 w_tp=0.5, w_c=0.5,
                 arrivals=[(0.0, 1, 5), (32.0, 64, 1)]),
        [binary])
    check(result.ok, f"prefill chunk-budget run failed: {result.error}")
    check_invariants(result)

    pieces = [line.split() for line in result.responses.splitlines()
              if line.startswith("C0 P PROC") and line.endswith(" 1")]
    ranges = [(int(fields[3]), int(fields[4])) for fields in pieces]
    check(ranges[:6] == [(layer, layer + 1) for layer in range(6)] and
          ranges[-1] == (6, 64),
          f"expected six budgeted layers then one drain piece: {ranges}")
    check(result.tpot < 25.0,
          f"budgeted P PROC pieces should keep TPOT below 25ms: {result.tpot}")
    check(result.elapsed < 725.0,
          f"chunked prefill should finish before 725ms: {result.elapsed}")


def test_prefill_proc_does_not_oversplit_during_long_decode_post(binary):
    # A decode POST that lasts longer than the whole pending P PROC gives the
    # remote enough time to drain that prefill.  Aligning pieces only to SLO2
    # would pay S once per layer while the edge is still unavailable.
    rows = [(1, 0.1, 0.1, 0.1, 0.1, 100.0, 200.0),
            (64, 0.1, 1.0, 0.1, 0.1, 100.0, 200.0)]
    result = simulate(
        scenario(remote_count=1, schedule_cost=10.0, latency_ms=0.001,
                 bandwidth_gbps=0.01024, bytes_per_token=1000,
                 num_layers=64, rows=rows, slo1=1000.0, slo2=1.0,
                 tp_ub=1.0, tp_base=0.0, dist_base=10.0,
                 w_tp=0.5, w_c=0.5,
                 arrivals=[(0.0, 1, 2), (0.0, 64, 1)]),
        [binary])
    check(result.ok, f"long-decode-post run failed: {result.error}")
    check_invariants(result)

    pieces = [line.split() for line in result.responses.splitlines()
              if line.startswith("C0 P PROC") and line.endswith(" 1")]
    ranges = [(int(fields[3]), int(fields[4])) for fields in pieces]
    check(ranges == [(0, 64)],
          f"prefill should drain during the long decode POST: {ranges}")
    check(result.tasks <= 15 and result.elapsed < 850.0,
          f"oversplitting regression: tasks={result.tasks}, "
          f"elapsed={result.elapsed}")


def test_split_prefill_pieces():
    # Two halves of the public example's 4-layer prefill: each piece costs
    # 2/4 * 10 = 5ms of compute plus its own schedule cost, so P POST lands at
    # 31.0 instead of 30.0 and the extra schedule cost shows up in the makespan.
    rules = [("XDN UP 0 500000 PRE", "C0 P PROC 0 2 0 0"),
             ("TDN C0 P PROC 0 2", "C0 P PROC 2 4 0 0"),
             ("XDN DOWN 0 500000 PRE", "E P POST 0 0"),
             ("TDN E P POST", "E D PRE -1 1 0"),
             ("XDN UP 0 125000 DEC", "C0 D PROC 0 1 0"),
             ("XDN DOWN 0 125000 DEC", "E D POST -1 1 0"),
             ("ARR", "E P PRE 0 0")]
    body = ("rules = %r\n"
            "for events in frames():\n"
            "    reply = '0\\n'\n"
            "    for prefix, task in rules:\n"
            "        if any(e.startswith(prefix) for e in events):\n"
            "            reply = '1\\n' + task + '\\n'\n"
            "            break\n"
            "    say(reply)" % rules)
    result = simulate(scenario(), fake(body))
    check(result.ok, f"split-prefill run failed: {result.error}")
    check_invariants(result)
    starts = [line for _, events in frames(result) for line in events
              if line.startswith("TDN C0 P PROC")]
    check(starts == ["TDN C0 P PROC 0 2 0 0 5.000000000",
                     "TDN C0 P PROC 2 4 0 0 5.000000000"],
          f"each half costs 2/4 of prefill_proc(4)=10ms, got {starts}")
    check(result.tdr == 31.0, f"hand-computed TDR 31.0, got {result.tdr}")
    check(result.elapsed == 46.0,
          f"the extra schedule cost must reach the makespan, got {result.elapsed}")


def test_idling_is_safe_while_arrivals_remain():
    result = simulate(scenario(arrivals=[(0.0, 4, 1), (100.0, 4, 1)]),
                      fake("for _ in frames(): say('0\\n')"))
    check(not result.ok, "a scheduler that never assigns must be reported")
    check("stuck" in result.error, f"expected a stuck report, got {result.error}")
    check("t=100.000000000" in result.error,
          f"idling is safe until the last arrival is consumed, got {result.error}")


def test_violations_are_reported_with_context():
    cases = [
        ("0\\n", "stuck", scenario()),
        ("1\\nE P PRE 0 5\\n", "unknown request 5", scenario()),
        ("1\\nE P PRE 9 0\\n", "remote 9 outside", scenario()),
        ("1\\nC0 P PRE 0 0\\n", "C0 cannot run", scenario()),
        ("2\\nE P PRE 0 0\\nE P PRE 0 0\\n", "assigned twice", scenario()),
        ("1\\nE D PRE -1 2 0 0\\n", "duplicate request ids", scenario()),
        ("1\\nE D PRE -1 0\\n", "group size 0", scenario()),
        ("1\\nE P\\n", "malformed assignment", scenario()),
        ("1\\nE P PRE 0 0 0\\n", "malformed assignment", scenario()),
        ("5\\n", "outside [0, 2]", scenario()),
        ("oops\\n", "not an integer", scenario()),
    ]
    for reply, expected, cfg in cases:
        result = simulate(cfg, fake(f"for _ in frames(): say('{reply}')"))
        check(not result.ok, f"{reply!r} should have been rejected")
        check(expected in result.error,
              f"{reply!r}: expected {expected!r} in {result.error!r}")
        check(result.error.startswith("t="),
              f"{reply!r}: the report must name the timestamp, got {result.error!r}")


def test_busy_server_is_rejected():
    result = simulate(scenario(arrivals=[(0.0, 4, 1), (1.0, 4, 1)]),
                      fake("for index, events in enumerate(frames()):\n"
                           "    say('1\\nE P PRE 0 %d\\n' % index)"))
    check(not result.ok, "assigning to a running server must be rejected")
    check("is busy" in result.error, f"expected a busy-server report, got {result.error}")


def test_bad_layer_range_is_rejected():
    body = ("for events in frames():\n"
            "    if any(e.startswith('XDN UP') for e in events):\n"
            "        say('1\\nC0 P PROC 0 9 0 0\\n')\n"
            "    elif any(e.startswith('ARR') for e in events):\n"
            "        say('1\\nE P PRE 0 0\\n')\n"
            "    else: say('0\\n')")
    result = simulate(scenario(), fake(body))
    check(not result.ok, "an out-of-range layer piece must be rejected")
    check("layer range [0, 9)" in result.error, f"got {result.error}")


def test_gapped_layer_piece_is_rejected():
    body = ("for events in frames():\n"
            "    if any(e.startswith('XDN UP') for e in events):\n"
            "        say('1\\nC0 P PROC 1 4 0 0\\n')\n"
            "    elif any(e.startswith('ARR') for e in events):\n"
            "        say('1\\nE P PRE 0 0\\n')\n"
            "    else: say('0\\n')")
    result = simulate(scenario(), fake(body))
    check(not result.ok, "a piece that leaves a gap must be rejected")
    check("expects piece starting at 0" in result.error, f"got {result.error}")


def test_scheduler_that_exits_early():
    result = simulate(scenario(), fake("pass"))
    check(not result.ok, "an early exit must be reported")
    check("exited before answering" in result.error, f"got {result.error}")


def test_hanging_scheduler_times_out():
    result = simulate(scenario(), fake("import time\ntime.sleep(30)"), timeout=0.5)
    check(not result.ok, "a hung scheduler must be reported")
    check("no output within" in result.error, f"got {result.error}")


def main():
    binary = sys.argv[1] if len(sys.argv) > 1 else "./scheduler"
    cases = [test_lookup, test_missing_table_entries, test_transfer_time, test_score,
             test_idling_is_safe_while_arrivals_remain, test_violations_are_reported_with_context,
             test_busy_server_is_rejected, test_bad_layer_range_is_rejected,
             test_gapped_layer_piece_is_rejected, test_split_prefill_pieces,
             test_scheduler_that_exits_early,
             test_hanging_scheduler_times_out]
    needs_binary = [test_public_example, test_single_layer_and_repeated_tokens,
                    test_one_token_request_has_no_gap,
                    test_hidden_output_length_cannot_change_visible_prefix,
                    test_shortest_prefill_is_admitted_first,
                    test_shortest_prefill_does_not_depend_on_score_weight,
                    test_overdue_prefill_beats_shorter_new_arrival,
                    test_score_scaled_aging_keeps_shortest_prefill,
                    test_prefill_aging_does_not_preempt_ready_post,
                    test_uplink_queues_behind_a_transfer_in_flight,
                    test_decode_group_splits_by_remote,
                    test_prefill_placement_balances_remaining_compute,
                    test_ready_decode_runs_before_late_uplink,
                    test_decode_post_waits_only_when_batch_reduces_drain_time,
                    test_decode_plan_trades_link_latency_for_remote_parallelism,
                    test_placement_preserves_known_prefill_work_balance,
                    test_single_remote_avoids_fixed_waves_for_hidden_lengths,
                    test_ready_final_steps_fill_active_decode_batch,
                    test_cheap_prefill_uses_active_decode_gap,
                    test_current_workload_can_enable_joint_decode_plan,
                    test_finite_decode_drain_merges_ready_waves,
                    test_finite_decode_drain_uses_faster_plan,
                    test_completed_decode_waves_are_not_discounted_twice,
                    test_costly_prefill_waits_for_active_decode,
                    test_prefill_proc_chunks_to_decode_gap_budget,
                    test_prefill_proc_does_not_oversplit_during_long_decode_post]
    for case in cases:
        case()
    for case in needs_binary:
        case(binary)
    print(f"{len(cases) + len(needs_binary)} simulator cases passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
