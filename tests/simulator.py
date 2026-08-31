#!/usr/bin/env python3
"""Exact local interactor and scorer for the edge-cloud scheduling problem.

Mirrors the statement: ``S + dur`` task occupancy, piecewise-linear task times,
independent one-at-a-time FIFO uplink/downlink queues, per-remote D PRE
transfer splitting, output lengths hidden until FIN, and the published score.

Scheduler output is untrusted: every assignment is legality-checked and any
violation aborts the run with the timestamp and the offending line.
"""
import heapq
import math
import queue
import subprocess
import threading
from dataclasses import dataclass, field

# Task-time table columns, in the order the startup rows list them.
P_PRE, P_PROC, P_POST, D_PRE, D_PROC, D_POST = range(6)

# Stages a request sits in while no task of its own is running.
(READY_P_PRE, WAIT_P_UP, READY_P_PROC, WAIT_P_DOWN, READY_P_POST,
 READY_D_PRE, WAIT_D_UP, READY_D_PROC, WAIT_D_DOWN, READY_D_POST,
 FINISHED) = range(11)


class Violation(Exception):
    """Raised when the scheduler breaks the protocol or the run gets stuck."""


@dataclass
class Scenario:
    """One synthetic test: system configuration, task times, and workload."""

    remote_count: int
    schedule_cost: float
    latency_ms: float
    bandwidth_gbps: float
    bytes_per_token: int
    num_layers: int
    slo1: float
    slo2: float
    tp_ub: float
    tp_base: float
    dist_base: float
    w_tp: float
    w_c: float
    # (batch_size, prefill_pre, prefill_proc, prefill_post,
    #  decode_pre, decode_proc, decode_post); -1.0 marks a missing entry.
    rows: list
    # (arrival_time, input_length, output_length), in nondecreasing arrival
    # order; the index is the request id.
    arrivals: list

    def curve(self, column):
        """Listed (size, time) pairs for one column, sorted by size."""
        points = sorted((int(row[0]), float(row[1 + column])) for row in self.rows
                        if float(row[1 + column]) >= 0.0)
        if not points:
            raise ValueError(f"task-time column {column} has no entry")
        return points

    def task_time(self, column, size):
        return lookup(self.curve(column), size)

    def transfer_ms(self, length):
        data_bits = 8.0 * length * self.bytes_per_token
        return self.latency_ms + data_bits / (self.bandwidth_gbps * 1e6)


@dataclass
class Result:
    """Outcome of one simulated run."""

    scenario: Scenario
    transcript: str = ""       # every byte the scheduler read
    responses: str = ""        # every byte the scheduler wrote
    error: str = None
    elapsed: float = 0.0
    tp: float = 0.0
    tdr: float = 0.0
    tpot: float = 0.0
    dist: float = 0.0
    norm_tp: float = 0.0
    norm_c: float = 0.0
    score: float = 0.0
    tasks: int = 0             # tasks the scheduler started
    idle_frames: int = 0       # frames answered with no assignment
    per_request: list = field(default_factory=list)  # (tdr, token_times)

    @property
    def ok(self):
        return self.error is None

    def summary(self):
        return (f"tp={self.tp:.6f} mean_tdr={self.tdr:.6f} mean_tpot={self.tpot:.6f} "
                f"dist={self.dist:.6f} norm_tp={self.norm_tp:.6f} "
                f"norm_c={self.norm_c:.6f} points={self.score:.6f}")


def lookup(points, size):
    """Piecewise-linear task time: flat outside the listed sizes."""
    if size <= points[0][0]:
        return points[0][1]
    if size >= points[-1][0]:
        return points[-1][1]
    for (lo_size, lo_time), (hi_size, hi_time) in zip(points, points[1:]):
        if size == lo_size:
            return lo_time
        if size < hi_size:
            span = (size - lo_size) / (hi_size - lo_size)
            return lo_time + span * (hi_time - lo_time)
    return points[-1][1]


def score(scenario, tp, tdr, tpot):
    """Return (dist, norm_tp, norm_c, points) exactly as the judge computes."""
    excess_tdr = max(0.0, (tdr - scenario.slo1) / scenario.slo1)
    excess_tpot = max(0.0, (tpot - scenario.slo2) / scenario.slo2)
    dist = math.hypot(excess_tdr, excess_tpot)
    norm_tp = (tp - scenario.tp_base) / (scenario.tp_ub - scenario.tp_base)
    norm_tp = max(0.0, min(1.0, norm_tp))
    if scenario.dist_base > 0.0:
        norm_c = max(0.0, 1.0 - dist / scenario.dist_base)
    else:
        norm_c = 1.0 if dist == 0.0 else 0.0
    return dist, norm_tp, norm_c, 1000.0 * (scenario.w_tp * norm_tp + scenario.w_c * norm_c)


class _Child:
    """Line-oriented pipe to the scheduler with a read deadline."""

    def __init__(self, command, timeout):
        self.timeout = timeout
        self.proc = subprocess.Popen(
            command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL, text=True, bufsize=1)
        self.lines = queue.Queue()
        threading.Thread(target=self._pump, daemon=True).start()

    def _pump(self):
        for line in self.proc.stdout:
            self.lines.put(line)
        self.lines.put(None)

    def send(self, text):
        try:
            self.proc.stdin.write(text)
            self.proc.stdin.flush()
        except (BrokenPipeError, ValueError):
            raise Violation("scheduler closed its input stream")

    def readline(self):
        try:
            line = self.lines.get(timeout=self.timeout)
        except queue.Empty:
            raise Violation(f"scheduler produced no output within {self.timeout}s")
        if line is None:
            raise Violation("scheduler exited before answering the frame")
        return line

    def close(self):
        try:
            self.proc.stdin.close()
        except (BrokenPipeError, ValueError):
            pass
        try:
            self.proc.wait(timeout=self.timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()


def simulate(scenario, command, timeout=30.0):
    """Run ``command`` against ``scenario`` and return a scored Result."""
    result = Result(scenario=scenario)
    run = _Run(scenario, result)
    child = _Child(command, timeout)
    try:
        run.play(child)
    except Violation as exc:
        result.error = str(exc)
    finally:
        child.close()
    run.finish()
    return result


class _Run:
    """Mutable simulation state. Kept private so Scenario/Result stay the API."""

    def __init__(self, scenario, result):
        self.cfg = scenario
        self.out = result
        self.now = 0.0
        self.seq = 0
        self.events = []
        # A request: stage, running flag, assigned remote, prefill progress,
        # and the timing needed for scoring.
        self.req = [dict(rid=rid, lin=lin, lout=lout, arrival=arrival,
                         stage=None, running=False, remote=-1, next_layer=0,
                         tdr=None, tokens=[])
                    for rid, (arrival, lin, lout) in enumerate(scenario.arrivals)]
        self.busy = {-1: False}
        self.busy.update({r: False for r in range(scenario.remote_count)})
        self.link_free = {"UP": 0.0, "DOWN": 0.0}
        self.left = len(self.req)
        for r in self.req:
            self.push(r["arrival"], 0, ("ARR", r["rid"]))

    # -- event queue ------------------------------------------------------
    def push(self, when, rank, payload):
        self.seq += 1
        heapq.heappush(self.events, (when, rank, self.seq, payload))

    def pop_frame(self):
        """All events sharing the earliest internal timestamp, in order."""
        when = self.events[0][0]
        batch = []
        while self.events and self.events[0][0] == when:
            batch.append(heapq.heappop(self.events))
        return when, batch

    # -- transfers --------------------------------------------------------
    def enqueue(self, direction, remote, kind, length, rids):
        """Append to a one-at-a-time FIFO link queue and schedule its XDN."""
        duration = self.cfg.transfer_ms(length)
        done = max(self.now, self.link_free[direction]) + duration
        self.link_free[direction] = done
        size = length * self.cfg.bytes_per_token
        ids = " ".join(str(rid) for rid in rids)
        line = f"XDN {direction} {remote} {size} {kind} {len(rids)} {ids}"
        self.push(done, 2, ("XDN", line, direction, kind, list(rids)))

    # -- frame construction ----------------------------------------------
    def apply(self, payload):
        """Advance state for one event; return its frame line plus any FIN."""
        kind = payload[0]
        if kind == "ARR":
            request = self.req[payload[1]]
            request["stage"] = READY_P_PRE
            return [f"ARR {request['rid']} {request['lin']}"], []
        if kind == "XDN":
            _, line, direction, dec, rids = payload
            for rid in rids:
                request = self.req[rid]
                if dec == "PRE":
                    request["stage"] = READY_P_PROC if direction == "UP" else READY_P_POST
                else:
                    request["stage"] = READY_D_PROC if direction == "UP" else READY_D_POST
            return [line], []
        return self.complete(payload)

    def complete(self, payload):
        """Finish a task: free its server, advance members, queue transfers."""
        _, server, spec, duration, work, step, rids, remote, layer_end = payload
        self.busy[server] = False
        fins = []
        for rid in rids:
            self.req[rid]["running"] = False
        if work == "P":
            request = self.req[rids[0]]
            if step == "PRE":
                request["stage"] = WAIT_P_UP
                self.enqueue("UP", remote, "PRE", request["lin"], rids)
            elif step == "PROC":
                request["next_layer"] = layer_end
                if layer_end == self.cfg.num_layers:
                    request["stage"] = WAIT_P_DOWN
                    self.enqueue("DOWN", remote, "PRE", request["lin"], rids)
                else:
                    request["stage"] = READY_P_PROC
            else:
                request["stage"] = READY_D_PRE
                request["tdr"] = self.now - request["arrival"]
        elif step == "PRE":
            by_remote = {}
            for rid in rids:
                self.req[rid]["stage"] = WAIT_D_UP
                by_remote.setdefault(self.req[rid]["remote"], []).append(rid)
            for target in sorted(by_remote):
                members = by_remote[target]
                self.enqueue("UP", target, "DEC", len(members), members)
        elif step == "PROC":
            for rid in rids:
                self.req[rid]["stage"] = WAIT_D_DOWN
            self.enqueue("DOWN", remote, "DEC", len(rids), rids)
        else:
            for rid in rids:
                request = self.req[rid]
                request["tokens"].append(self.now)
                if len(request["tokens"]) == request["lout"]:
                    request["stage"] = FINISHED
                    self.left -= 1
                    fins.append(f"FIN {rid}")
                else:
                    request["stage"] = READY_D_PRE
        return [f"TDN {name(server)} {spec} {duration:.9f}"], fins

    # -- main loop --------------------------------------------------------
    def play(self, child):
        cfg = self.cfg
        startup = (f"{cfg.remote_count} {cfg.schedule_cost:.9f} {cfg.latency_ms:.9f} "
                   f"{cfg.bandwidth_gbps:.9f} {cfg.bytes_per_token} {cfg.num_layers}\n"
                   f"{cfg.slo1:.9f} {cfg.slo2:.9f} {cfg.tp_ub:.9f} {cfg.tp_base:.9f} "
                   f"{cfg.dist_base:.9f} {cfg.w_tp:.9f} {cfg.w_c:.9f}\n"
                   f"{len(cfg.rows)}\n")
        for row in cfg.rows:
            startup += (f"{int(row[0])} "
                        + " ".join(f"{float(v):.9f}" for v in row[1:]) + "\n")
        self.write(child, startup)

        while True:
            if not self.events:
                raise Violation(f"t={self.now:.9f}: stuck with {self.left} "
                                "unfinished requests and no future event")
            self.now, batch = self.pop_frame()
            lines, fins = [], []
            for _, _, _, payload in batch:
                emitted, finished = self.apply(payload)
                lines += emitted
                fins += finished
            lines += fins
            self.write(child, f"{self.now:.9f}\n{len(lines)}\n"
                       + "".join(line + "\n" for line in lines))
            self.respond(child)
            if self.left == 0:
                self.write(child, "END\n")
                return

    def write(self, child, text):
        self.out.transcript += text
        child.send(text)

    def respond(self, child):
        """Read one response and start every assignment it contains."""
        header = child.readline()
        self.out.responses += header
        try:
            count = int(header.strip())
        except ValueError:
            raise Violation(f"t={self.now:.9f}: response count {header.strip()!r} "
                            "is not an integer")
        if not 0 <= count <= self.cfg.remote_count + 1:
            raise Violation(f"t={self.now:.9f}: response count {count} outside "
                            f"[0, {self.cfg.remote_count + 1}]")
        if count == 0:
            self.out.idle_frames += 1
        claimed = set()
        for _ in range(count):
            line = child.readline()
            self.out.responses += line
            self.start(line.strip(), claimed)
        self.out.tasks += count

    def start(self, line, claimed):
        cfg = self.cfg
        fields = line.split()
        if len(fields) < 4:
            raise Violation(f"t={self.now:.9f}: malformed assignment {line!r}")
        server = self.server(fields[0], line)
        work, step = fields[1], fields[2]
        if work not in ("P", "D") or step not in ("PRE", "PROC", "POST"):
            raise Violation(f"t={self.now:.9f}: unknown task {work} {step} in {line!r}")

        layer_start = layer_end = -1
        if work == "P":
            if step == "PROC":
                layer_start, layer_end, remote, rid = self.ints(fields[3:], 4, line)
                extra = len(fields) - 7
            else:
                remote, rid = self.ints(fields[3:], 2, line)
                extra = len(fields) - 5
            rids = [rid]
        else:
            remote, member_count = self.ints(fields[3:5], 2, line)
            if member_count < 1:
                raise Violation(f"t={self.now:.9f}: group size {member_count} in {line!r}")
            rids = self.ints(fields[5:], member_count, line)
            extra = len(fields) - 5 - member_count
            if len(set(rids)) != len(rids):
                raise Violation(f"t={self.now:.9f}: duplicate request ids in {line!r}")
        if extra:
            raise Violation(f"t={self.now:.9f}: malformed assignment {line!r}")

        expected = remote if step == "PROC" else -1
        if server != expected:
            raise Violation(f"t={self.now:.9f}: {name(server)} cannot run {line!r}")
        if server in claimed:
            raise Violation(f"t={self.now:.9f}: {name(server)} assigned twice in one response")
        if self.busy[server]:
            raise Violation(f"t={self.now:.9f}: {name(server)} is busy, got {line!r}")
        claimed.add(server)

        stage = {("P", "PRE"): READY_P_PRE, ("P", "PROC"): READY_P_PROC,
                 ("P", "POST"): READY_P_POST, ("D", "PRE"): READY_D_PRE,
                 ("D", "PROC"): READY_D_PROC, ("D", "POST"): READY_D_POST}[(work, step)]
        for rid in rids:
            if not 0 <= rid < len(self.req):
                raise Violation(f"t={self.now:.9f}: unknown request {rid} in {line!r}")
            request = self.req[rid]
            if request["stage"] != stage or request["running"]:
                raise Violation(f"t={self.now:.9f}: request {rid} is not ready for {line!r}")
            if work == "P" and step == "PRE":
                if not 0 <= remote < cfg.remote_count:
                    raise Violation(f"t={self.now:.9f}: remote {remote} outside "
                                    f"[0, {cfg.remote_count}) in {line!r}")
            elif work == "P" or step == "PROC":
                if request["remote"] != remote:
                    raise Violation(f"t={self.now:.9f}: request {rid} is assigned to "
                                    f"C{request['remote']}, not C{remote}, in {line!r}")
            elif remote != -1:
                raise Violation(f"t={self.now:.9f}: expected -1 remote in {line!r}")

        if work == "P" and step == "PRE":
            self.req[rids[0]]["remote"] = remote
        if work == "P" and step == "PROC":
            request = self.req[rids[0]]
            if not 0 <= layer_start < layer_end <= cfg.num_layers:
                raise Violation(f"t={self.now:.9f}: layer range [{layer_start}, "
                                f"{layer_end}) outside [0, {cfg.num_layers}] in {line!r}")
            if layer_start != request["next_layer"]:
                raise Violation(f"t={self.now:.9f}: request {rids[0]} expects piece "
                                f"starting at {request['next_layer']}, got {line!r}")
            duration = ((layer_end - layer_start) / cfg.num_layers
                        * cfg.task_time(P_PROC, request["lin"]))
            spec = f"P PROC {layer_start} {layer_end} {remote} {rids[0]}"
        elif work == "P":
            column = P_PRE if step == "PRE" else P_POST
            duration = cfg.task_time(column, self.req[rids[0]]["lin"])
            spec = f"P {step} {remote} {rids[0]}"
        else:
            column = {"PRE": D_PRE, "PROC": D_PROC, "POST": D_POST}[step]
            duration = cfg.task_time(column, len(rids))
            spec = f"D {step} {remote} {len(rids)} " + " ".join(str(r) for r in rids)

        self.busy[server] = True
        for rid in rids:
            self.req[rid]["running"] = True
        self.push(self.now + cfg.schedule_cost + duration, 1,
                  ("TDN", server, spec, duration, work, step, rids, remote, layer_end))

    def server(self, token, line):
        if token == "E":
            return -1
        if token.startswith("C") and token[1:].isdigit():
            index = int(token[1:])
            if 0 <= index < self.cfg.remote_count:
                return index
        raise Violation(f"t={self.now:.9f}: unknown server {token!r} in {line!r}")

    def ints(self, fields, count, line):
        if len(fields) < count:
            raise Violation(f"t={self.now:.9f}: malformed assignment {line!r}")
        try:
            values = [int(field) for field in fields[:count]]
        except ValueError:
            raise Violation(f"t={self.now:.9f}: non-integer field in {line!r}")
        return values

    # -- scoring ----------------------------------------------------------
    def finish(self):
        out, cfg = self.out, self.cfg
        out.per_request = [(r["tdr"], list(r["tokens"])) for r in self.req]
        if out.error is None and self.left:
            out.error = f"{self.left} requests never finished"
        if out.error is not None:
            return
        tokens = [time for r in self.req for time in r["tokens"]]
        out.elapsed = max(tokens) - min(r["arrival"] for r in self.req)
        out.tp = len(tokens) / out.elapsed if out.elapsed > 0 else 0.0
        out.tdr = sum(r["tdr"] for r in self.req) / len(self.req)
        gaps = [later - earlier for r in self.req
                for earlier, later in zip(r["tokens"], r["tokens"][1:])]
        out.tpot = sum(gaps) / len(gaps) if gaps else 0.0
        out.dist, out.norm_tp, out.norm_c, out.score = score(cfg, out.tp, out.tdr, out.tpot)


def name(server):
    return "E" if server == -1 else f"C{server}"
