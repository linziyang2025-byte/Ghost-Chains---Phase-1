#!/usr/bin/env python3
"""Kan Chiong Delivery Driver — exact, dependency-free HTTP solution.

Run directly:
    python kan_cheong_delivery_driver.py

Then POST a batch JSON object to:
    /kan-cheong-delivery-driver

The module also exposes ``app`` and ``application`` as WSGI callables, so it
can be served by a production WSGI server (for example, ``gunicorn file:app``).

Algorithmic basis (publisher pages / DOI):

* Orda & Rom, Journal of the ACM 37(3), 607–625 (1990),
  doi:10.1145/79147.214078 — time-dependent paths and waiting constraints.
* Sung et al., European Journal of Operational Research 121(1), 32–39
  (2000), doi:10.1016/S0377-2217(99)00035-1 — time-dependent flow speeds.
* Ahuja et al., Transportation Science 36(3), 326–336 (2002),
  doi:10.1287/trsc.36.3.326.7827 — street networks with timed closures.
* Dean, Networks 44(1), 41–46 (2004), doi:10.1002/net.20013 — constrained
  waiting policies and time-expanded/dynamic-programming viewpoints.
* Dehne, Omran & Sack, Algorithmica 62, 416–435 (2012),
  doi:10.1007/s00453-010-9461-6 — FIFO time-dependent shortest paths.

Important model choices:

* Obstruction intervals are half-open: [start_time, end_time). This makes a
  closure active exactly at its start and cleared exactly at its end, matching
  the examples in the challenge.
* If several obstructions overlap on the same directed edge, the most
  restrictive active speed factor is used. A zero factor therefore dominates.
* Entering an edge while its factor is zero is forbidden. If a zero factor
  starts after entry, progress pauses on the edge and resumes when speed is
  positive again; this follows the requirement that only the untravelled
  portion is affected by a newly active obstruction.

All route/event arithmetic uses fractions, not binary floating point. That is
especially important at exact obstruction boundaries.
"""

from __future__ import annotations

import heapq
import json
import os
import sys
from bisect import bisect_right
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from fractions import Fraction
from http import HTTPStatus
from socketserver import ThreadingMixIn
from typing import Any, Iterable, Mapping, Optional
from wsgiref.simple_server import WSGIServer, make_server


ZERO = Fraction(0, 1)
ONE = Fraction(1, 1)
NANOSECONDS = 1_000_000_000
MAX_BODY_BYTES = 64 * 1024 * 1024
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _unreachable() -> dict[str, Any]:
    return {"total_duration_sec": None, "arrival_time": None, "path": []}


def _as_fraction(value: Any, field: str) -> Fraction:
    """Convert JSON-compatible numeric values without float round-off."""
    if isinstance(value, bool):
        raise ValueError(f"{field} must be numeric, not boolean")
    if isinstance(value, Fraction):
        return value
    if isinstance(value, Decimal):
        return Fraction(value)
    if isinstance(value, int):
        return Fraction(value, 1)
    if isinstance(value, float):
        return Fraction(str(value))
    if isinstance(value, str):
        return Fraction(value)
    raise ValueError(f"{field} must be numeric")


def _coordinate(value: Any, field: str) -> tuple[int, int]:
    if not isinstance(value, (list, tuple)) or len(value) != 2:
        raise ValueError(f"{field} must be a two-element coordinate")
    result: list[int] = []
    for component in value:
        if isinstance(component, bool):
            raise ValueError(f"{field} components must be integers")
        if isinstance(component, int):
            result.append(component)
            continue
        if isinstance(component, Decimal) and component == component.to_integral_value():
            result.append(int(component))
            continue
        if isinstance(component, float) and component.is_integer():
            result.append(int(component))
            continue
        raise ValueError(f"{field} components must be integers")
    return result[0], result[1]


def _parse_iso8601(value: Any) -> Fraction:
    """Parse an ISO-8601 instant into exact seconds since Unix epoch."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError("timestamp must be a non-empty ISO-8601 string")
    text = value.strip()
    if text[-1:] in ("Z", "z"):
        text = text[:-1] + "+00:00"
    try:
        instant = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"invalid ISO-8601 timestamp: {value!r}") from exc
    if instant.tzinfo is None:
        instant = instant.replace(tzinfo=timezone.utc)
    instant = instant.astimezone(timezone.utc)
    delta = instant - EPOCH
    whole = delta.days * 86_400 + delta.seconds
    return Fraction(whole, 1) + Fraction(delta.microseconds, 1_000_000)


def _round_fraction(value: Fraction) -> int:
    """Round a non-negative fraction to the nearest integer, halves upward."""
    if value < 0:
        return -_round_fraction(-value)
    quotient, remainder = divmod(value.numerator, value.denominator)
    return quotient + (1 if remainder * 2 >= value.denominator else 0)


def _format_iso8601(epoch_seconds: Fraction) -> str:
    total_ns = _round_fraction(epoch_seconds * NANOSECONDS)
    whole_seconds, nanoseconds = divmod(total_ns, NANOSECONDS)
    instant = EPOCH + timedelta(seconds=whole_seconds)
    result = instant.strftime("%Y-%m-%dT%H:%M:%S")
    if nanoseconds:
        result += "." + f"{nanoseconds:09d}".rstrip("0")
    return result + "Z"


def _json_number(seconds: Fraction) -> int | float:
    if seconds.denominator == 1:
        return seconds.numerator
    nanoseconds = _round_fraction(seconds * NANOSECONDS)
    if nanoseconds % NANOSECONDS == 0:
        return nanoseconds // NANOSECONDS
    return nanoseconds / NANOSECONDS


@dataclass(frozen=True, slots=True)
class SpeedProfile:
    """Piecewise-constant speed, represented by factor changes at each time."""

    times: tuple[Fraction, ...]
    factor_after: tuple[Fraction, ...]

    def factor_at(self, when: Fraction) -> tuple[Fraction, int]:
        position = bisect_right(self.times, when)
        factor = self.factor_after[position - 1] if position else ONE
        return factor, position

    def traverse(self, departure: Fraction, distance: int) -> Optional[Fraction]:
        """Return arrival time, or None when entry is blocked at departure."""
        factor, position = self.factor_at(departure)
        if factor == 0:
            return None
        if distance == 0:
            return departure

        remaining = Fraction(distance, 1)
        current = departure
        count = len(self.times)

        while True:
            if position >= count:
                # Outside all future obstruction changes, normal/default speed
                # (or the last active speed) remains in effect.
                if factor == 0:  # Defensive; well-formed finite windows clear.
                    return None
                return current + remaining / factor

            next_change = self.times[position]
            if factor == 0:
                # A closure that starts after entry pauses movement on the edge.
                current = next_change
            else:
                progress = factor * (next_change - current)
                if remaining <= progress:
                    return current + remaining / factor
                remaining -= progress
                current = next_change

            factor = self.factor_after[position]
            position += 1


EMPTY_PROFILE = SpeedProfile((), ())


def _build_profile(
    intervals: Iterable[tuple[Fraction, Fraction, Fraction]],
) -> SpeedProfile:
    """Merge possibly overlapping windows using the minimum active factor."""
    starts: dict[Fraction, list[Fraction]] = defaultdict(list)
    ends: dict[Fraction, list[Fraction]] = defaultdict(list)
    for start, end, factor in intervals:
        if end <= start:
            continue
        starts[start].append(factor)
        ends[end].append(factor)

    event_times = sorted(set(starts) | set(ends))
    if not event_times:
        return EMPTY_PROFILE

    active: Counter[Fraction] = Counter()
    minimum_heap: list[Fraction] = []
    changes: list[Fraction] = []
    factors: list[Fraction] = []
    current = ONE

    for event_time in event_times:
        # Half-open windows: intervals ending now are removed before intervals
        # starting now are activated.
        for factor in ends.get(event_time, ()):
            active[factor] -= 1
        for factor in starts.get(event_time, ()):
            active[factor] += 1
            heapq.heappush(minimum_heap, factor)
        while minimum_heap and active[minimum_heap[0]] <= 0:
            heapq.heappop(minimum_heap)
        effective = minimum_heap[0] if minimum_heap else ONE
        if effective != current:
            changes.append(event_time)
            factors.append(effective)
            current = effective

    return SpeedProfile(tuple(changes), tuple(factors))


@dataclass(frozen=True, slots=True)
class Arc:
    edge_id: str
    target: tuple[int, int]
    base_duration: int
    profile: SpeedProfile


def _optimistic_distances(
    destination: tuple[int, int],
    reverse_graph: Mapping[tuple[int, int], list[tuple[tuple[int, int], Fraction]]],
) -> dict[tuple[int, int], Fraction]:
    """Directed reverse Dijkstra used as an admissible A* potential."""
    distances: dict[tuple[int, int], Fraction] = {destination: ZERO}
    queue: list[tuple[Fraction, int, tuple[int, int]]] = [(ZERO, 0, destination)]
    serial = 1

    while queue:
        distance, _, node = heapq.heappop(queue)
        if distances.get(node) != distance:
            continue
        for predecessor, weight in reverse_graph.get(node, ()):
            candidate = distance + weight
            if candidate < distances.get(predecessor, candidate + ONE):
                distances[predecessor] = candidate
                heapq.heappush(queue, (candidate, serial, predecessor))
                serial += 1
    return distances


def solve_case(case: Mapping[str, Any]) -> dict[str, Any]:
    """Solve one independent case exactly under the challenge semantics."""
    if not isinstance(case, Mapping):
        raise ValueError("each case must be an object")

    start = _coordinate(case["start_coordinate"], "start_coordinate")
    destination = _coordinate(case["end_coordinate"], "end_coordinate")
    start_epoch = _parse_iso8601(case["start_time"])

    if start == destination:
        return {
            "total_duration_sec": 0,
            "arrival_time": _format_iso8601(start_epoch),
            "path": [],
        }

    raw_edges = case.get("edges", [])
    raw_obstructions = case.get("obstructions", [])
    if not isinstance(raw_edges, list) or not isinstance(raw_obstructions, list):
        raise ValueError("edges and obstructions must be arrays")

    edge_specs: list[tuple[str, tuple[int, int], tuple[int, int], int]] = []
    valid_directions: set[tuple[str, tuple[int, int], tuple[int, int]]] = set()
    known_nodes: set[tuple[int, int]] = {start, destination}

    for index, raw_edge in enumerate(raw_edges):
        if not isinstance(raw_edge, Mapping):
            raise ValueError(f"edges[{index}] must be an object")
        edge_id = raw_edge.get("edge_id")
        if not isinstance(edge_id, str):
            raise ValueError(f"edges[{index}].edge_id must be a string")
        node1 = _coordinate(raw_edge.get("node1"), f"edges[{index}].node1")
        node2 = _coordinate(raw_edge.get("node2"), f"edges[{index}].node2")
        duration_value = raw_edge.get("base_duration_sec")
        if isinstance(duration_value, bool) or not isinstance(duration_value, int):
            raise ValueError(f"edges[{index}].base_duration_sec must be an integer")
        if not 0 <= duration_value <= 999:
            raise ValueError(f"edges[{index}].base_duration_sec is out of range")
        edge_specs.append((edge_id, node1, node2, duration_value))
        valid_directions.add((edge_id, node1, node2))
        valid_directions.add((edge_id, node2, node1))
        known_nodes.update((node1, node2))

    raw_nodes = case.get("nodes", [])
    if not isinstance(raw_nodes, list):
        raise ValueError("nodes must be an array")
    for index, raw_node in enumerate(raw_nodes):
        known_nodes.add(_coordinate(raw_node, f"nodes[{index}]"))

    intervals_by_direction: dict[
        tuple[str, tuple[int, int], tuple[int, int]],
        list[tuple[Fraction, Fraction, Fraction]],
    ] = defaultdict(list)
    max_speed_by_direction: dict[
        tuple[str, tuple[int, int], tuple[int, int]], Fraction
    ] = defaultdict(lambda: ONE)
    fifo_from = ZERO

    for index, raw_obstruction in enumerate(raw_obstructions):
        if not isinstance(raw_obstruction, Mapping):
            raise ValueError(f"obstructions[{index}] must be an object")
        edge_id = raw_obstruction.get("edge_id")
        edge_value = raw_obstruction.get("edge")
        if not isinstance(edge_id, str) or not isinstance(edge_value, Mapping):
            raise ValueError(f"obstructions[{index}] has invalid edge identity")
        source = _coordinate(edge_value.get("from"), f"obstructions[{index}].edge.from")
        target = _coordinate(edge_value.get("to"), f"obstructions[{index}].edge.to")
        key = (edge_id, source, target)

        # Per the statement, both edge_id and direction must match.
        if key not in valid_directions:
            continue

        interval_start = _parse_iso8601(raw_obstruction.get("start_time")) - start_epoch
        interval_end = _parse_iso8601(raw_obstruction.get("end_time")) - start_epoch
        factor = _as_fraction(raw_obstruction.get("speed_factor"), "speed_factor")
        if factor < 0:
            raise ValueError("speed_factor cannot be negative")
        if interval_end <= interval_start or interval_end <= 0:
            continue

        intervals_by_direction[key].append((interval_start, interval_end, factor))
        if factor > max_speed_by_direction[key]:
            max_speed_by_direction[key] = factor
        if factor == 0 and interval_end > fifo_from:
            # After the final zero-speed window, every edge traversal function
            # is total and FIFO, so the earliest label at a node dominates.
            fifo_from = interval_end

    profiles = {
        key: _build_profile(intervals)
        for key, intervals in intervals_by_direction.items()
    }

    graph: dict[tuple[int, int], list[Arc]] = {node: [] for node in known_nodes}
    reverse_graph: dict[
        tuple[int, int], list[tuple[tuple[int, int], Fraction]]
    ] = defaultdict(list)

    for edge_id, node1, node2, base_duration in edge_specs:
        for source, target in ((node1, node2), (node2, node1)):
            key = (edge_id, source, target)
            profile = profiles.get(key, EMPTY_PROFILE)
            graph[source].append(Arc(edge_id, target, base_duration, profile))
            max_speed = max_speed_by_direction[key]
            optimistic = (
                ZERO if base_duration == 0 else Fraction(base_duration, 1) / max_speed
            )
            reverse_graph[target].append((source, optimistic))

    heuristic = _optimistic_distances(destination, reverse_graph)
    if start not in heuristic:
        return _unreachable()

    # Labels retain parent links so repeated edge_ids from deliberate cycling
    # can be reconstructed exactly without copying whole paths into the heap.
    label_nodes: list[tuple[int, int]] = [start]
    label_times: list[Fraction] = [ZERO]
    label_parents: list[int] = [-1]
    label_edges: list[Optional[str]] = [None]
    label_hops: list[int] = [0]

    queue: list[tuple[Fraction, Fraction, int, int, int]] = []
    heapq.heappush(queue, (heuristic[start], ZERO, 0, 0, 0))
    serial = 1

    # Before fifo_from, a later label can be useful because an earlier arrival
    # may coincide with a no-wait closure. Exact (node,time) states are kept.
    # From fifo_from onward, classic FIFO earliest-arrival dominance is safe.
    pre_fifo_seen: set[tuple[tuple[int, int], Fraction]] = set()
    fifo_best: dict[tuple[int, int], Fraction] = {}
    if ZERO < fifo_from:
        pre_fifo_seen.add((start, ZERO))
    else:
        fifo_best[start] = ZERO

    while queue:
        _, arrival, _, _, label_index = heapq.heappop(queue)
        node = label_nodes[label_index]

        if arrival >= fifo_from and fifo_best.get(node) != arrival:
            continue  # A strictly earlier FIFO-era label superseded this one.

        if node == destination:
            path: list[str] = []
            cursor = label_index
            while label_parents[cursor] != -1:
                edge_id = label_edges[cursor]
                if edge_id is not None:
                    path.append(edge_id)
                cursor = label_parents[cursor]
            path.reverse()
            return {
                "total_duration_sec": _json_number(arrival),
                "arrival_time": _format_iso8601(start_epoch + arrival),
                "path": path,
            }

        for arc in graph.get(node, ()):
            next_arrival = arc.profile.traverse(arrival, arc.base_duration)
            if next_arrival is None or arc.target not in heuristic:
                continue

            if next_arrival >= fifo_from:
                previous = fifo_best.get(arc.target)
                if previous is not None and previous <= next_arrival:
                    continue
                fifo_best[arc.target] = next_arrival
            else:
                state = (arc.target, next_arrival)
                if state in pre_fifo_seen:
                    continue
                pre_fifo_seen.add(state)

            next_index = len(label_nodes)
            next_hops = label_hops[label_index] + 1
            label_nodes.append(arc.target)
            label_times.append(next_arrival)
            label_parents.append(label_index)
            label_edges.append(arc.edge_id)
            label_hops.append(next_hops)
            heapq.heappush(
                queue,
                (
                    next_arrival + heuristic[arc.target],
                    next_arrival,
                    next_hops,
                    serial,
                    next_index,
                ),
            )
            serial += 1

    return _unreachable()


def _solve_case_safely(case: Any) -> dict[str, Any]:
    try:
        return solve_case(case)
    except (KeyError, TypeError, ValueError, OverflowError):
        # The challenge promises valid inputs. Keeping the failure local still
        # preserves the mandatory one-output-per-case batch shape.
        return _unreachable()


def solve_batch(payload: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    if not isinstance(payload, Mapping):
        raise ValueError("request JSON must be an object keyed by case id")
    return {str(case_id): _solve_case_safely(case) for case_id, case in payload.items()}


def _respond(start_response: Any, status: HTTPStatus, body: Any) -> list[bytes]:
    encoded = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    start_response(
        f"{status.value} {status.phrase}",
        [
            ("Content-Type", "application/json; charset=utf-8"),
            ("Content-Length", str(len(encoded))),
            ("Cache-Control", "no-store"),
        ],
    )
    return [encoded]


def application(environ: Mapping[str, Any], start_response: Any) -> list[bytes]:
    """Dependency-free WSGI endpoint."""
    method = str(environ.get("REQUEST_METHOD", "GET")).upper()
    path = str(environ.get("PATH_INFO", ""))

    if method == "GET" and path in ("/", "/health"):
        return _respond(start_response, HTTPStatus.OK, {"status": "ok"})
    if path != "/kan-cheong-delivery-driver":
        return _respond(start_response, HTTPStatus.NOT_FOUND, {"error": "not found"})
    if method != "POST":
        return _respond(
            start_response, HTTPStatus.METHOD_NOT_ALLOWED, {"error": "POST required"}
        )

    try:
        content_length_text = str(environ.get("CONTENT_LENGTH", "") or "0")
        content_length = int(content_length_text)
        if content_length < 0 or content_length > MAX_BODY_BYTES:
            return _respond(
                start_response,
                HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                {"error": "request body too large"},
            )
        raw_body = environ["wsgi.input"].read(content_length)
        payload = json.loads(raw_body.decode("utf-8"), parse_float=Decimal)
        result = solve_batch(payload)
    except (KeyError, UnicodeDecodeError, json.JSONDecodeError, ValueError):
        return _respond(
            start_response, HTTPStatus.BAD_REQUEST, {"error": "invalid batch JSON"}
        )
    return _respond(start_response, HTTPStatus.OK, result)


app = application


class _ThreadingWSGIServer(ThreadingMixIn, WSGIServer):
    daemon_threads = True


def _self_test() -> None:
    """Fast deterministic checks for the challenge's hardest semantics."""
    simple = {
        "start_coordinate": [0, 0],
        "end_coordinate": [1, 0],
        "start_time": "2026-06-10T08:30:00Z",
        "nodes": [[0, 0], [1, 0]],
        "edges": [
            {
                "edge_id": "edge_0",
                "node1": [0, 0],
                "node2": [1, 0],
                "base_duration_sec": 60,
            }
        ],
        "obstructions": [],
    }
    assert solve_case(simple) == {
        "total_duration_sec": 60,
        "arrival_time": "2026-06-10T08:31:00Z",
        "path": ["edge_0"],
    }

    blocked = dict(simple)
    blocked["obstructions"] = [
        {
            "edge_id": "edge_0",
            "edge": {"from": [0, 0], "to": [1, 0]},
            "start_time": "2026-06-10T08:00:00Z",
            "end_time": "2026-06-10T09:00:00Z",
            "speed_factor": Decimal("0.0"),
        }
    ]
    assert solve_case(blocked) == _unreachable()

    cycle = {
        "start_coordinate": [0, 0],
        "end_coordinate": [2, 0],
        "start_time": "2026-06-10T08:30:00Z",
        "nodes": [[0, 0], [1, 0], [2, 0]],
        "edges": [
            {"edge_id": "edge_0", "node1": [0, 0], "node2": [1, 0], "base_duration_sec": 10},
            {"edge_id": "edge_1", "node1": [1, 0], "node2": [2, 0], "base_duration_sec": 10},
            {"edge_id": "edge_2", "node1": [0, 0], "node2": [2, 0], "base_duration_sec": 20},
        ],
        "obstructions": [
            {
                "edge_id": "edge_1",
                "edge": {"from": [1, 0], "to": [2, 0]},
                "start_time": "2026-06-10T08:30:10Z",
                "end_time": "2026-06-10T08:30:20Z",
                "speed_factor": 0,
            },
            {
                "edge_id": "edge_1",
                "edge": {"from": [1, 0], "to": [2, 0]},
                "start_time": "2026-06-10T08:30:30Z",
                "end_time": "2026-06-10T08:30:40Z",
                "speed_factor": 0,
            },
            {
                "edge_id": "edge_2",
                "edge": {"from": [0, 0], "to": [2, 0]},
                "start_time": "2026-06-10T08:30:00Z",
                "end_time": "2026-06-10T08:32:00Z",
                "speed_factor": Decimal("0.2"),
            },
        ],
    }
    assert solve_case(cycle) == {
        "total_duration_sec": 60,
        "arrival_time": "2026-06-10T08:31:00Z",
        "path": ["edge_0", "edge_0", "edge_0", "edge_0", "edge_0", "edge_1"],
    }

    # A closure beginning during traversal pauses only the remaining portion:
    # 30 s progress, 60 s stopped, then 30 s progress => 120 s total.
    mid_edge = dict(simple)
    mid_edge["obstructions"] = [
        {
            "edge_id": "edge_0",
            "edge": {"from": [0, 0], "to": [1, 0]},
            "start_time": "2026-06-10T08:30:30Z",
            "end_time": "2026-06-10T08:31:30Z",
            "speed_factor": 0,
        }
    ]
    assert solve_case(mid_edge)["total_duration_sec"] == 120

    zero_edge = {
        "start_coordinate": [0, 0],
        "end_coordinate": [1, 0],
        "start_time": "2026-06-10T08:30:00Z",
        "nodes": [[0, 0], [1, 0]],
        "edges": [
            {"edge_id": "instant", "node1": [0, 0], "node2": [1, 0], "base_duration_sec": 0}
        ],
        "obstructions": [],
    }
    assert solve_case(zero_edge)["total_duration_sec"] == 0
    assert solve_case({**zero_edge, "end_coordinate": [0, 0]})["path"] == []
    print("all self-tests passed")


def main() -> None:
    if "--self-test" in sys.argv:
        _self_test()
        return
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8000"))
    with make_server(host, port, application, server_class=_ThreadingWSGIServer) as server:
        print(f"Listening on http://{host}:{port}", flush=True)
        server.serve_forever()


if __name__ == "__main__":
    main()
