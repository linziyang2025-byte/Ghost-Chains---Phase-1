#!/usr/bin/env python3
"""Ghost Chains Phase 1 streaming structural-risk service.

The model deliberately uses topology and time only. Amount and identity fields
are accepted for forward compatibility but do not influence Phase 1 scores.
"""
from __future__ import annotations

import heapq
import json
import math
import os
import threading
from collections import defaultdict, deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

WINDOW_SECONDS = 24 * 60 * 60
MAX_GRAPH_VISITS = 20_000
MAX_SIMPLE_PATHS = 16
MODEL_VERSION = "phase1-evidence-v3"
TRACE_JSON = os.environ.get("TRACE_JSON", "1").lower() not in {"0", "false", "off", "no"}


def trace(event: str, **fields: Any) -> None:
    """Emit one searchable JSON record to Render application logs."""
    if not TRACE_JSON:
        return
    record = {"event": event, **fields}
    print(
        json.dumps(record, ensure_ascii=False, separators=(",", ":"), default=str),
        flush=True,
    )


def parse_time(value: str) -> float:
    if not isinstance(value, str):
        raise ValueError("createdAt must be an ISO 8601 string")
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        dt = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError("createdAt must be a valid ISO 8601 timestamp") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def canonical(tx: dict[str, Any]) -> str:
    try:
        return json.dumps(
            tx, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("transaction must contain valid JSON values") from exc


def saturate(value: float, scale: float = 1.0) -> float:
    """Monotone [0, 1) transform that prevents large graphs dominating scores."""
    return 1.0 - math.exp(-max(0.0, value) / scale)


class RiskGraph:
    """Incremental directed multigraph with an event-time 24-hour window."""

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.reset()

    def reset(self) -> None:
        self.out: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self.inn: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self.expiry_heap: list[tuple[float, int, str, str]] = []
        self.sequence = 0
        self.watermark: float | None = None
        # Idempotency is independent of the active graph: an expired duplicate must
        # still return its original score without rebuilding old graph state.
        self.seen: dict[str, tuple[str, float]] = {}

    @staticmethod
    def _validate(tx: Any) -> tuple[str, str, str, float, str]:
        if not isinstance(tx, dict):
            raise ValueError("each transaction must be an object")
        txid, source, target = tx.get("txId"), tx.get("fromUserId"), tx.get("toUserId")
        if not all(isinstance(item, str) and item for item in (txid, source, target)):
            raise ValueError("txId, fromUserId and toUserId must be non-empty strings")
        if isinstance(tx.get("amount"), bool):
            raise ValueError("amount must be a finite non-negative number")
        try:
            amount = float(tx["amount"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("amount must be a finite non-negative number") from exc
        if not math.isfinite(amount) or amount < 0:
            raise ValueError("amount must be a finite non-negative number")
        timestamp = parse_time(tx.get("createdAt"))
        return txid, source, target, timestamp, canonical(tx)

    def _remove_expired(self) -> None:
        if self.watermark is None:
            return
        # "Within the most recent 24 hours" is inclusive at the exact boundary:
        # active event-time interval is [watermark - 24h, watermark].
        cutoff = self.watermark - WINDOW_SECONDS
        while self.expiry_heap and self.expiry_heap[0][0] < cutoff:
            _, _, source, target = heapq.heappop(self.expiry_heap)
            self.out[source][target] -= 1
            self.inn[target][source] -= 1
            if self.out[source][target] == 0:
                del self.out[source][target]
            if self.inn[target][source] == 0:
                del self.inn[target][source]
            if not self.out[source]:
                del self.out[source]
            if not self.inn[target]:
                del self.inn[target]

    def _reachable(self, start: str, reverse: bool = False) -> set[str]:
        graph = self.inn if reverse else self.out
        found, queue = {start}, deque([start])
        while queue and len(found) < MAX_GRAPH_VISITS:
            node = queue.popleft()
            for nxt in graph.get(node, {}):
                if nxt not in found:
                    found.add(nxt)
                    queue.append(nxt)
        return found

    def _shortest_distance(self, start: str, target: str) -> int | None:
        if start == target:
            return 0
        seen, queue = {start}, deque([(start, 0)])
        while queue and len(seen) < MAX_GRAPH_VISITS:
            node, distance = queue.popleft()
            for nxt in self.out.get(node, {}):
                if nxt == target:
                    return distance + 1
                if nxt not in seen:
                    seen.add(nxt)
                    queue.append((nxt, distance + 1))
        return None

    def _count_simple_paths(
        self, start: str, target: str, max_paths: int = MAX_SIMPLE_PATHS
    ) -> int:
        """Count capped shortest simple paths, including parallel-edge capacity.

        Breadth-first dynamic programming avoids exponential path enumeration on
        dense streaming graphs while still distinguishing independent return routes.
        """
        if start == target:
            return 1
        distance = {start: 0}
        ways = {start: 1}
        queue = deque([start])
        target_distance: int | None = None
        while queue and len(distance) < MAX_GRAPH_VISITS:
            node = queue.popleft()
            next_distance = distance[node] + 1
            if target_distance is not None and next_distance > target_distance:
                continue
            for nxt, multiplicity in self.out.get(node, {}).items():
                contribution = min(max_paths, ways[node] * multiplicity)
                if nxt not in distance:
                    distance[nxt] = next_distance
                    ways[nxt] = contribution
                    if nxt == target:
                        target_distance = next_distance
                    else:
                        queue.append(nxt)
                elif distance[nxt] == next_distance:
                    ways[nxt] = min(max_paths, ways[nxt] + contribution)
        return ways.get(target, 0)

    def _cycles_through(self, node: str) -> int:
        """Capped count of non-degenerate cycle branches through the node."""
        can_return = self._reachable(node, reverse=True)
        total = 0
        for nxt, multiplicity in self.out.get(node, {}).items():
            if nxt == node or total >= MAX_SIMPLE_PATHS:
                continue
            if nxt in can_return:
                total += min(MAX_SIMPLE_PATHS - total, multiplicity)
        return total

    def _return_route_branches(self, start: str, target: str) -> int:
        """Count distinct first-leg capacity on routes from start to target.

        Unlike shortest-path counting, this also recognizes independent return
        routes with different lengths, while remaining linear in graph size.
        """
        if start == target:
            return 0
        can_reach_target = self._reachable(target, reverse=True)
        total = 0
        for nxt, multiplicity in self.out.get(start, {}).items():
            if nxt == start:
                continue
            if nxt == target or nxt in can_reach_target:
                total += multiplicity
                if total >= MAX_SIMPLE_PATHS:
                    return MAX_SIMPLE_PATHS
        return total

    def _path_impact(self, source: str, target: str) -> tuple[int, int, int]:
        """Return (new pairs, convergent pairs, shortcut pairs) from source->target."""
        ancestors = self._reachable(source, reverse=True)
        target_ancestors = self._reachable(target, reverse=True)
        descendants = self._reachable(target)
        source_reachable = self._reachable(source)
        # Common upstream entities already have a different route to the target;
        # source->target therefore creates convergence rather than mere extension.
        convergence_pairs = len((ancestors & target_ancestors) - {source})
        shortcut_pairs = len(source_reachable & descendants)
        potential_pairs = len(ancestors) * len(descendants)
        new_pairs = max(0, potential_pairs - convergence_pairs - shortcut_pairs)
        return new_pairs, convergence_pairs, shortcut_pairs

    def _structural_score(self, source: str, target: str) -> tuple[float, dict[str, Any]]:
        existing_parallel = self.out.get(source, {}).get(target, 0)
        repetition = saturate(existing_parallel, 1.5)

        # A self-transfer does not create or shorten a path between distinct
        # entities. It can reinforce an already recurrent component, but a lone
        # self-loop must rank far below a genuine multi-entity return path.
        if source == target:
            surrounding_cycles = self._cycles_through(source)
            score = min(
                0.30,
                0.08 * repetition + 0.18 * saturate(surrounding_cycles, 1.0),
            )
            return score, {
                "classification": "degenerate_self_loop",
                "newPairs": 0,
                "convergencePairs": 0,
                "shortcutPairs": 0,
                "existingParallelEdges": existing_parallel,
                "returnPaths": 0,
                "surroundingCycles": surrounding_cycles,
                "activeEdgesBefore": len(self.expiry_heap),
            }

        new_pairs, convergence_pairs, shortcut_pairs = self._path_impact(source, target)
        shortest_return_paths = self._count_simple_paths(target, source)
        return_route_branches = self._return_route_branches(target, source)
        return_paths = max(shortest_return_paths, return_route_branches)

        novelty = saturate(max(0, new_pairs - 1), 3.0)
        convergence = saturate(convergence_pairs, 2.0)
        features: dict[str, Any] = {
            "newPairs": new_pairs,
            "convergencePairs": convergence_pairs,
            "shortcutPairs": shortcut_pairs,
            "existingParallelEdges": existing_parallel,
            "returnPaths": return_paths,
            "shortestReturnPaths": shortest_return_paths,
            "returnRouteBranches": return_route_branches,
            "activeEdgesBefore": len(self.expiry_heap),
        }

        if return_paths:
            # Closing target->...->source creates a cycle. Existing cycles at either
            # endpoint make the new cycle overlap with recurring flow, which is what
            # distinguishes the official multi-loop example from a single return.
            shared_cycles = self._cycles_through(source) + self._cycles_through(target)
            cycle_routes = saturate(return_paths, 1.0)
            overlap = saturate(shared_cycles, 1.0)
            score = (
                0.58
                + 0.22 * cycle_routes
                + 0.14 * overlap
                + 0.03 * convergence
                + 0.02 * novelty
                + 0.01 * repetition
            )
            features.update(
                {
                    "classification": "return",
                    "sharedCycles": shared_cycles,
                    "cycleRouteSignal": round(cycle_routes, 6),
                    "overlapSignal": round(overlap, 6),
                }
            )
            return min(1.0, score), features

        prior_distance = self._shortest_distance(source, target)
        shortcut = 0.0
        if prior_distance is not None:
            shortcut = saturate(
                max(1, prior_distance - 1) + max(0, shortcut_pairs - 1), 1.5
            )
        fan_in = saturate(len(self.inn.get(target, {})), 1.5)
        continuation = 1.0 if self.inn.get(source) else 0.0
        branching = saturate(len(self.out.get(source, {})), 1.0)

        score = (
            0.10 * novelty
            + 0.42 * convergence
            + 0.18 * shortcut
            + 0.12 * fan_in
            + 0.05 * continuation
            + 0.05 * branching
            + 0.08 * repetition
        )
        # Any genuine return path should remain above acyclic patterns.
        features.update(
            {
                "classification": "acyclic",
                "priorDistance": prior_distance,
                "fanIn": len(self.inn.get(target, {})),
                "continuation": bool(self.inn.get(source)),
                "branchOutDegree": len(self.out.get(source, {})),
            }
        )
        return min(0.57, score), features

    def score(self, tx: Any) -> float:
        txid, source, target, timestamp, digest = self._validate(tx)
        with self.lock:
            duplicate = self.seen.get(txid)
            if duplicate is not None:
                old_digest, old_score = duplicate
                if old_digest != digest:
                    raise ValueError("txId was reused with a different payload")
                trace(
                    "tx_score",
                    tx=tx,
                    riskScore=old_score,
                    duplicate=True,
                    watermark=self.watermark,
                    activeEdges=len(self.expiry_heap),
                )
                return old_score

            self.watermark = timestamp if self.watermark is None else max(self.watermark, timestamp)
            self._remove_expired()
            cutoff = self.watermark - WINDOW_SECONDS

            # A late event strictly older than 24 hours cannot change current state.
            if timestamp < cutoff:
                score = 0.0
                features: dict[str, Any] = {
                    "classification": "late_expired",
                    "timestamp": timestamp,
                    "cutoff": cutoff,
                }
            else:
                raw_score, features = self._structural_score(source, target)
                score = round(max(0.0, min(1.0, raw_score)), 6)
                self.sequence += 1
                heapq.heappush(
                    self.expiry_heap, (timestamp, self.sequence, source, target)
                )
                self.out[source][target] += 1
                self.inn[target][source] += 1

            self.seen[txid] = (digest, score)
            trace(
                "tx_score",
                tx=tx,
                riskScore=score,
                duplicate=False,
                watermark=self.watermark,
                cutoff=cutoff,
                features=features,
                activeEdgesAfter=len(self.expiry_heap),
            )
            return score

    def process_batch(self, transactions: list[Any]) -> list[dict[str, Any]]:
        # Keep the entire batch atomic relative to concurrent requests.
        with self.lock:
            return [
                {
                    "txId": tx.get("txId") if isinstance(tx, dict) else None,
                    "riskScore": self.score(tx),
                }
                for tx in transactions
            ]


graph = RiskGraph()


class Handler(BaseHTTPRequestHandler):
    def _send(self, status: int, body: dict[str, Any]) -> None:
        data = json.dumps(body, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        path = self.path.split("?", 1)[0]
        if path == "/ghost-chains/health":
            self._send(200, {"status": "ok"})
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self) -> None:
        try:
            length = int(self.headers.get("Content-Length", "0"))
            raw_body = self.rfile.read(length)
            body = json.loads(raw_body or b"{}")
            if not isinstance(body, dict):
                raise ValueError("request body must be a JSON object")
            path = self.path.split("?", 1)[0]
            trace(
                "http_request",
                method="POST",
                path=path,
                contentLength=length,
                body=body,
            )
            if path == "/ghost-chains/reset":
                if body.get("clearTransactions") is not True:
                    raise ValueError("clearTransactions must be true")
                with graph.lock:
                    graph.reset()
                response = {"clearTransactions": True}
                trace("state_reset", response=response)
                self._send(200, response)
                return
            if path == "/ghost-chains/transactions":
                transactions = body.get("transactions")
                if not isinstance(transactions, list):
                    raise ValueError("transactions must be an array")
                response = {"transactions": graph.process_batch(transactions)}
                trace("http_response", path=path, status=200, body=response)
                self._send(200, response)
                return
            self._send(404, {"error": "not found"})
        except (ValueError, TypeError, json.JSONDecodeError) as exc:
            trace("http_error", path=self.path, status=400, error=str(exc))
            self._send(400, {"error": str(exc)})

    def log_message(self, *_: Any) -> None:
        pass


if __name__ == "__main__":
    server = ThreadingHTTPServer(("0.0.0.0", int(os.environ.get("PORT", "8080"))), Handler)
    server.daemon_threads = True
    trace(
        "service_start",
        modelVersion=MODEL_VERSION,
        port=server.server_address[1],
        lookbackSeconds=WINDOW_SECONDS,
        boundary="inclusive",
    )
    server.serve_forever()
