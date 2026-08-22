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
MAX_LOCAL_DEPTH = 4
MAX_WALK_COUNT = 64
LOCAL_PATH_DECAY = 0.40
MODEL_VERSION = "phase1-local-motif-v4"
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

    def _walk_profile(
        self, start: str, target: str, max_depth: int = MAX_LOCAL_DEPTH
    ) -> list[int]:
        """Count bounded directed walks of each exact length.

        Phase 1 is about the *increment* caused by the incoming edge.  Counting
        arbitrary reachability made an evolved graph look almost completely risky:
        once the graph became connected, even a remote accidental route was treated
        like a deliberate return.  A truncated Katz-style profile keeps the useful
        short-path signal and discounts remote connectivity.  Counts and depth are
        capped so work remains bounded on dense multigraphs.
        """
        profile = [0] * (max_depth + 1)
        frontier: dict[str, int] = {start: 1}
        for depth in range(1, max_depth + 1):
            next_frontier: dict[str, int] = defaultdict(int)
            for node, ways in frontier.items():
                for nxt, multiplicity in self.out.get(node, {}).items():
                    # A degenerate self-transfer must not manufacture many local
                    # paths or turn an ordinary edge into an apparent multi-loop.
                    if nxt == node:
                        continue
                    contribution = min(MAX_WALK_COUNT, ways * multiplicity)
                    next_frontier[nxt] = min(
                        MAX_WALK_COUNT, next_frontier[nxt] + contribution
                    )
            profile[depth] = next_frontier.get(target, 0)
            frontier = next_frontier
            if not frontier:
                break
        return profile

    @staticmethod
    def _weighted_paths(profile: list[int], weights: dict[int, float]) -> float:
        return sum(profile[depth] * weight for depth, weight in weights.items())

    def _cycle_context(self, node: str) -> float:
        """Short non-degenerate closed-walk capacity already touching ``node``."""
        profile = self._walk_profile(node, node)
        return sum(
            profile[depth] * (LOCAL_PATH_DECAY ** (depth - 2))
            for depth in range(2, MAX_LOCAL_DEPTH + 1)
        )

    def _diamond_routes(self, source: str, target: str) -> int:
        """Count the local convergence motif created by ``source -> target``.

        For P->source plus P->middle->target, the new edge gives P a second route
        to target.  This is the exact structural relationship in the convergence
        example and is far less vulnerable to unrelated long paths than intersecting
        the transitive closure of the whole graph.
        """
        total = 0
        for parent, parent_to_source in self.inn.get(source, {}).items():
            for middle, parent_to_middle in self.out.get(parent, {}).items():
                if middle == source:
                    continue
                middle_to_target = self.out.get(middle, {}).get(target, 0)
                total += parent_to_source * parent_to_middle * middle_to_target
                if total >= MAX_WALK_COUNT:
                    return MAX_WALK_COUNT
        return total

    def _structural_score(self, source: str, target: str) -> tuple[float, dict[str, Any]]:
        existing_parallel = self.out.get(source, {}).get(target, 0)
        repetition = saturate(existing_parallel, 1.0)

        # A self-transfer does not create or shorten a path between distinct
        # entities. It can reinforce an already recurrent component, but a lone
        # self-loop must rank far below a genuine multi-entity return path.
        if source == target:
            surrounding_cycles = self._cycle_context(source)
            score = min(
                0.18,
                0.05 * repetition + 0.10 * saturate(surrounding_cycles, 0.5),
            )
            return score, {
                "classification": "degenerate_self_loop",
                "newPairs": 0,
                "convergencePairs": 0,
                "shortcutPairs": 0,
                "existingParallelEdges": existing_parallel,
                "returnPaths": 0,
                "surroundingCycleStrength": round(surrounding_cycles, 6),
                "activeEdgesBefore": len(self.expiry_heap),
            }

        return_profile = self._walk_profile(target, source)
        # A reciprocal or two-hop return is strong. Three- and four-hop routes are
        # still evidence, but are aggressively attenuated because accidental long
        # paths become common as a transaction graph grows.
        return_strength = self._weighted_paths(
            return_profile, {1: 1.15, 2: 1.0, 3: 0.35, 4: 0.12}
        )
        distant_return_distance: int | None = None
        if return_strength == 0:
            distant_return_distance = self._shortest_distance(target, source)
            if distant_return_distance is not None:
                # Preserve the principle that an arbitrarily long return is still
                # more recurrent than a one-way extension, without allowing remote
                # connectivity to dominate a dense graph.
                return_strength = 0.08 * (
                    0.5 ** max(0, distant_return_distance - MAX_LOCAL_DEPTH - 1)
                )
        diamond_routes = self._diamond_routes(source, target)
        shortcut_profile = self._walk_profile(source, target)
        shortcut_strength = self._weighted_paths(
            shortcut_profile, {2: 1.0, 3: 0.35, 4: 0.12}
        )
        continuation_edges = sum(self.inn.get(source, {}).values())
        branch_edges = sum(self.out.get(source, {}).values())
        features: dict[str, Any] = {
            "existingParallelEdges": existing_parallel,
            "returnWalksByDepth": return_profile[1:],
            "returnStrength": round(return_strength, 6),
            "distantReturnDistance": distant_return_distance,
            "diamondRoutes": diamond_routes,
            "shortcutWalksByDepth": shortcut_profile[1:],
            "shortcutStrength": round(shortcut_strength, 6),
            "continuationEdges": continuation_edges,
            "branchEdges": branch_edges,
            "activeEdgesBefore": len(self.expiry_heap),
        }

        if return_strength > 0:
            # This edge closes target->...->source->target. Existing short cycles at
            # the destination mean it adds a second recurrent route into the same
            # node: precisely the multi-loop distinction in the challenge.
            cycle_context = self._cycle_context(target)
            score = (
                0.26
                + 0.62 * saturate(return_strength, 0.8)
                + 0.22 * saturate(cycle_context, 0.5)
                + 0.02 * repetition
            )
            features.update(
                {
                    "classification": "return",
                    "destinationCycleContext": round(cycle_context, 6),
                }
            )
            return min(1.0, score), features

        if diamond_routes:
            classification = "convergence"
            score = 0.30 + 0.20 * saturate(diamond_routes, 1.0)
        elif shortcut_strength > 0:
            classification = "shortcut"
            score = 0.20 + 0.12 * saturate(shortcut_strength, 1.0)
        elif existing_parallel:
            classification = "repeated_edge"
            score = 0.15 + 0.06 * repetition
        else:
            classification = "extension" if continuation_edges else "ordinary"
            score = (
                0.11 * saturate(continuation_edges, 1.0)
                + 0.035 * saturate(branch_edges, 1.0)
            )

        features.update(
            {
                "classification": classification,
            }
        )
        return min(0.52, score), features

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
