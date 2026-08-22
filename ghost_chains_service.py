#!/usr/bin/env python3
"""Ghost Chains Phase 1 streaming risk service.

The implementation is intentionally dependency-free. Start it with:
    python3 ghost_chains_service.py
Render and similar hosts provide PORT through the environment.
"""

from collections import defaultdict, deque
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hashlib
import json
import math
import os
import threading


WINDOW_SECONDS = 24 * 60 * 60
MAX_PATH_DEPTH = 10
MAX_ENUMERATED_PATHS = 4000


def parse_timestamp(value):
    if not isinstance(value, str):
        raise ValueError("createdAt must be an ISO 8601 timestamp")
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError("createdAt must be an ISO 8601 timestamp") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def fingerprint(transaction):
    # Unknown fields are ignored. Absence and present-null remain distinct.
    known_fields = (
        "txId", "fromUserId", "toUserId", "amount", "createdAt",
        "ipAddress", "deviceId",
    )
    known = {key: transaction[key] for key in known_fields if key in transaction}
    encoded = json.dumps(known, sort_keys=True, separators=(",", ":"),
                         ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


class RiskEngine:
    def __init__(self):
        self.lock = threading.RLock()
        self.reset()

    def reset(self):
        with getattr(self, "lock", threading.RLock()):
            # source -> [(target, epoch_seconds, txId)]
            self.edges = defaultdict(list)
            # txId -> (epoch_seconds, source, target)
            self.active = {}
            # Keep duplicate identity stable for the lifetime of the process.
            self.seen = {}
            self.watermark = None

    def _expire(self, event_time):
        """Advance event time and remove transactions older than 24 hours."""
        if self.watermark is None or event_time > self.watermark:
            self.watermark = event_time
        cutoff = self.watermark.timestamp() - WINDOW_SECONDS
        expired_ids = [
            txid for txid, (epoch, _, _) in self.active.items()
            if epoch < cutoff
        ]
        for txid in expired_ids:
            _, source, _ = self.active.pop(txid)
            remaining = [item for item in self.edges[source] if item[2] != txid]
            if remaining:
                self.edges[source] = remaining
            else:
                del self.edges[source]

    def _adjacency(self):
        # Parallel transactions do not create duplicate topological paths.
        return {
            source: tuple(sorted({target for target, _, _ in items}))
            for source, items in self.edges.items()
            if items
        }

    @staticmethod
    def _reverse(adjacency):
        reverse = defaultdict(list)
        for source, targets in adjacency.items():
            for target in targets:
                reverse[target].append(source)
        return reverse

    @staticmethod
    def _reachable(adjacency, start):
        seen = {start}
        queue = deque([start])
        while queue:
            node = queue.popleft()
            for target in adjacency.get(node, ()):
                if target not in seen:
                    seen.add(target)
                    queue.append(target)
        return seen

    @staticmethod
    def _distances(adjacency, start):
        distances = {start: 0}
        queue = deque([start])
        while queue:
            node = queue.popleft()
            for target in adjacency.get(node, ()):
                if target not in distances:
                    distances[target] = distances[node] + 1
                    queue.append(target)
        return distances

    def _simple_paths(self, start, adjacency, reverse=False):
        """Enumerate bounded simple paths, including the zero-length path."""
        graph = self._reverse(adjacency) if reverse else adjacency
        result = [(start, (start,), 1.0)]
        stack = [(start, (start,), 1.0)]
        while stack and len(result) < MAX_ENUMERATED_PATHS:
            node, path, weight = stack.pop()
            if len(path) - 1 >= MAX_PATH_DEPTH:
                continue
            for target in graph.get(node, ()):
                if target in path:
                    continue
                next_weight = weight * 0.70
                next_path = path + (target,)
                result.append((target, next_path, next_weight))
                stack.append((target, next_path, next_weight))
                if len(result) >= MAX_ENUMERATED_PATHS:
                    break
        return result

    def _scc_cycle_strength(self, adjacency, source, target):
        """Return existing cycle strength near the edge being scored."""
        reverse = self._reverse(adjacency)
        undirected = defaultdict(set)
        for node, neighbours in adjacency.items():
            for neighbour in neighbours:
                undirected[node].add(neighbour)
                undirected[neighbour].add(node)
        component = {source, target}
        queue = deque((source, target))
        while queue:
            node = queue.popleft()
            for neighbour in undirected.get(node, ()):
                if neighbour not in component:
                    component.add(neighbour)
                    queue.append(neighbour)

        index = 0
        indices = {}
        lowlink = {}
        stack = []
        on_stack = set()
        cyclic_sizes = []

        def visit(node):
            nonlocal index
            indices[node] = index
            lowlink[node] = index
            index += 1
            stack.append(node)
            on_stack.add(node)
            for neighbour in adjacency.get(node, ()):
                if neighbour not in component:
                    continue
                if neighbour not in indices:
                    visit(neighbour)
                    lowlink[node] = min(lowlink[node], lowlink[neighbour])
                elif neighbour in on_stack:
                    lowlink[node] = min(lowlink[node], indices[neighbour])
            if lowlink[node] == indices[node]:
                members = []
                while True:
                    member = stack.pop()
                    on_stack.remove(member)
                    members.append(member)
                    if member == node:
                        break
                if len(members) > 1 or node in adjacency.get(node, ()):
                    cyclic_sizes.append(len(members))

        for node in component:
            if node not in indices:
                visit(node)
        return min(8.0, float(sum(cyclic_sizes)))

    def _structural_delta(self, source, target):
        """Measure new paths, closed loops, convergence and route shortening."""
        adjacency = self._adjacency()
        if target in adjacency.get(source, ()):
            return 0.0, 0.0, 0.0, 0.0

        upstream = self._simple_paths(source, adjacency, reverse=True)
        downstream = self._simple_paths(target, adjacency, reverse=False)
        new_paths = 0.0
        upstream_starts = set()
        downstream_ends = set()
        for start, up_path, up_weight in upstream:
            upstream_starts.add(start)
            for end, down_path, down_weight in downstream:
                if set(up_path).isdisjoint(down_path):
                    new_paths += up_weight * down_weight
                    downstream_ends.add(end)

        # Existing target -> source paths become cycles when source -> target
        # arrives. This is the central return-path signal in Phase 1.
        return_paths = self._simple_paths(target, adjacency, reverse=False)
        closed_cycles = sum(
            weight for end, _, weight in return_paths if end == source
        )

        # If an upstream node already reached target, this edge adds an
        # alternative route and therefore a convergence signal.
        alternative_paths = 0.0
        for start in upstream_starts:
            if start == source:
                continue
            paths = self._simple_paths(start, adjacency, reverse=False)
            alternative_paths += sum(
                weight for end, _, weight in paths if end == target
            )

        # Shortenings are measured only for pairs touched by the new edge.
        shortening = 0.0
        old_from = {start: self._distances(adjacency, start) for start, _, _ in upstream}
        old_to = {end: self._distances(adjacency, target) for end in downstream_ends}
        for start, _, _ in upstream:
            start_distances = old_from[start]
            if source not in start_distances:
                continue
            for end in downstream_ends:
                old = start_distances.get(end)
                if old is None:
                    continue
                through = start_distances[source] + 1 + old_to[end].get(end, 0)
                if through < old:
                    shortening += (old - through) / max(1, old)

        existing_cycles = self._scc_cycle_strength(adjacency, source, target)
        return new_paths, closed_cycles + 0.45 * existing_cycles, shortening, alternative_paths

    def _score_new_edge(self, source, target):
        if not self.edges:
            return 0.0
        if source == target:
            return 0.85
        new_paths, cycles, shortening, alternatives = self._structural_delta(source, target)
        if new_paths == 0.0 and cycles == 0.0 and shortening == 0.0 and alternatives == 0.0:
            return 0.0

        def saturate(value, scale):
            return 1.0 - math.exp(-max(0.0, value) / scale)

        # Combined graph change: no single feature is the score by itself.
        score = (
            0.12 * saturate(new_paths, 2.0)
            + 0.52 * saturate(cycles, 1.0)
            + 0.20 * saturate(alternatives, 1.0)
            + 0.16 * saturate(shortening, 1.0)
        )
        return max(0.0, min(1.0, round(score, 6)))

    def score(self, transaction):
        if not isinstance(transaction, dict):
            raise ValueError("each transaction must be an object")
        required = ("txId", "fromUserId", "toUserId", "amount", "createdAt")
        if any(field not in transaction for field in required):
            raise ValueError("transaction is missing a required field")

        txid = transaction["txId"]
        source = transaction["fromUserId"]
        target = transaction["toUserId"]
        if not isinstance(txid, str) or not txid:
            raise ValueError("txId must be a non-empty string")
        if not isinstance(source, str) or not isinstance(target, str):
            raise ValueError("fromUserId and toUserId must be strings")
        try:
            amount = Decimal(str(transaction["amount"]))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ValueError("amount must be numeric") from exc
        if not amount.is_finite():
            raise ValueError("amount must be finite")
        event_time = parse_timestamp(transaction["createdAt"])
        tx_fingerprint = fingerprint(transaction)

        with self.lock:
            previous = self.seen.get(txid)
            if previous is not None:
                if previous[0] != tx_fingerprint:
                    raise ValueError("txId was previously used with a different payload")
                return previous[1]

            self._expire(event_time)
            risk_score = self._score_new_edge(source, target)
            epoch = event_time.timestamp()
            self.edges[source].append((target, epoch, txid))
            self.active[txid] = (epoch, source, target)
            self.seen[txid] = (tx_fingerprint, risk_score)
            self._expire(event_time)
            return risk_score


ENGINE = RiskEngine()


class Handler(BaseHTTPRequestHandler):
    def _json(self, status, payload):
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/ghost-chains/health":
            self._json(200, {"status": "ok"})
            return
        self._json(404, {"error": "not found"})

    def do_POST(self):
        try:
            length = int(self.headers.get("Content-Length", "0"))
            body = json.loads(self.rfile.read(length))
            if self.path == "/ghost-chains/reset":
                if not isinstance(body, dict) or body.get("clearTransactions") is not True:
                    raise ValueError("clearTransactions must be true")
                ENGINE.reset()
                self._json(200, {"clearTransactions": True})
                return
            if self.path == "/ghost-chains/transactions":
                transactions = body.get("transactions") if isinstance(body, dict) else None
                if not isinstance(transactions, list):
                    raise ValueError("transactions must be an array")
                results = []
                for transaction in transactions:
                    results.append({
                        "txId": transaction["txId"],
                        "riskScore": ENGINE.score(transaction),
                    })
                self._json(200, {"transactions": results})
                return
            self._json(404, {"error": "not found"})
        except (ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
            self._json(400, {"error": str(exc)})

    def log_message(self, *_):
        pass


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
