#!/usr/bin/env python3
"""Ghost Chains - Phase 1 reference-quality streaming risk service.

Run with: python3 ghost_chains_service.py
The PORT environment variable is honoured (default: 8080).
"""

from collections import defaultdict, deque
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

WINDOW_SECONDS = 24 * 60 * 60
MAX_PATH_DEPTH = 32


def parse_time(value):
    if not isinstance(value, str):
        raise ValueError("createdAt must be an ISO 8601 string")
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        result = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError("createdAt must be an ISO 8601 timestamp") from exc
    if result.tzinfo is None:
        result = result.replace(tzinfo=timezone.utc)
    return result.astimezone(timezone.utc)


def canonical_payload(tx):
    # Unknown fields are deliberately ignored; known optional fields retain
    # their present/absent distinction for exact duplicate detection.
    known = {key: tx.get(key) for key in
             ("txId", "fromUserId", "toUserId", "amount", "createdAt",
              "ipAddress", "deviceId") if key in tx}
    return json.dumps(known, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False)


class RiskEngine:
    def __init__(self):
        self.lock = threading.RLock()
        self.reset()

    def reset(self):
        with getattr(self, "lock", threading.RLock()):
            self.edges = defaultdict(list)       # source -> (target, time)
            self.active = {}                     # txid -> (time, source, target)
            self.seen = {}                       # txid -> (fingerprint, score)
            self.watermark = None

    def _evict(self, now):
        if self.watermark is None or now > self.watermark:
            self.watermark = now
        cutoff = self.watermark.timestamp() - WINDOW_SECONDS
        expired = [txid for txid, item in self.active.items()
                   if item[0] < cutoff]
        for txid in expired:
            _, source, target = self.active.pop(txid)
            entries = self.edges[source]
            self.edges[source] = [item for item in entries if item[2] != txid]
            if not self.edges[source]:
                del self.edges[source]

    def _adjacency(self):
        # Parallel transactions do not manufacture duplicate topological paths.
        return {source: sorted({target for target, _, _ in values})
                for source, values in self.edges.items() if values}

    def _path_counts(self, source, target):
        """Return bounded counts of existing paths and shortest path length."""
        adjacency = self._adjacency()
        queue = deque([(source, 0, (source,))])
        paths = 0
        shortest = None
        while queue and paths < 1000:
            node, depth, visited = queue.popleft()
            if depth >= MAX_PATH_DEPTH:
                continue
            for nxt in adjacency.get(node, ()):
                new_depth = depth + 1
                if nxt == target:
                    paths += 1
                    shortest = new_depth if shortest is None else min(shortest, new_depth)
                if nxt not in visited:
                    queue.append((nxt, new_depth, visited + (nxt,)))
        return min(paths, 1000), shortest

    def _weighted_paths(self, start, adjacency, reverse=False):
        """Count bounded simple paths from start, including the zero path."""
        if reverse:
            reverse_graph = defaultdict(list)
            for node, neighbours in adjacency.items():
                for neighbour in neighbours:
                    reverse_graph[neighbour].append(node)
            graph = reverse_graph
        else:
            graph = adjacency
        counts = defaultdict(float)
        counts[start] = 1.0
        stack = [(start, frozenset((start,)), 0, 1.0)]
        while stack:
            node, visited, depth, weight = stack.pop()
            if depth >= MAX_PATH_DEPTH or weight < 0.02:
                continue
            for neighbour in graph.get(node, ()):
                if neighbour in visited:
                    continue
                next_weight = weight * 0.68
                counts[neighbour] += next_weight
                if counts[neighbour] < 1000:
                    stack.append((neighbour, visited | {neighbour},
                                  depth + 1, next_weight))
        return counts

    def _shortest_distances(self, adjacency, start):
        distances = {start: 0}
        queue = [start]
        for node in queue:
            for neighbour in adjacency.get(node, ()):
                if neighbour not in distances:
                    distances[neighbour] = distances[node] + 1
                    queue.append(neighbour)
        return distances

    def _path_delta(self, source, target):
        """Measure new, alternate, and shortened paths created by one edge."""
        before = self._adjacency()
        nodes = set(before) | {source, target}
        nodes.update(n for values in before.values() for n in values)
        if target in before.get(source, ()):
            return 0.0, 0, 0.0, 0
        upstream = self._weighted_paths(source, before, reverse=True)
        downstream = self._weighted_paths(target, before, reverse=False)
        path_mass = sum(upstream[u] * downstream[v]
                        for u in upstream for v in downstream)
        cycle_count, _ = self._path_counts(target, source)
        old_distances = {u: self._shortest_distances(before, u) for u in nodes}
        after = {u: list(values) for u, values in before.items()}
        after.setdefault(source, []).append(target)
        new_distances = {u: self._shortest_distances(after, u) for u in nodes}
        shortening = 0.0
        alternative = 0
        for u in nodes:
            old = old_distances[u]
            new = new_distances[u]
            for v, distance in new.items():
                if v not in old:
                    continue
                if distance < old[v]:
                    shortening += (old[v] - distance) / max(old[v], 1)
                elif distance == old[v] and v != source:
                    alternative += 1
        return path_mass, cycle_count, shortening, alternative

    def _score_new_edge(self, source, target):
        # Score the structural change before inserting the edge. A direct
        # return is the strongest signal; multiple existing routes and route
        # shortening/convergence follow. Isolated extensions remain low.
        if source == target:
            return 1.0
        path_mass, cycle_count, shortening, alternative = self._path_delta(source, target)
        score = 0.06
        score += 0.24 * (1.0 - math.exp(-0.32 * max(0.0, path_mass - 1.0)))
        score += 0.42 * (1.0 - math.exp(-0.75 * min(cycle_count, 12)))
        # A new edge that fans into an already-fed destination or leaves a
        # branching source increases structural recurrence even without a
        # complete cycle yet.
        score += 0.18 * (1.0 - math.exp(-0.22 * shortening))
        score += 0.10 * (1.0 - math.exp(-0.12 * min(alternative, 12)))
        return max(0.0, min(1.0, score))

    def score(self, tx):
        if not isinstance(tx, dict):
            raise ValueError("each transaction must be an object")
        required = ("txId", "fromUserId", "toUserId", "amount", "createdAt")
        if any(key not in tx for key in required):
            raise ValueError("transaction is missing a required field")
        txid = tx["txId"]
        source, target = tx["fromUserId"], tx["toUserId"]
        if not isinstance(txid, str) or not txid:
            raise ValueError("txId must be a non-empty string")
        if not isinstance(source, str) or not isinstance(target, str):
            raise ValueError("fromUserId and toUserId must be strings")
        try:
            amount = Decimal(str(tx["amount"]))
        except (InvalidOperation, ValueError):
            raise ValueError("amount must be numeric")
        if not amount.is_finite():
            raise ValueError("amount must be finite")
        timestamp = parse_time(tx["createdAt"])
        fingerprint = hashlib.sha256(canonical_payload(tx).encode()).hexdigest()
        with self.lock:
            previous = self.seen.get(txid)
            if previous is not None:
                if previous[0] != fingerprint:
                    raise ValueError("txId was previously used with a different payload")
                return previous[1]
            self._evict(timestamp)
            score = round(self._score_new_edge(source, target), 6)
            self.edges[source].append((target, timestamp, txid))
            self.active[txid] = (timestamp.timestamp(), source, target)
            self._evict(timestamp)
            self.seen[txid] = (fingerprint, score)
            return score


ENGINE = RiskEngine()


class Handler(BaseHTTPRequestHandler):
    def _send(self, status, payload):
        body = json.dumps(payload, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/ghost-chains/health":
            self._send(200, {"status": "ok"})
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        try:
            body = json.loads(self.rfile.read(length))
            if self.path == "/ghost-chains/reset":
                if not isinstance(body, dict) or body.get("clearTransactions") is not True:
                    raise ValueError("clearTransactions must be true")
                ENGINE.reset()
                self._send(200, {"clearTransactions": True})
                return
            if self.path == "/ghost-chains/transactions":
                transactions = body.get("transactions") if isinstance(body, dict) else None
                if not isinstance(transactions, list):
                    raise ValueError("transactions must be an array")
                results = [{"txId": tx["txId"], "riskScore": ENGINE.score(tx)}
                           for tx in transactions]
                self._send(200, {"transactions": results})
                return
            self._send(404, {"error": "not found"})
        except (ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
            self._send(400, {"error": str(exc)})

    def log_message(self, *_):
        pass


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
