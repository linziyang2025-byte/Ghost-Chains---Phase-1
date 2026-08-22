#!/usr/bin/env python3
"""Ghost Chains Phase 1: small, dependency-free streaming AML scorer."""
from __future__ import annotations

import hashlib
import json
import math
import threading
from collections import defaultdict, deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

WINDOW_SECONDS = 24 * 60 * 60


def parse_time(value: str) -> float:
    if not isinstance(value, str):
        raise ValueError("createdAt must be an ISO 8601 string")
    text = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def canonical(tx: dict[str, Any]) -> str:
    return json.dumps(tx, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


class RiskGraph:
    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.reset()

    def reset(self) -> None:
        self.edges: list[tuple[float, str, str, str]] = []
        self.out: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self.inn: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self.seen: dict[str, tuple[str, float]] = {}

    def _remove_expired(self, now: float) -> None:
        cutoff = now - WINDOW_SECONDS
        while self.edges and self.edges[0][0] <= cutoff:
            _, a, b, _ = self.edges.pop(0)
            self.out[a][b] -= 1
            self.inn[b][a] -= 1
            if not self.out[a][b]: del self.out[a][b]
            if not self.inn[b][a]: del self.inn[b][a]

    def _reachable(self, start: str, reverse: bool = False, limit: int = 2000) -> set[str]:
        graph = self.inn if reverse else self.out
        found, q = {start}, deque([start])
        while q and len(found) < limit:
            node = q.popleft()
            for nxt in graph.get(node, {}):
                if nxt not in found:
                    found.add(nxt); q.append(nxt)
        return found

    def score(self, tx: dict[str, Any]) -> float:
        txid = tx.get("txId")
        a, b = tx.get("fromUserId"), tx.get("toUserId")
        if not isinstance(txid, str) or not isinstance(a, str) or not isinstance(b, str):
            raise ValueError("txId, fromUserId and toUserId are required strings")
        try:
            ts = parse_time(tx["createdAt"])
            amount = float(tx["amount"])
            if not math.isfinite(amount) or amount < 0: raise ValueError
        except (KeyError, TypeError, ValueError):
            raise ValueError("amount and createdAt are invalid")
        digest = canonical(tx)
        with self.lock:
            if txid in self.seen:
                old_digest, old_score = self.seen[txid]
                if old_digest != digest: raise ValueError("txId was reused with a different payload")
                return old_score
            self._remove_expired(ts)

            # Structural novelty is measured before insertion: new paths, converging
            # paths, and especially return paths through the active directed graph.
            reaches_a = self._reachable(a)
            ancestors_a = self._reachable(a, reverse=True)
            return_path = b in reaches_a or a == b
            upstream_overlap = len(ancestors_a & self._reachable(b, reverse=True) - {a})
            downstream_overlap = len(self._reachable(a) & self._reachable(b) - {a, b})
            fan_in = len(self.inn.get(b, {}))
            fan_out = len(self.out.get(a, {}))
            cycle_strength = 1.0 if return_path else 0.0
            convergence = min(1.0, (upstream_overlap + downstream_overlap) / 3.0)
            local = min(1.0, (fan_in + fan_out) / 6.0)
            # Keep isolated flow near zero; reserve the upper range for cycles.
            raw = 0.04 * local + 0.25 * convergence + 0.71 * cycle_strength
            score = round(max(0.0, min(1.0, raw)), 6)
            self.edges.append((ts, a, b, txid))
            self.out[a][b] += 1; self.inn[b][a] += 1
            self.seen[txid] = (digest, score)
            return score


graph = RiskGraph()


class Handler(BaseHTTPRequestHandler):
    def _send(self, status: int, body: dict[str, Any]) -> None:
        data = json.dumps(body, separators=(",", ":")).encode()
        self.send_response(status); self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data))); self.end_headers(); self.wfile.write(data)

    def do_GET(self) -> None:
        if self.path == "/ghost-chains/health": self._send(200, {"status": "ok"})
        else: self._send(404, {"error": "not found"})

    def do_POST(self) -> None:
        try:
            length = int(self.headers.get("Content-Length", "0")); body = json.loads(self.rfile.read(length) or b"{}")
            if self.path == "/ghost-chains/reset":
                if body.get("clearTransactions") is not True: raise ValueError("clearTransactions must be true")
                with graph.lock: graph.reset()
                return self._send(200, {"clearTransactions": True})
            if self.path == "/ghost-chains/transactions":
                txs = body.get("transactions")
                if not isinstance(txs, list): raise ValueError("transactions must be an array")
                results = [{"txId": tx.get("txId"), "riskScore": graph.score(tx)} for tx in txs]
                return self._send(200, {"transactions": results})
            self._send(404, {"error": "not found"})
        except (ValueError, json.JSONDecodeError) as exc: self._send(400, {"error": str(exc)})

    def log_message(self, *_: Any) -> None: pass


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", int(__import__("os").environ.get("PORT", "8080"))), Handler).serve_forever()
