# Ghost Chains — Phase 1

Run locally:

```bash
python3 ghost_chains_phase1.py
```

Render settings:

```text
Build Command: python3 -m py_compile ghost_chains_phase1.py
Start Command: python3 ghost_chains_phase1.py
Health Check Path: /ghost-chains/health
```

## Scoring model

The service maintains an event-time, directed transaction multigraph over the inclusive active interval `[watermark - 24 hours, watermark]`. Every transaction is scored before insertion from its incremental structural effect:

- **New reachability:** how many entity pairs become newly connected.
- **Shortened/repeated paths:** whether an existing route gains a direct or parallel path.
- **Convergence:** whether the new leg creates another route from an upstream entity to an already reachable destination.
- **Return paths:** whether the recipient can already reach the sender, so the new edge closes a directed cycle.
- **Overlapping cycles:** whether a return edge connects to nodes already participating in recurring flow; this makes the official multi-loop example rank above a single return.
- **Degenerate edges:** a lone self-loop does not connect distinct entities and therefore ranks below a genuine return path; repeated self-loops can only add a small reinforcement signal.

Phase 1 does not use amount, IP, or device values. Unknown and absent optional fields remain observable in the idempotency payload and are accepted for later-phase compatibility.

The score bands are intentionally ordinal rather than probabilistic: isolated or degenerate flow is lowest, extension remains low, convergence is intermediate, a genuine multi-entity return is high, and overlapping/independent return routes are highest. Unrelated graph components never affect one another's score.

## Research basis

- Wu et al., **GRANDE: a neural model over directed multigraphs with application to anti-money laundering**, 2023 — directed multigraph and edge-level AML modelling ([arXiv](https://arxiv.org/abs/2302.02101)).
- Assumpção et al., **DELATOR: Money Laundering Detection via Multi-Task Learning on Large Transaction Graphs**, 2022 — temporal transaction graphs and streaming-scale AML ([arXiv](https://arxiv.org/abs/2205.10293)).
- Pocher et al., **Detecting anomalous cryptocurrency transactions**, *Electronic Markets*, 2023 — directed transaction-network analysis for AML ([Springer](https://link.springer.com/article/10.1007/s12525-023-00654-3)).
- García-Pérez et al., **The geometry of suspicious money laundering activities in financial networks**, *EPJ Data Science*, 2022 — cycles, bifurcations, and intersecting cycles as structural AML signals ([Springer](https://link.springer.com/article/10.1140/epjds/s13688-022-00318-w)).

Run verification:

```bash
python3 -m unittest -v test_phase1.py
```

## Evaluation diagnostics

JSON tracing is enabled by default. Open **Render → Monitor → Logs** and search for:

- `http_request` — the complete JSON body received from the evaluator.
- `tx_score` — the original transaction, graph features, time-window state, and score.
- `http_response` — the exact JSON returned to the evaluator.
- `state_reset` / `http_error` — reset calls and invalid requests.

Each record is emitted as one JSON line. Set the Render environment variable `TRACE_JSON=0` after diagnosis if verbose logging is no longer needed.
