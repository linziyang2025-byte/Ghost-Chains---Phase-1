# Ghost Chains — Phase 1

Run with `python3 app.py` (or set `PORT`). The implementation is dependency-free and exposes the three required endpoints.

## Research basis

The scorer uses an incremental directed transaction graph, bounded temporal state, and structural signals. This follows the problem framing in:

- Wu et al., **GRANDE: a neural model over directed multigraphs with application to anti-money laundering**, 2023: directed multigraphs and edge-level AML scoring ([arXiv](https://arxiv.org/abs/2302.02101)).
- Assumpção et al., **DELATOR: Money Laundering Detection via Multi-Task Learning on Large Transaction Graphs**, 2022: temporal transaction graphs and streaming-scale AML ([arXiv](https://arxiv.org/abs/2205.10293)).
- Alarab & Prakoonwit, **Graph-Based LSTM for Anti-money Laundering**, Neural Processing Letters, 2023: temporal graph modelling for AML ([Springer](https://link.springer.com/article/10.1007/s11063-022-10904-8)).

The implementation deliberately keeps Phase 1 signals structural only; optional identity and amount fields are accepted and preserved for later phases without affecting today's score.
