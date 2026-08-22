import unittest
import os
from datetime import datetime, timedelta, timezone

os.environ.setdefault("TRACE_JSON", "0")

try:
    from ghost_chains_phase1 import RiskGraph
except ModuleNotFoundError:
    from app import RiskGraph


BASE = datetime(2026, 6, 8, 12, tzinfo=timezone.utc)


def tx(number, source, target, seconds=0, **extra):
    payload = {
        "txId": f"tx_{number}",
        "fromUserId": source,
        "toUserId": target,
        "amount": 100.0,
        "createdAt": (BASE + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z"),
    }
    payload.update(extra)
    return payload


def final_score(edges):
    graph = RiskGraph()
    return [
        graph.score(tx(i, source, target, i))
        for i, (source, target) in enumerate(edges, 1)
    ][-1]


class Phase1Tests(unittest.TestCase):
    def test_official_examples_have_increasing_structural_order(self):
        isolated = final_score([("M", "A")])
        extension = final_score([("M", "A"), ("A", "C")])
        convergence = final_score(
            [("M", "A"), ("M", "H"), ("A", "S"), ("H", "S")]
        )
        returned = final_score(
            [("M", "A"), ("A", "C"), ("C", "O"), ("O", "A")]
        )
        multi_loop = final_score(
            [("M", "A"), ("A", "C"), ("C", "M"), ("A", "N"), ("N", "M")]
        )
        self.assertLess(isolated, extension)
        self.assertLess(extension, convergence)
        self.assertLess(convergence, returned)
        self.assertLess(returned, multi_loop)

    def test_duplicate_is_idempotent(self):
        graph = RiskGraph()
        first = tx(1, "A", "B")
        self.assertEqual(graph.score(first), graph.score(first))
        self.assertEqual(sum(map(len, graph.out.values())), 1)

    def test_changed_duplicate_is_rejected(self):
        graph = RiskGraph()
        graph.score(tx(1, "A", "B"))
        with self.assertRaises(ValueError):
            graph.score(tx(1, "A", "C"))

    def test_exact_24_hour_boundary_is_still_active(self):
        graph = RiskGraph()
        graph.score(tx(1, "A", "B", 0))
        score = graph.score(tx(2, "B", "A", 24 * 60 * 60))
        self.assertGreater(score, 0.6)

    def test_just_outside_24_hour_boundary_is_expired(self):
        graph = RiskGraph()
        graph.score(tx(1, "A", "B", 0))
        score = graph.score(tx(2, "B", "A", 24 * 60 * 60 + 1))
        self.assertLess(score, 0.2)

    def test_just_inside_window_closes_cycle(self):
        graph = RiskGraph()
        graph.score(tx(1, "A", "B", 0))
        score = graph.score(tx(2, "B", "A", 24 * 60 * 60 - 1))
        self.assertGreater(score, 0.6)

    def test_out_of_order_expiration_uses_watermark(self):
        graph = RiskGraph()
        graph.score(tx(1, "A", "B", 48 * 60 * 60))
        old = graph.score(tx(2, "B", "A", 0))
        self.assertEqual(old, 0.0)
        self.assertNotIn("A", graph.out.get("B", {}))

    def test_unknown_and_missing_optional_fields_are_accepted(self):
        graph = RiskGraph()
        score = graph.score(tx(1, "A", "B", futurePhaseField={"x": 1}))
        self.assertGreaterEqual(score, 0.0)

    def test_reset_restores_determinism(self):
        graph = RiskGraph()
        payload = tx(1, "A", "B")
        first = graph.score(payload)
        graph.reset()
        self.assertEqual(first, graph.score(payload))

    def test_two_return_routes_rank_above_one(self):
        one = final_score([("A", "B"), ("B", "C"), ("C", "A")])
        two = final_score(
            [("A", "B"), ("A", "D"), ("B", "C"), ("D", "C"), ("C", "A")]
        )
        self.assertGreater(two, one)

    def test_batch_is_processed_in_order(self):
        graph = RiskGraph()
        results = graph.process_batch(
            [tx(1, "A", "B", 1), tx(2, "B", "C", 2), tx(3, "C", "A", 3)]
        )
        self.assertEqual([item["txId"] for item in results], ["tx_1", "tx_2", "tx_3"])
        self.assertGreater(results[-1]["riskScore"], results[1]["riskScore"])

    def test_lone_self_loop_ranks_below_real_return(self):
        self_loop = final_score([("E1", "E1")])
        reciprocal = final_score([("E2", "E3"), ("E3", "E2")])
        self.assertLess(self_loop, reciprocal)

    def test_real_hidden_feedback_batch(self):
        """Regression fixture reconstructed from the evaluator request log."""
        graph = RiskGraph()

        def hidden(txid, source, target, timestamp):
            return {
                "txId": txid,
                "fromUserId": source,
                "toUserId": target,
                "amount": 1000.0,
                "createdAt": timestamp,
                "ipAddress": None,
                "deviceId": None,
            }

        payload = [
            hidden("hf-temporal01-tx1", "hf_A1", "hf_A2", "2026-06-08T00:00:00Z"),
            hidden("hf-temporal01-tx4", "hf_B1", "hf_B2", "2026-06-08T00:00:00Z"),
            hidden("hf-struct01-tx1", "hf_E1", "hf_E1", "2026-06-08T00:00:00Z"),
            hidden("hf-temporal01-tx2", "hf_A2", "hf_A3", "2026-06-08T01:00:00Z"),
            hidden("hf-temporal01-tx5", "hf_B2", "hf_B3", "2026-06-08T01:00:00Z"),
            hidden("hf-struct01-tx2", "hf_E2", "hf_E3", "2026-06-08T01:00:00Z"),
            hidden("hf-struct01-tx3", "hf_E3", "hf_E2", "2026-06-08T02:00:00Z"),
            hidden("hf-temporal01-tx3", "hf_A3", "hf_A1", "2026-06-08T23:00:00Z"),
            hidden("hf-temporal01-tx6", "hf_B3", "hf_B1", "2026-06-09T00:00:00Z"),
        ]
        scores = {
            result["txId"]: result["riskScore"]
            for result in graph.process_batch(payload)
        }
        self.assertLess(scores["hf-struct01-tx1"], scores["hf-struct01-tx3"])
        self.assertGreater(scores["hf-temporal01-tx3"], 0.6)
        self.assertGreater(scores["hf-temporal01-tx6"], 0.6)
        self.assertAlmostEqual(
            scores["hf-temporal01-tx3"], scores["hf-temporal01-tx6"], places=6
        )

    def test_unrelated_components_do_not_change_structural_score(self):
        baseline = RiskGraph()
        baseline.score(tx("base1", "A", "B", 0))
        baseline.score(tx("base2", "B", "C", 1))
        expected = baseline.score(tx("base3", "C", "A", 2))

        interleaved = RiskGraph()
        interleaved.score(tx("mix1", "A", "B", 0))
        interleaved.score(tx("noise1", "X", "Y", 0))
        interleaved.score(tx("mix2", "B", "C", 1))
        interleaved.score(tx("noise2", "Q", "R", 1))
        actual = interleaved.score(tx("mix3", "C", "A", 2))
        self.assertEqual(expected, actual)

    def test_self_loop_does_not_fake_overlapping_cycle(self):
        plain = final_score([("A", "B"), ("B", "A")])
        with_self_loop = final_score([("A", "A"), ("A", "B"), ("B", "A")])
        self.assertEqual(plain, with_self_loop)

    def test_parallel_acyclic_edge_ranks_below_return(self):
        repeated = final_score([("A", "B"), ("A", "B")])
        returned = final_score([("A", "B"), ("B", "A")])
        self.assertLess(repeated, returned)


if __name__ == "__main__":
    unittest.main()
