from __future__ import annotations

import unittest
from pathlib import Path

import pandas as pd

from fmetl.config.settings import Settings
from fmetl.connectors.processing_relations import ProcessingRelationSource
from fmetl.facts.processing_plan import build_processing_plan
from fmetl.relations.resolver import resolve_relations


class _Response:
    def __init__(self, payload: dict) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self.payload


class _Session:
    def __init__(self, payload: dict) -> None:
        self.payload = payload
        self.calls = 0

    def get(self, url: str, timeout: int) -> _Response:
        self.calls += 1
        return _Response(self.payload)


def _settings() -> Settings:
    return Settings(
        store_id="A3XV", duckdb_path=Path("/tmp/fm_v013_test.duckdb"),
        qdm_host="x", qdm_api_id="x", qdm_access_key="x", qdm_secret_key="x",
        qdm_version="1.0", processing_relation_url="http://foodmart.test/export",
    )


class ProcessingRelationSourceTests(unittest.TestCase):
    def test_fetches_once_and_returns_an_immutable_run_snapshot(self) -> None:
        session = _Session({
            "exported_at": "2026-07-15T09:00:00", "count": 1,
            "relations": [{
                "finished_sku": "F", "finished_name": "finished",
                "raw_sku": "R", "raw_name": "raw", "raw_qty": 2,
                "raw_unit": "kg", "yield_qty": 3, "yield_unit": "个",
                "category_type": "烘焙类",
            }],
        })
        source = ProcessingRelationSource(_settings(), session=session)
        first = source.fetch_once()
        second = source.fetch_once()
        self.assertIs(first, second)
        self.assertEqual(session.calls, 1)
        self.assertEqual(first.source_count, 1)
        changed = first.frame
        changed.loc[0, "raw_qty"] = 999
        self.assertEqual(first.frame.loc[0, "raw_qty"], 2)
        recipes = first.frame
        candidates = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-07-14",
            "from_article_id": "R", "to_article_id": "F",
        }])
        resolution = resolve_relations(
            candidates, relation_snapshot_id=first.snapshot.snapshot_id,
            processing_recipes=recipes,
        )
        actual = pd.DataFrame([
            {"store_id": "A3XV", "business_date": "2026-07-14", "article_id": "R",
             "compose_in_qty": 0, "compose_out_qty": 2},
            {"store_id": "A3XV", "business_date": "2026-07-14", "article_id": "F",
             "compose_in_qty": 3, "compose_out_qty": 0},
        ])
        plan = build_processing_plan(actual, recipes, resolution)
        self.assertEqual(len(plan.trace), 1)

    def test_invalid_count_and_nonpositive_quantities_are_blocked(self) -> None:
        relation = {
            "finished_sku": "F", "finished_name": "finished",
            "raw_sku": "R", "raw_name": "raw", "raw_qty": 0,
            "raw_unit": "kg", "yield_qty": 3, "yield_unit": "个",
            "category_type": "烘焙类",
        }
        with self.assertRaises(ValueError):
            ProcessingRelationSource(
                _settings(), session=_Session({"count": 2, "relations": [relation]})
            ).fetch_once()
        with self.assertRaises(ValueError):
            ProcessingRelationSource(
                _settings(), session=_Session({"count": 1, "relations": [relation]})
            ).fetch_once()

    def test_null_ids_and_nonfinite_quantities_are_blocked(self) -> None:
        base = {
            "finished_sku": "F", "finished_name": "finished",
            "raw_sku": "R", "raw_name": "raw", "raw_qty": 1,
            "raw_unit": "kg", "yield_qty": 3, "yield_unit": "个",
            "category_type": "烘焙类",
        }
        for override in ({"raw_sku": None}, {"raw_qty": float("inf")}):
            with self.subTest(override=override), self.assertRaises(ValueError):
                ProcessingRelationSource(
                    _settings(),
                    session=_Session({"count": 1, "relations": [{**base, **override}]}),
                ).fetch_once()


if __name__ == "__main__":
    unittest.main()
