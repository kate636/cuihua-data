from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
import tempfile
import unittest

import duckdb
import numpy as np
import pandas as pd

from fmetl.calculations.ledger import run_weighted_ledger
from fmetl.contracts.v014 import OUTPUT_CONTRACT, V15_COMPATIBLE_FIELDS
from fmetl.facts.inventory_inputs import normalize_inventory_inputs
from fmetl.outputs.levels_result import ADDITIVE_INPUTS, build_v014_levels_result
from fmetl.outputs.persistence import persist_v014_shadow
from fmetl.facts.formal_events import build_formal_event_legs
from fmetl.facts.processing_inference import infer_processing_postings
from fmetl.relations.registry import (
    V014RelationError,
    build_product_group_candidates,
    freeze_product_group_snapshot,
    resolve_relation_registry,
)
from fmetl.mirror.v014_source import (
    DAILY_SOURCE_KEYS,
    LATEST_SOURCE_KEYS,
    MirrorSourceBundle,
    _write_mirror_frame,
    extract_and_persist_v014_mirror_sources,
    extract_v014_mirror_sources,
    persist_mirror_source_bundle,
    shadow_source_days_between,
    shadow_source_days_ending,
)
from fmetl.mirror.v014_stage import (
    _bom_events,
    _build_day_clear_frame,
    _exclude_bom_backed_explicit_relations,
    _fixed_relation_convert_events,
    _mark_receipt_backed_processing,
    _observed_receipt_relation_pairs,
    _processing_stage,
    build_v014_stage_bundle,
)
from fmetl.mirror.registry import EXTRACTION_CONTRACTS
from fmetl.validation.run_v014 import REQUIRED_SOURCE_NAMES, select_complete_window
from fmetl.validation.run_v014 import run_v014_shadow_week
from fmetl.validation.v014 import validate_v014_ledger


class V014ContractTests(unittest.TestCase):
    def test_frozen_contract_is_123_plus_two(self) -> None:
        self.assertEqual(123, len(V15_COMPATIBLE_FIELDS))
        self.assertEqual(125, len(OUTPUT_CONTRACT))
        self.assertEqual("store_flag", OUTPUT_CONTRACT[0].name)
        self.assertEqual("old_cust_item_per_customer", OUTPUT_CONTRACT[122].name)
        self.assertEqual(["article_group_id", "article_group_name"], [f.name for f in OUTPUT_CONTRACT[-2:]])


class V014MirrorSourceTests(unittest.TestCase):
    class FakeExtractor:
        def extract_day(self, contract, business_day):
            row = {column: None for column in contract.projection}
            if contract.partition_column:
                row[contract.partition_column] = str(business_day)
            if contract.store_column:
                row[contract.store_column] = "A3XV"
            frame = pd.DataFrame([row], columns=contract.projection)
            frame.attrs["source_snapshot_day"] = str(business_day)
            return frame

    def test_explicit_shadow_end_has_one_warmup_and_seven_publish_days(self) -> None:
        self.assertEqual(
            (
                "2026-07-23", "2026-07-24", "2026-07-25", "2026-07-26",
                "2026-07-27", "2026-07-28", "2026-07-29", "2026-07-30",
            ),
            shadow_source_days_ending("2026-07-30"),
        )

    def test_explicit_shadow_range_has_one_warmup_and_all_publish_days(self) -> None:
        self.assertEqual(
            (
                "2026-07-26", "2026-07-27", "2026-07-28", "2026-07-29",
                "2026-07-30", "2026-07-31", "2026-08-01", "2026-08-02",
                "2026-08-03", "2026-08-04",
            ),
            shadow_source_days_between("2026-07-27", "2026-08-04"),
        )

    def test_nonempty_partition_rebuilds_schema_after_empty_frame(self) -> None:
        contract = EXTRACTION_CONTRACTS["inventory_pool"]
        empty = pd.DataFrame(columns=contract.projection)
        populated = pd.DataFrame([{
            column: (
                "2026-07-30" if column in {"inc_day", "inventory_date"}
                else "A3XV" if column == "shop_id"
                else "SKU-1" if column == "sku_code"
                else None
            )
            for column in contract.projection
        }])
        conn = duckdb.connect(":memory:")
        try:
            _write_mirror_frame(conn, "inventory_pool", contract, empty)
            self.assertEqual(
                0,
                conn.execute("SELECT COUNT(*) FROM v014_mirror_inventory_pool").fetchone()[0],
            )
            _write_mirror_frame(conn, "inventory_pool", contract, populated)
            self.assertEqual(
                ("2026-07-30", "SKU-1"),
                conn.execute(
                    "SELECT inc_day, sku_code FROM v014_mirror_inventory_pool"
                ).fetchone(),
            )
        finally:
            conn.close()

    def test_pork_without_sku_label_uses_day_clear_fallback(self) -> None:
        days = ("2026-07-20", "2026-07-21")
        raw_day_clear = pd.DataFrame([{
            "store_id": "A3XV", "inc_day": days[0], "article_id": "PORK",
            "day_clear": 1.0,
        }])
        sales = pd.DataFrame([{
            "store_id": "A3XV", "business_date": days[0], "article_id": "VEG",
            "source_sales_day_clear": 0.0,
        }])
        goods = pd.DataFrame([
            {"article_id": "PORK", "category_level1_description": "猪肉类"},
            {"article_id": "VEG", "category_level1_description": "蔬菜类"},
        ])

        resolved = _build_day_clear_frame(
            store_id="A3XV",
            days=days,
            universe={"PORK", "VEG"},
            raw_day_clear=raw_day_clear,
            sales=sales,
            goods=goods,
        ).set_index(["business_date", "article_id"])["day_clear"]

        self.assertEqual("1", resolved.loc[(days[0], "PORK")])
        self.assertEqual("0", resolved.loc[(days[1], "PORK")])
        self.assertEqual("0", resolved.loc[(days[0], "VEG")])
        self.assertEqual("1", resolved.loc[(days[1], "VEG")])

        membership = _build_day_clear_frame(
            store_id="A3XV",
            days=(days[0],),
            universe={"VEG"},
            raw_day_clear=pd.DataFrame([{
                "store_id": "A3XV", "inc_day": days[0], "article_id": "VEG",
            }]),
            sales=pd.DataFrame(),
            goods=goods,
        ).iloc[0]
        self.assertEqual("0", membership.day_clear)

    def test_hive_mirror_bundle_is_complete_and_persisted(self) -> None:
        days = tuple((date(2026, 7, 20) + timedelta(days=index)).isoformat() for index in range(2))
        bundle = extract_v014_mirror_sources(
            store_id="A3XV", days=days, extractor=self.FakeExtractor()
        )
        self.assertEqual(set(DAILY_SOURCE_KEYS) | set(LATEST_SOURCE_KEYS), set(bundle.frames))
        self.assertEqual(len(DAILY_SOURCE_KEYS) * 2 + len(LATEST_SOURCE_KEYS) * 2, len(bundle.completeness))
        self.assertTrue(bundle.completeness["is_complete"].all())
        sales_manifest = bundle.manifest.loc[bundle.manifest["source_name"].eq("sales")].iloc[0]
        self.assertEqual("HIVE_MIRROR", sales_manifest["source_system"])
        self.assertEqual(
            "hive.dsl.dsl_transaction_non_daily_store_order_details_di",
            sales_manifest["hive_source_tables"],
        )
        group_manifest = bundle.manifest.loc[
            bundle.manifest["source_name"].eq("product_group")
        ].iloc[0]
        self.assertEqual("AUXILIARY_RELATION_EVIDENCE", group_manifest["source_system"])
        self.assertEqual("", group_manifest["hive_source_tables"])
        with tempfile.TemporaryDirectory() as temp_dir:
            path = persist_mirror_source_bundle(Path(temp_dir) / "source.duckdb", bundle)
            conn = duckdb.connect(str(path), read_only=True)
            try:
                names = {row[0] for row in conn.execute(
                    "SELECT table_name FROM information_schema.tables"
                ).fetchall()}
                self.assertIn("v014_mirror_sales", names)
                self.assertIn("v014_mirror_source_manifest", names)
                self.assertEqual(2, conn.execute(
                    "SELECT COUNT(*) FROM v014_mirror_sales"
                ).fetchone()[0])
            finally:
                conn.close()

    def test_resumable_mirror_cache_keeps_completed_source_days(self) -> None:
        days = ("2026-07-23", "2026-07-24")

        class FailsOnSecondSalesDay(self.FakeExtractor):
            def __init__(self) -> None:
                self.calls: list[tuple[str, str]] = []

            def extract_day(self, contract, business_day):
                self.calls.append((contract.name, str(business_day)))
                if (
                    contract is EXTRACTION_CONTRACTS["sales"]
                    and str(business_day) == days[1]
                ):
                    raise TimeoutError("simulated source timeout")
                return super().extract_day(contract, business_day)

        class RecordsCalls(self.FakeExtractor):
            def __init__(self) -> None:
                self.calls: list[tuple[str, str]] = []

            def extract_day(self, contract, business_day):
                self.calls.append((contract.name, str(business_day)))
                return super().extract_day(contract, business_day)

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "source.duckdb"
            first = FailsOnSecondSalesDay()
            with self.assertRaisesRegex(
                RuntimeError, "source=sales, day=2026-07-24"
            ):
                extract_and_persist_v014_mirror_sources(
                    store_id="A3XV", days=days, path=path, extractor=first
                )

            conn = duckdb.connect(str(path), read_only=True)
            try:
                completed = conn.execute(
                    "SELECT business_date FROM v014_mirror_source_completeness "
                    "WHERE source_name='sales' ORDER BY business_date"
                ).fetchall()
                self.assertEqual([(days[0],)], completed)
                self.assertEqual(
                    1, conn.execute("SELECT COUNT(*) FROM v014_mirror_sales").fetchone()[0]
                )
            finally:
                conn.close()

            second = RecordsCalls()
            bundle = extract_and_persist_v014_mirror_sources(
                store_id="A3XV", days=days, path=path, extractor=second
            )
            sales_name = EXTRACTION_CONTRACTS["sales"].name
            self.assertNotIn((sales_name, days[0]), second.calls)
            self.assertIn((sales_name, days[1]), second.calls)
            self.assertEqual(days, bundle.requested_days)
            self.assertEqual(
                len(DAILY_SOURCE_KEYS) * len(days)
                + len(LATEST_SOURCE_KEYS) * len(days),
                len(bundle.completeness),
            )

    def test_hive_mirrors_programmatically_build_internal_stage(self) -> None:
        day = "2026-07-20"
        frames = {
            name: pd.DataFrame(columns=EXTRACTION_CONTRACTS[name].projection)
            for name in (*DAILY_SOURCE_KEYS, *LATEST_SOURCE_KEYS)
        }
        sale_row = {
            "store_id": "A3XV", "business_date": day, "inc_day": day,
            "order_id": "O1", "order_status": "os.completed", "abi_article_id": "S1",
            "day_clear": "1", "qty": 1.0, "qty_spec": 1.0,
            "actual_weight": 1.0, "sales_amt": 20.0, "af19_sales_qty": 0.0,
            "af19_sales_amt": 0.0, "p_lp_sub_amt": 20.0, "discount_amt": 0.0,
            "hour_discount_amt": 0.0, "promotion_amt": 0.0, "online_flag": 0,
            "business_source": "OFFLINE", "jielong_flag": 0, "customer_id": "C1",
            "first_buy_flag": 1, "return_sale_qty": 0.0, "return_sale_amt": 0.0,
        }
        # The same person can visit twice.  v1.5 customer counts are distinct
        # orders, so the second zero-value row still counts as another visit.
        second_order = {**sale_row, "order_id": "O2", "qty": 0.0, "qty_spec": 0.0,
                        "actual_weight": 0.0, "sales_amt": 0.0}
        frames["sales"] = pd.DataFrame([sale_row, second_order]).reindex(
            columns=EXTRACTION_CONTRACTS["sales"].projection
        )
        frames["store_receipt"] = pd.DataFrame([{
            "store_id": "A3XV", "business_date": day, "inc_day": day,
            "day_clear": "1", "article_id": "S1", "article_name": "测试品",
            "sale_article_id": "S1", "sale_article_name": "测试品",
            "sale_article_qty": 10.0, "sale_article_purchase_amt": 100.0,
            "avg_inbound_price": 10.0, "init_stock_qty": 0.0,
            "init_stock_amt": 0.0, "end_stock_qty": 9.0,
            "end_stock_amt": 90.0, "inventory_cost": 10.0,
        }]).reindex(columns=EXTRACTION_CONTRACTS["store_receipt"].projection)
        frames["day_clear"] = pd.DataFrame([{
            "business_date": day, "store_id": "A3XV", "article_id": "S1",
            "day_clear": "1", "inc_day": day,
        }]).reindex(columns=EXTRACTION_CONTRACTS["day_clear"].projection)
        frames["product_group"] = pd.DataFrame([{
            "inc_day": day, "area_name": None, "article_group_id": "G1",
            "article_group_name": "测试集", "article_id": "S1",
            "article_name": "测试品", "spu_name": "测试SPU",
            "category_level1_description": "蔬菜类",
        }]).reindex(columns=EXTRACTION_CONTRACTS["product_group"].projection)
        frames["goods"] = pd.DataFrame([{
            "inc_day": day, "article_id": "S1", "article_name": "测试品",
            "sale_unit": "千克", "unit_weight": 1.0, "matnr": "M1",
            "matnr_unit": "KG", "order_unit": "KG", "atob_value": 1.0,
            "zglfz": 1.0, "zglfm": 1.0, "category_level1_id": "10",
            "category_level1_description": "蔬菜类", "category_level2_id": "101",
            "category_level2_description": "叶菜", "category_level3_id": "10101",
            "category_level3_description": "菜心", "spu_id": "SP1",
            "spu_name": "测试SPU", "blackwhite_pig_id": None,
            "blackwhite_pig_name": None,
        }]).reindex(columns=EXTRACTION_CONTRACTS["goods"].projection)
        frames["store_profile"] = pd.DataFrame([{
            "inc_day": day, "sp_store_id": "A3XV", "sp_store_name": "花家",
            "store_flag_name": "翠花店", "manage_area_name": "广州",
            "sap_area_name": "广州", "city_description": "广州",
        }]).reindex(columns=EXTRACTION_CONTRACTS["store_profile"].projection)
        frames["order_saleability"] = pd.DataFrame([{
            "store_id": "A3XV", "inc_day": day, "effective_date": day,
            "article_id": "S1", "article_name": "测试品", "is_order": 1,
            "saleable": 1, "status": 1,
        }]).reindex(columns=EXTRACTION_CONTRACTS["order_saleability"].projection)
        frames["article_sale"] = pd.DataFrame([{
            "store_id": "A3XV", "business_date": day, "inc_day": day,
            "article_id": "S1", "business_flag": "ALL", "sale_qty": 1.0,
            "sale_amt": 20.0, "sale_weight": 1.0, "sale_piece_qty": 1.0,
            "bf19_sale_qty": 1.0, "bf19_sale_amt": 20.0,
            "bf19_sale_weight": 1.0, "bf19_sale_piece_qty": 1.0,
            "cust_num": 1.0, "bf19_cust_num": 1.0,
            "online_cust_num": 0.0, "offline_cust_num": 1.0,
            "online_sale_qty": 0.0, "offline_sale_qty": 1.0,
            "online_sale_amt": 0.0, "offline_sale_amt": 20.0,
            "original_price_sale_amt": 20.0, "discount_amt": 0.0,
            "hour_discount_amt": 0.0, "promotion_discount_amt": 0.0,
            "return_sale_qty": 0.0, "return_sale_amt": 0.0,
        }]).reindex(columns=EXTRACTION_CONTRACTS["article_sale"].projection)
        frames["supply_chain"] = pd.DataFrame([{
            "store_id": "A3XV", "business_date": day, "inc_day": day,
            "article_id": "S1", "order_qty_payean": 10.0,
            "total_outstock_qty": 10.0, "out_stock_pay_amt": 100.0,
            "out_stock_pay_amt_notax": 95.0,
            "out_stock_amt_cb": 90.0, "return_stock_qty": 0.0,
            "out_stock_amt_cb_notax": 85.0,
            "return_stock_pay_amt": 0.0, "return_stock_amt_cb": 0.0,
            "return_stock_pay_amt_notax": -9.5,
            "return_stock_amt_cb_notax": -8.5,
            "scm_promotion_amt_total": 0.0, "adjustment_amt": 0.0,
        }]).reindex(columns=EXTRACTION_CONTRACTS["supply_chain"].projection)
        frames["order_offline"] = pd.DataFrame([{
            "business_date": day, "inc_day": day, "store_id": "A3XV",
            "order_id": "O1", "serial_id": "1", "root_order_id": "O1",
            "afs_order_id": None, "je_order_id": None, "rje_order_id": None,
            "pay_at": f"{day} 18:00:00", "abi_article_id": "S1",
            "order_status": "os.completed", "jielong_flag": 0,
            "sales_amt": 20.0, "qty": 1.0,
            "thirdparty_user_identity": "C1",
        }]).reindex(columns=EXTRACTION_CONTRACTS["order_offline"].projection)
        completeness = pd.DataFrame([
            {"store_id": "A3XV", "business_date": day, "source_name": name,
             "is_complete": True, "row_count": len(frame),
             "source_table": EXTRACTION_CONTRACTS[name].full_name}
            for name, frame in frames.items()
        ])
        mirrors = MirrorSourceBundle(
            "A3XV", (day,), frames, pd.DataFrame(), completeness
        )
        stage = build_v014_stage_bundle(mirrors)
        self.assertIn("source_manifest", stage.frames)
        activity = stage.frames["activities"].iloc[0]
        self.assertEqual(10.0, activity.store_receive_qty)
        self.assertEqual(100.0, activity.store_receive_amt)
        self.assertEqual("HIVE_PURCHASE_DI_BOOTSTRAP_ZERO", stage.frames["openings"].iloc[0].opening_source)
        metric = stage.frames["reporting_metrics"].iloc[0]
        self.assertEqual("S1", metric.sku_id)
        self.assertEqual(1.0, metric.active_sku_flag)
        self.assertEqual(1.0, metric.new_cust_num)
        customer = stage.frames["customer_events"].iloc[0]
        self.assertEqual("O1", customer.order_id)
        self.assertTrue(bool(customer.is_before_19))
        self.assertEqual("food mart", metric.store_no)
        self.assertEqual("翠花店", metric.store_flag)
        self.assertEqual(0.0, metric.initial_inventory_amount)
        self.assertEqual(90.0, metric.ending_inventory_amount)
        self.assertEqual(0.0, metric.init_stock_qty)
        self.assertEqual(9.0, metric.end_stock_qty)
        self.assertTrue(bool(metric.has_observed_inventory))
        self.assertEqual(10.0, metric.supply_chain_profit_amount)
        self.assertEqual(10.0, metric.purchase_weight_amount)


class V014RelationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.groups_raw = pd.DataFrame([
            {"inc_day": "2026-07-20", "area_name": None, "article_group_id": "G1", "article_group_name": "组1", "article_id": "A"},
            {"inc_day": "2026-07-20", "area_name": None, "article_group_id": "G1", "article_group_name": "组1", "article_id": "B"},
            {"inc_day": "2026-07-20", "area_name": "广州", "article_group_id": "BAD", "article_group_name": "区域", "article_id": "A"},
        ])
        self.candidates = pd.DataFrame([
            {"store_id": "A3XV", "business_date": "2026-07-20", "source_article_id": "A", "target_article_id": "B"},
        ])

    def test_only_posted_cross_code_receipt_marks_processing_priority(self) -> None:
        processing = pd.DataFrame([
            {
                "store_id": "A3XV", "business_date": "2026-07-20",
                "raw_article_id": "A", "finished_article_id": target,
                "external_finished_receipt_qty": 0.0,
                "external_finished_receipt_amt": 0.0,
            }
            for target in ("B", "C")
        ])
        postings = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-07-20",
            "source_parent_article_id": "A", "article_id": "B",
            "receive_qty": 3.0, "receive_amt": 60.0,
        }])

        marked = _mark_receipt_backed_processing(processing, postings).set_index(
            "finished_article_id"
        )

        self.assertEqual(3.0, marked.loc["B", "external_finished_receipt_qty"])
        self.assertEqual(60.0, marked.loc["B", "external_finished_receipt_amt"])
        self.assertEqual(0.0, marked.loc["C", "external_finished_receipt_qty"])

    def test_dense_zero_receipt_snapshot_does_not_create_relation_candidate(self) -> None:
        purchase = pd.DataFrame([
            {
                "store_id": "A3XV", "business_date": "2026-07-20",
                "article_id": "A", "sale_article_id": "B",
                "sale_article_qty": 0.0, "sale_article_purchase_amt": 0.0,
            },
            {
                "store_id": "A3XV", "business_date": "2026-07-20",
                "article_id": "C", "sale_article_id": "D",
                "sale_article_qty": -2.0, "sale_article_purchase_amt": -20.0,
            },
        ])
        observed = _observed_receipt_relation_pairs(purchase)
        self.assertEqual([("C", "D")], list(observed[["article_id", "sale_article_id"]].itertuples(index=False, name=None)))

    def test_fixed_relation_directly_converts_same_code_receipt(self) -> None:
        purchase = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-07-20",
            "article_id": "A", "sale_article_id": "A",
            "sale_article_qty": 6.0, "sale_article_purchase_amt": 60.0,
        }])
        explicit = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-07-20",
            "source_article_id": "A", "target_article_id": "B",
            "fixed_rule": True, "approved": True, "convert_rate": 0.5,
        }])

        event = _fixed_relation_convert_events(purchase, explicit).iloc[0]

        self.assertEqual("A", event.source_article_id)
        self.assertEqual("B", event.target_article_id)
        self.assertEqual(6.0, event.source_qty)
        self.assertEqual(3.0, event.target_qty)
        self.assertEqual(
            "ARTICLE_CONVERT_FIXED_RELATION_PLUS_A_RECEIPT",
            event.quantity_source,
        )

    def test_fixed_relation_skips_ambiguous_or_already_allocated_source(self) -> None:
        purchase = pd.DataFrame([
            {
                "store_id": "A3XV", "business_date": "2026-07-20",
                "article_id": "A", "sale_article_id": "A",
                "sale_article_qty": 6.0, "sale_article_purchase_amt": 60.0,
            },
            {
                "store_id": "A3XV", "business_date": "2026-07-20",
                "article_id": "A", "sale_article_id": "B",
                "sale_article_qty": 3.0, "sale_article_purchase_amt": 60.0,
            },
        ])
        explicit = pd.DataFrame([
            {
                "store_id": "A3XV", "business_date": "2026-07-20",
                "source_article_id": "A", "target_article_id": target,
                "fixed_rule": True, "approved": True, "convert_rate": rate,
            }
            for target, rate in (("B", 0.5), ("C", 2.0))
        ])

        self.assertTrue(_fixed_relation_convert_events(purchase, explicit).empty)

    def test_fixed_relation_rejects_conflicting_rates_for_same_pair(self) -> None:
        purchase = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-07-20",
            "article_id": "A", "sale_article_id": "A",
            "sale_article_qty": 6.0, "sale_article_purchase_amt": 60.0,
        }])
        explicit = pd.DataFrame([
            {
                "store_id": "A3XV", "business_date": "2026-07-20",
                "source_article_id": "A", "target_article_id": "B",
                "fixed_rule": True, "approved": True, "convert_rate": rate,
            }
            for rate in (0.5, 0.25)
        ])

        with self.assertRaisesRegex(ValueError, "conflicting rates"):
            _fixed_relation_convert_events(purchase, explicit)

    def test_group_identity_is_candidate_not_flow(self) -> None:
        groups = freeze_product_group_snapshot(self.groups_raw)
        pairs = build_product_group_candidates(self.candidates, groups)
        registry, quarantine = resolve_relation_registry(
            self.candidates, product_group_pairs=pairs, relation_version="r1"
        )
        self.assertEqual("PRODUCT_GROUP_CANDIDATE", registry.iloc[0]["relation_type"])
        self.assertFalse(bool(registry.iloc[0]["formal_flow_allowed"]))
        self.assertEqual(
            "PRODUCT_GROUP_CONVERSION_EVIDENCE_MISSING",
            quarantine.iloc[0]["reason_code"],
        )

    def test_allocated_receipt_uses_observed_ratio_and_weight_only_as_audit(self) -> None:
        raw = self.groups_raw.copy()
        raw["unit_weight"] = [1.0, 0.25, 1.0]
        raw["sale_unit"] = ["袋", "份", "袋"]
        candidates = self.candidates.assign(
            external_receipt_source_qty=2.0,
            external_receipt_target_qty=8.0,
            external_receipt_target_amt=80.0,
            external_receipt_quantity_rate=4.0,
        )
        pairs = build_product_group_candidates(
            candidates, freeze_product_group_snapshot(raw)
        )
        registry, quarantine = resolve_relation_registry(
            candidates, product_group_pairs=pairs, relation_version="r1"
        )
        self.assertEqual(4.0, pairs.iloc[0]["product_weight_quantity_rate"])
        self.assertEqual("ALLOCATED_RECEIPT", registry.iloc[0]["relation_type"])
        self.assertEqual(4.0, registry.iloc[0]["quantity_rate"])
        self.assertEqual("EXTERNAL_DIRECT", registry.iloc[0]["posting_mode"])
        self.assertFalse(bool(registry.iloc[0]["formal_flow_allowed"]))
        self.assertTrue(quarantine.empty)

    def test_article_convert_confirms_allocated_receipt_without_convert_detail(self) -> None:
        candidates = self.candidates.assign(
            external_receipt_source_qty=2.0,
            external_receipt_target_qty=8.0,
            external_receipt_target_amt=80.0,
            external_receipt_quantity_rate=4.0,
        )
        convert = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-07-20",
            "source_article_id": "A", "target_article_id": "B",
            "effective_from": "2026-07-20", "effective_to": "2026-07-20",
            "actual_event": False, "fixed_rule": True,
            "convert_rate": 4.0, "cost_rate": 1.0, "approved": True,
        }])
        registry, quarantine = resolve_relation_registry(
            candidates, explicit_convert=convert, relation_version="r1"
        )
        row = registry.iloc[0]
        self.assertEqual("ALLOCATED_RECEIPT", row["relation_type"])
        self.assertEqual(4.0, row["quantity_rate"])
        self.assertEqual("EXTERNAL_DIRECT", row["posting_mode"])
        self.assertFalse(bool(row["formal_flow_allowed"]))
        self.assertIn("article_convert_ratio", row["evidence_source"])
        self.assertTrue(quarantine.empty)

    def test_formal_type_conflict_is_quarantined(self) -> None:
        bom = pd.DataFrame([{
            "parent_article_id": "A", "sub_article_id": "B", "effective_from": "2026-07-01",
            "effective_to": None, "category_level1_description": "猪肉类", "dressing_rate": 2,
            "cost_rate": 1, "approved": True,
        }])
        convert = pd.DataFrame([{
            "source_article_id": "A", "target_article_id": "B", "effective_from": "2026-07-01",
            "effective_to": None, "actual_event": True, "convert_rate": 2, "approved": True,
        }])
        registry, quarantine = resolve_relation_registry(
            self.candidates, bom=bom, explicit_convert=convert, relation_version="r1"
        )
        self.assertEqual("CONFLICT", registry.iloc[0]["relation_type"])
        self.assertEqual("MULTIPLE_FORMAL_RELATION_TYPES", quarantine.iloc[0]["reason_code"])

    def test_missing_formal_ratio_is_quarantined(self) -> None:
        convert = pd.DataFrame([{
            "source_article_id": "A", "target_article_id": "B", "effective_from": "2026-07-01",
            "effective_to": None, "actual_event": True, "approved": True,
        }])
        registry, quarantine = resolve_relation_registry(
            self.candidates, explicit_convert=convert, relation_version="r1"
        )
        self.assertFalse(bool(registry.iloc[0]["formal_flow_allowed"]))
        self.assertEqual("RELATION_RATIO_MISSING", quarantine.iloc[0]["reason_code"])

    def test_snapshot_rejects_one_article_in_two_groups(self) -> None:
        bad = pd.concat([
            self.groups_raw,
            pd.DataFrame([{"inc_day": "2026-07-20", "area_name": None, "article_group_id": "G2", "article_group_name": "组2", "article_id": "A"}]),
        ], ignore_index=True)
        with self.assertRaises(V014RelationError):
            freeze_product_group_snapshot(bad)

    def test_official_bom_edge_removes_competing_explicit_semantics(self) -> None:
        explicit = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-07-20",
            "source_article_id": "P", "target_article_id": "C",
            "effective_from": "2026-07-20", "effective_to": "2026-07-20",
            "actual_event": True, "fixed_rule": False, "convert_rate": 2.0,
            "cost_rate": 1.0, "approved": True,
        }])
        bom = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-07-20",
            "parent_article_id": "P", "sub_article_id": "C",
            "approved": True,
        }])
        self.assertTrue(_exclude_bom_backed_explicit_relations(explicit, bom).empty)

    def test_bom_cost_allocation_uses_split_amount_and_zero_gift_rate_fallback(self) -> None:
        receive_sale = pd.DataFrame([
            {
                "store_id": "A3XV", "inc_day": "2026-07-20",
                "article_id": "P", "sale_article_id": "C1",
                "inbound_qty": 10.0, "sale_article_qty": 6.0,
                "spilit_sale_article_amt": 60.0, "sale_recev_rate": 0.6,
            },
            {
                "store_id": "A3XV", "inc_day": "2026-07-20",
                "article_id": "P", "sale_article_id": "C2",
                "inbound_qty": 10.0, "sale_article_qty": 4.0,
                "spilit_sale_article_amt": 40.0, "sale_recev_rate": 0.4,
            },
        ])
        bom = pd.DataFrame([
            {
                "store_id": "A3XV", "business_date": "2026-07-20",
                "parent_article_id": "P", "sub_article_id": child,
                "dressing_rate": rate, "approved": True,
            }
            for child, rate in (("C1", 0.6), ("C2", 0.4))
        ])
        events = _bom_events(receive_sale, bom)
        self.assertAlmostEqual(1.0, events["amount_allocation_ratio"].sum())
        self.assertAlmostEqual(10.0, events["target_common_qty"].sum())
        gift = receive_sale.copy()
        gift.loc[gift["sale_article_id"].eq("C2"), "spilit_sale_article_amt"] = 0.0
        gift_events = _bom_events(gift, bom)
        allocation = gift_events.set_index("target_article_id")["amount_allocation_ratio"]
        self.assertAlmostEqual(0.6, allocation["C1"])
        self.assertAlmostEqual(0.4, allocation["C2"])


class V014ProcessingTests(unittest.TestCase):
    def _inputs(self):
        finished = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-07-20", "article_id": "F",
            "net_sale_qty": 3, "known_lost_qty": 1,
        }])
        recipes = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-07-20",
            "relation_id": "P1", "raw_article_id": "R", "finished_article_id": "F",
            "raw_qty": 2, "yield_qty": 1, "effective_from": "2026-07-01",
            "effective_to": None, "approved": True,
        }])
        registry = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-07-20", "source_article_id": "R",
            "target_article_id": "F", "relation_type": "PROCESSING", "status": "ACTIVE",
        }])
        return finished, recipes, registry

    def test_nonoverlapping_recipe_versions_are_allowed(self) -> None:
        raw = pd.DataFrame([
            {
                "relation_id": "OLD", "raw_article_id": "R",
                "finished_article_id": "F", "raw_qty": 1, "yield_qty": 1,
                "effective_from": "2026-07-01", "effective_to": "2026-07-19",
                "approved": True,
            },
            {
                "relation_id": "NEW", "raw_article_id": "R",
                "finished_article_id": "F", "raw_qty": 2, "yield_qty": 1,
                "effective_from": "2026-07-20", "effective_to": None,
                "approved": True,
            },
        ])

        staged, _ = _processing_stage(
            raw, store_id="A3XV", days=("2026-07-19", "2026-07-20")
        )

        active = staged.loc[staged["approved"]]
        self.assertEqual(["OLD", "NEW"], active.sort_values("business_date")["relation_id"].tolist())

    def test_one_undated_recipe_does_not_disable_other_dated_recipes(self) -> None:
        raw = pd.DataFrame([
            {
                "relation_id": "DATED", "raw_article_id": "R1",
                "finished_article_id": "F1", "raw_qty": 1, "yield_qty": 1,
                "effective_from": "2026-07-01", "effective_to": None,
                "approved": True,
            },
            {
                "relation_id": "UNDATED", "raw_article_id": "R2",
                "finished_article_id": "F2", "raw_qty": 1, "yield_qty": 1,
                "effective_from": None, "effective_to": None,
                "approved": True,
            },
        ])

        staged, quarantined = _processing_stage(
            raw, store_id="A3XV", days=("2026-07-20",)
        )

        active = staged.set_index("relation_id")["approved"]
        self.assertTrue(bool(active["DATED"]))
        self.assertFalse(bool(active["UNDATED"]))
        self.assertEqual(
            "PROCESSING_RELATION_EFFECTIVE_DATE_MISSING",
            quarantined.loc[
                quarantined["article_id"].eq("F2"), "reason_code"
            ].item(),
        )

    def test_overlapping_recipe_versions_are_blocked(self) -> None:
        raw = pd.DataFrame([
            {
                "relation_id": relation_id, "raw_article_id": "R",
                "finished_article_id": "F", "raw_qty": raw_qty,
                "yield_qty": 1, "effective_from": "2026-07-01",
                "effective_to": None, "approved": True,
            }
            for relation_id, raw_qty in (("A", 1), ("B", 2))
        ])

        with self.assertRaisesRegex(ValueError, "overlapping active versions"):
            _processing_stage(raw, store_id="A3XV", days=("2026-07-20",))

    def test_processing_uses_sales_and_ordinary_loss_backflush(self) -> None:
        plan = infer_processing_postings(*self._inputs(), relation_snapshot_id="r1")
        # consumed finished = net sale 3 + ordinary loss 1 = 4;
        # raw out = 4 * 2 / 1 = 8. Counts do not inflate consumed quantity.
        self.assertAlmostEqual(4.0, plan.targets.iloc[0]["target_in_qty"])
        self.assertAlmostEqual(8.0, plan.sources.iloc[0]["source_out_qty"])
        self.assertEqual("FINISHED_USAGE_BACKFLUSH", plan.trace.iloc[0]["quantity_source"])

    def test_processing_uses_only_the_current_day_recipe_snapshot(self) -> None:
        finished, recipes, registry = self._inputs()
        prior = recipes.copy()
        prior["business_date"] = "2026-07-19"
        recipes = pd.concat([prior, recipes], ignore_index=True)
        plan = infer_processing_postings(
            finished, recipes, registry, relation_snapshot_id="r1"
        )
        self.assertEqual(1, len(plan.sources))
        self.assertEqual(1, len(plan.targets))
        self.assertAlmostEqual(8.0, plan.sources.iloc[0]["source_out_qty"])

    def test_multiple_relation_ids_need_explicit_recipe_group_evidence(self) -> None:
        finished, recipes, registry = self._inputs()
        second = recipes.copy()
        second["relation_id"] = "P2"
        second["raw_article_id"] = "R2"
        second["raw_qty"] = 0.5
        recipes = pd.concat([recipes, second], ignore_index=True)
        second_registry = registry.copy()
        second_registry["source_article_id"] = "R2"
        registry = pd.concat([registry, second_registry], ignore_index=True)

        plan = infer_processing_postings(
            finished, recipes, registry, relation_snapshot_id="r1"
        )

        self.assertTrue(plan.targets.empty)
        self.assertTrue(plan.sources.empty)
        self.assertEqual(
            "MULTIPLE_ACTIVE_PROCESSING_RECIPES",
            plan.quarantined.iloc[0]["reason_code"],
        )

    def test_missing_count_uses_sales_and_ordinary_loss_backflush(self) -> None:
        plan = infer_processing_postings(*self._inputs(), relation_snapshot_id="r1")
        self.assertTrue(plan.quarantined.empty)
        self.assertAlmostEqual(4.0, plan.targets.iloc[0]["target_in_qty"])
        self.assertAlmostEqual(8.0, plan.sources.iloc[0]["source_out_qty"])
        self.assertEqual("FINISHED_USAGE_BACKFLUSH", plan.trace.iloc[0]["quantity_source"])

    def test_missing_count_with_no_consumption_creates_no_event(self) -> None:
        finished, recipes, registry = self._inputs()
        finished[["net_sale_qty", "known_lost_qty"]] = 0.0
        plan = infer_processing_postings(
            finished, recipes, registry, relation_snapshot_id="r1"
        )
        self.assertTrue(plan.sources.empty)
        self.assertTrue(plan.targets.empty)
        self.assertTrue(plan.quarantined.empty)

    def test_ssls_raw_loss_has_priority_over_processing_backflush(self) -> None:
        reserved = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-07-20",
            "article_id": "R", "reserved_loss_qty": 1.0,
        }])
        plan = infer_processing_postings(
            *self._inputs(),
            relation_snapshot_id="r1",
            reserved_raw_loss=reserved,
        )
        self.assertTrue(plan.sources.empty)
        self.assertTrue(plan.targets.empty)
        self.assertEqual(
            "PROCESSING_RAW_LOSS_PRIORITY",
            plan.quarantined.iloc[0]["reason_code"],
        )

    def test_external_receipt_has_priority_over_same_processing_pair(self) -> None:
        finished, recipes, registry = self._inputs()
        recipes["external_finished_receipt_qty"] = 4.0
        recipes["external_finished_receipt_amt"] = 80.0

        plan = infer_processing_postings(
            finished, recipes, registry, relation_snapshot_id="r1"
        )

        self.assertTrue(plan.sources.empty)
        self.assertTrue(plan.targets.empty)
        self.assertEqual(
            "PROCESSING_EXTERNAL_RECEIPT_PRIORITY",
            plan.quarantined.iloc[0]["reason_code"],
        )

    def test_raw_loss_priority_hard_gate_rejects_processing_outflow(self) -> None:
        plan = infer_processing_postings(
            *self._inputs(), relation_snapshot_id="r1"
        )
        activities = pd.DataFrame([
            {
                "store_id": "A3XV", "business_date": "2026-07-20",
                "article_id": "R", "day_clear": "1", "gross_sale_qty": 0,
                "sale_return_qty": 0, "net_sale_qty": 0, "net_sale_amt": 0,
                "known_lost_qty": 0, "actual_stock_qty": float("nan"),
                "is_counted": False, "store_receive_qty": 10,
                "store_receive_amt": 100,
            },
            {
                "store_id": "A3XV", "business_date": "2026-07-20",
                "article_id": "F", "day_clear": "1", "gross_sale_qty": 3,
                "sale_return_qty": 0, "net_sale_qty": 3, "net_sale_amt": 60,
                "known_lost_qty": 1, "actual_stock_qty": 2,
                "is_counted": True, "store_receive_qty": 0,
                "store_receive_amt": 0,
            },
        ])
        openings = pd.DataFrame([
            {
                "store_id": "A3XV", "article_id": article,
                "opening_qty": 0, "opening_amt": 0,
                "opening_source": "OBSERVED_ZERO",
                "opening_source_day": "2026-07-20",
            }
            for article in ("R", "F")
        ])
        ledger = run_weighted_ledger(
            activities, openings, plan.sources, plan.targets
        )
        reserved = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-07-20",
            "article_id": "R", "reserved_loss_qty": 1.0,
        }])

        validation = validate_v014_ledger(
            ledger.sku_daily,
            ledger.internal_postings,
            reserved_raw_loss=reserved,
        ).set_index("check_name")

        self.assertFalse(validation.loc["PROCESSING_RAW_LOSS_PRIORITY", "passed"])
        self.assertEqual(
            1,
            validation.loc["PROCESSING_RAW_LOSS_PRIORITY", "failure_count"],
        )

    def test_processing_usage_is_not_gated_by_raw_book_balance(self) -> None:
        plan = infer_processing_postings(
            *self._inputs(), relation_snapshot_id="r1"
        )
        self.assertTrue(plan.quarantined.empty)
        self.assertAlmostEqual(8.0, plan.sources.iloc[0]["source_out_qty"])

    def test_external_receipt_priority_hard_gate_matches_exact_pair(self) -> None:
        sku_daily = pd.DataFrame([
            {
                "store_id": "A3XV", "business_date": "2026-07-20",
                "article_id": article_id, "end_qty": 0.0, "end_amt": 0.0,
                "qty_balance_residual": 0.0, "amount_balance_residual": 0.0,
                "init_stock_qty": 0.0, "init_stock_amt": 0.0,
            }
            for article_id in ("R", "F")
        ])
        postings = pd.DataFrame([
            {
                "store_id": "A3XV", "business_date": "2026-07-20",
                "event_group_id": "PROCESSING|1", "relation_type": "RECIPE_COMPOSE",
                "posting_role": role, "article_id": article_id, "amt": 80.0,
            }
            for role, article_id in (("OUT", "R"), ("IN", "F"))
        ])
        evidence = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-07-20",
            "raw_article_id": "R", "finished_article_id": "X",
            "external_finished_receipt_qty": 4.0,
            "external_finished_receipt_amt": 80.0,
        }])

        different_target = validate_v014_ledger(
            sku_daily, postings, receipt_backed_processing=evidence
        ).set_index("check_name")
        self.assertTrue(
            different_target.loc["PROCESSING_EXTERNAL_RECEIPT_PRIORITY", "passed"]
        )

        evidence["finished_article_id"] = "F"
        same_pair = validate_v014_ledger(
            sku_daily, postings, receipt_backed_processing=evidence
        ).set_index("check_name")
        self.assertFalse(
            same_pair.loc["PROCESSING_EXTERNAL_RECEIPT_PRIORITY", "passed"]
        )

    def test_processing_plan_prices_through_main_daily_ledger(self) -> None:
        plan = infer_processing_postings(*self._inputs(), relation_snapshot_id="r1")
        activities = pd.DataFrame([
            {
                "store_id": "A3XV", "business_date": "2026-07-20", "article_id": "R",
                "day_clear": "1", "gross_sale_qty": 0, "sale_return_qty": 0,
                "net_sale_qty": 0, "net_sale_amt": 0, "known_lost_qty": 0,
                "actual_stock_qty": float("nan"), "is_counted": False,
                "store_receive_qty": 10, "store_receive_amt": 100,
            },
            {
                "store_id": "A3XV", "business_date": "2026-07-20", "article_id": "F",
                "day_clear": "1", "gross_sale_qty": 3, "sale_return_qty": 0,
                "net_sale_qty": 3, "net_sale_amt": 60, "known_lost_qty": 1,
                "actual_stock_qty": 2, "is_counted": True,
                "store_receive_qty": 0, "store_receive_amt": 0,
            },
        ])
        openings = pd.DataFrame([
            {
                "store_id": "A3XV", "article_id": article, "opening_qty": 0,
                "opening_amt": 0, "opening_source": "OBSERVED_ZERO",
                "opening_source_day": "2026-07-20",
            }
            for article in ("R", "F")
        ])
        ledger = run_weighted_ledger(activities, openings, plan.sources, plan.targets)
        state = ledger.sku_daily.set_index("article_id")
        self.assertAlmostEqual(0.0, state.loc["R", "accounting_profit"])
        # The count remains independent evidence. Usage backflush posts only
        # the four consumed finished units, so the counted two-unit remainder
        # is an inventory gain instead of inferred production.
        self.assertAlmostEqual(20.0, state.loc["F", "accounting_profit"])
        posted = ledger.internal_postings.groupby("posting_role")["amt"].sum()
        self.assertAlmostEqual(80.0, posted["OUT"])
        self.assertAlmostEqual(80.0, posted["IN"])
        validation = validate_v014_ledger(ledger.sku_daily, ledger.internal_postings)
        self.assertTrue(validation["passed"].all())


class V014FormalPostingTests(unittest.TestCase):
    def _registry(self) -> pd.DataFrame:
        return pd.DataFrame([
            {
                "store_id": "A3XV", "business_date": "2026-07-20",
                "source_article_id": "P", "target_article_id": child,
                "relation_type": "BOM", "relation_version": "r1", "status": "ACTIVE",
                "formal_flow_allowed": True,
            }
            for child in ("C1", "C2")
        ])

    def test_bom_event_conserves_public_quantity(self) -> None:
        events = pd.DataFrame([
            {
                "store_id": "A3XV", "business_date": "2026-07-20", "event_group_id": "E1",
                "source_article_id": "P",
                "target_article_id": "C1", "source_qty": 1, "target_qty": 3,
                "source_common_qty": 1, "target_common_qty": 0.6,
                "amount_allocation_ratio": 0.6, "quantity_source": "RECEIPT_EVENT",
            },
            {
                "store_id": "A3XV", "business_date": "2026-07-20", "event_group_id": "E1",
                "source_article_id": "P",
                "target_article_id": "C2", "source_qty": 1, "target_qty": 2,
                "source_common_qty": 1, "target_common_qty": 0.4,
                "amount_allocation_ratio": 0.4, "quantity_source": "RECEIPT_EVENT",
            },
        ])
        plan = build_formal_event_legs(events, self._registry())
        self.assertTrue(plan.quarantined.empty)
        self.assertEqual("DISASSEMBLY_BOM", plan.sources.iloc[0]["relation_type"])
        self.assertEqual(1, len(plan.sources))
        self.assertEqual(2, len(plan.targets))

    def test_unbalanced_public_quantity_quarantines_whole_event(self) -> None:
        events = pd.DataFrame([{
            "store_id": "A3XV", "business_date": "2026-07-20", "event_group_id": "E2",
            "source_article_id": "P", "target_article_id": "C1",
            "source_qty": 1, "target_qty": 3, "source_common_qty": 1,
            "target_common_qty": 0.6, "amount_allocation_ratio": 1,
            "quantity_source": "RECEIPT_EVENT",
        }])
        plan = build_formal_event_legs(events, self._registry())
        self.assertTrue(plan.sources.empty)
        self.assertIn("EVENT_COMMON_QUANTITY_NOT_CONSERVED", plan.quarantined.iloc[0]["reason_code"])


class V014InventoryTests(unittest.TestCase):
    def test_negative_and_double_count_are_audited(self) -> None:
        counts = pd.DataFrame([
            {"store_id": "A3XV", "business_date": "2026-07-20", "article_id": "A", "actual_stock_qty": -1, "previous_stock_qty": 1, "count_group_id": "H", "code_role": "SALE"},
            {"store_id": "A3XV", "business_date": "2026-07-20", "article_id": "R", "actual_stock_qty": 5, "previous_stock_qty": 5, "count_group_id": "G", "code_role": "RECEIPT"},
            {"store_id": "A3XV", "business_date": "2026-07-20", "article_id": "B", "actual_stock_qty": 2, "previous_stock_qty": 2, "count_group_id": "G", "code_role": "SALE"},
        ])
        result = normalize_inventory_inputs(counts)
        self.assertTrue(pd.isna(result.normalized.iloc[0]["actual_stock_qty"]))
        self.assertTrue(pd.isna(result.normalized.iloc[1]["actual_stock_qty"]))
        self.assertEqual(2.0, result.normalized.iloc[2]["actual_stock_qty"])
        reasons = set(result.quarantined["reason_code"])
        self.assertIn("NEGATIVE_COUNT_INPUT", reasons)
        self.assertIn("RECEIPT_CODE_COUNT_IGNORED", reasons)
        normalized = result.normalized.set_index("article_id")
        self.assertFalse(bool(normalized.loc["A", "is_counted"]))
        self.assertFalse(bool(normalized.loc["R", "is_counted"]))
        self.assertTrue(bool(normalized.loc["B", "is_counted"]))


class V014WindowTests(unittest.TestCase):
    def test_explicit_window_keeps_requested_start_and_end(self) -> None:
        rows = []
        for offset in range(10):
            day = (date(2026, 7, 26) + timedelta(days=offset)).isoformat()
            for source in REQUIRED_SOURCE_NAMES:
                rows.append({
                    "store_id": "A3XV", "business_date": day,
                    "source_name": source, "is_complete": True,
                })
        window = select_complete_window(
            pd.DataFrame(rows), store_id="A3XV",
            start="2026-07-27", end="2026-08-04",
        )
        self.assertEqual("2026-07-27", window.start)
        self.assertEqual("2026-08-04", window.end)
        self.assertEqual(9, len(window.days))

    def test_auto_window_requires_all_sources_for_all_seven_days(self) -> None:
        end = date(2026, 7, 20)
        rows = []
        for offset in range(9):
            day = (end - timedelta(days=offset)).isoformat()
            for source in REQUIRED_SOURCE_NAMES:
                rows.append({"store_id": "A3XV", "business_date": day, "source_name": source, "is_complete": True})
        # newest day is incomplete; the selected week must end one day earlier.
        rows = [row for row in rows if not (row["business_date"] == "2026-07-20" and row["source_name"] == "sales")]
        window = select_complete_window(pd.DataFrame(rows), store_id="A3XV", end="2026-07-20")
        self.assertEqual("2026-07-19", window.end)
        self.assertEqual("2026-07-13", window.start)


class V014OutputTests(unittest.TestCase):
    def _sku_row(self) -> dict[str, object]:
        row: dict[str, object] = {column: 0.0 for column in ADDITIVE_INPUTS}
        row.update({
            "store_flag": "翠花店", "store_no": "A3XV", "business_date": "2026-07-20",
            "store_name": "花家", "sku_id": "S1", "sku_name": "商品1",
            "category_level1_description": "蔬菜类", "category_level2_description": "叶菜",
            "category_level3_description": "菜心", "spu_id": "P1", "spu_name": "菜心SPU",
            "day_clear": "1", "article_group_id": "G1", "article_group_name": "菜心组",
            "is_processing_raw": False, "is_reportable": True,
            "accounting_profit": 30, "accounting_full_profit": 35,
            "supply_chain_profit_amount": 5, "total_sale_amount": 100, "total_sale_qty": 10,
            "sales_weight": 5, "lp_sale_amt": 120, "active_sku_flag": 1, "stock_sku_flag": 1,
            "total_customer_count": 4, "online_customer_count": 1, "offline_customer_count": 3,
            "online_sale_amt": 25, "offline_sale_amt": 75, "online_sale_qty": 2,
            "offline_sale_qty": 8, "loss_rate_denominator": 50,
        })
        return row

    def _events(self, rows: list[dict[str, object]]) -> pd.DataFrame:
        events = []
        for index, row in enumerate(rows, start=1):
            events.append({
                "store_flag": row["store_flag"], "store_no": row["store_no"],
                "business_date": row["business_date"], "store_name": row["store_name"],
                "sku_id": row["sku_id"], "sku_name": row["sku_name"],
                "category_level1_description": row["category_level1_description"],
                "category_level2_description": row["category_level2_description"],
                "category_level3_description": row["category_level3_description"],
                "spu_id": row["spu_id"], "spu_name": row["spu_name"],
                "day_clear": row["day_clear"],
                "article_group_id": row.get("article_group_id", ""),
                "article_group_name": row.get("article_group_name", ""),
                "order_id": f"O{index}", "is_before_19": True,
                "is_online": index % 2 == 0, "is_offline": index % 2 == 1,
                "is_jielong": False, "is_jsd": index % 2 == 0,
                "is_new": True, "is_old": False,
            })
        return pd.DataFrame(events)

    def test_result_has_seven_levels_and_exact_contract(self) -> None:
        row = self._sku_row()
        result = build_v014_levels_result(
            pd.DataFrame([row]), self._events([row])
        )
        self.assertEqual([field.name for field in OUTPUT_CONTRACT], list(result.columns))
        self.assertEqual({"门店", "大分类", "中分类", "小分类", "spu", "sku", "商品集"}, set(result["level_description"]))
        sku = result.loc[result["level_description"].eq("sku")].iloc[0]
        self.assertAlmostEqual(0.3, float(sku["store_profit_rate"]))
        self.assertEqual("G1", sku["article_group_id"])

    def test_near_zero_aggregated_denominator_does_not_create_unbounded_rate(self) -> None:
        row = self._sku_row()
        row["loss_amount"] = 10.0
        row["loss_rate_denominator"] = 1e-12
        result = build_v014_levels_result(
            pd.DataFrame([row]), self._events([row])
        )
        self.assertTrue(result["loss_rate"].eq(0.0).all())

    def test_unmapped_product_group_uses_blank_contract_fields(self) -> None:
        source = pd.DataFrame([self._sku_row()])
        source["article_group_id"] = None
        source["article_group_name"] = None
        source["spu_name"] = None
        events = self._events(source.to_dict("records"))
        events["article_group_id"] = ""
        events["article_group_name"] = ""
        result = build_v014_levels_result(source, events)
        self.assertFalse(result.isna().any().any())
        self.assertNotIn("商品集", set(result["level_description"]))
        sku = result.loc[
            result["level_description"].eq("sku") & result["day_clear"].eq("1")
        ].iloc[0]
        self.assertEqual("", sku["article_group_id"])
        self.assertEqual("", sku["article_group_name"])

    def test_processing_raw_excluded_from_sku_counts(self) -> None:
        row = self._sku_row()
        row["is_processing_raw"] = True
        result = build_v014_levels_result(
            pd.DataFrame([row]), self._events([row])
        )
        sku = result.loc[result["level_description"].eq("sku")].iloc[0]
        self.assertEqual(0, float(sku["active_sku_count"]))
        self.assertEqual(100, float(sku["total_sale_amount"]))

    def test_special_wastage_is_display_only_and_ssls_is_store_neutral(self) -> None:
        source = self._sku_row()
        source.update({
            "sku_id": "SOURCE", "sku_name": "生熟联动来源",
            "category_level1_description": "猪肉类",
            "accounting_profit": 40.0, "accounting_full_profit": 45.0,
            "ccj_amt": 10.0, "ssls_amt": 3.0,
        })
        cooked = self._sku_row()
        cooked.update({
            "sku_id": "COOKED", "sku_name": "熟食成品",
            "category_level1_description": "熟食类",
            "accounting_profit": 20.0, "accounting_full_profit": 25.0,
            "ccj_amt": 0.0, "ssls_amt": 0.0,
        })
        result = build_v014_levels_result(
            pd.DataFrame([source, cooked]), self._events([source, cooked])
        )
        store = result.loc[
            result["level_description"].eq("门店") & result["day_clear"].eq("2")
        ].iloc[0]
        pork = result.loc[
            result["level_description"].eq("大分类")
            & result["category_level1_description"].eq("猪肉类")
            & result["day_clear"].eq("2")
        ].iloc[0]
        cooked_level = result.loc[
            result["level_description"].eq("大分类")
            & result["category_level1_description"].eq("熟食类")
            & result["day_clear"].eq("2")
        ].iloc[0]
        source_sku = result.loc[
            result["level_description"].eq("sku")
            & result["sku_id"].eq("SOURCE")
            & result["day_clear"].eq("2")
        ].iloc[0]
        cooked_sku = result.loc[
            result["level_description"].eq("sku")
            & result["sku_id"].eq("COOKED")
            & result["day_clear"].eq("2")
        ].iloc[0]
        self.assertEqual(70.0, float(store["store_profit_amount"]))
        self.assertEqual(53.0, float(pork["store_profit_amount"]))
        self.assertEqual(17.0, float(cooked_level["store_profit_amount"]))
        self.assertEqual(50.0, float(source_sku["store_profit_amount"]))
        self.assertEqual(20.0, float(cooked_sku["store_profit_amount"]))
        for level in ("大分类", "spu", "sku"):
            level_total = result.loc[
                result["level_description"].eq(level)
                & result["day_clear"].eq("2"),
                "store_profit_amount",
            ].sum()
            self.assertEqual(float(store["store_profit_amount"]), float(level_total))

    def test_nonreportable_material_is_excluded_from_public_output(self) -> None:
        reportable = self._sku_row()
        material = self._sku_row()
        material.update({
            "sku_id": "MATERIAL", "sku_name": "物料",
            "is_reportable": False, "total_sale_amount": 999.0,
        })
        result = build_v014_levels_result(
            pd.DataFrame([reportable, material]), self._events([reportable])
        )
        store = result.loc[
            result["level_description"].eq("门店") & result["day_clear"].eq("2")
        ].iloc[0]
        self.assertEqual(100.0, float(store["total_sale_amount"]))
        self.assertNotIn("MATERIAL", set(result["sku_id"]))

    def test_persistence_writes_only_v014_tables(self) -> None:
        row = self._sku_row()
        result = build_v014_levels_result(
            pd.DataFrame([row]), self._events([row])
        )
        empty = pd.DataFrame({"placeholder": pd.Series(dtype=str)})
        validation = pd.DataFrame([{"check_name": "x", "passed": True, "failure_count": 0, "detail": "ok"}])
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "shadow.duckdb"
            persist_v014_shadow(
                path, levels_result=result, relation_registry=empty,
                relation_resolution=empty, internal_posting=empty, sku_daily=empty,
                quarantine=empty, run_manifest=empty, validation_result=validation,
            )
            conn = duckdb.connect(str(path), read_only=True)
            try:
                tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
                self.assertIn("t_v014_levels_result", tables)
                self.assertNotIn("t_fm_levels_result", tables)
                schema = conn.execute("DESCRIBE t_v014_levels_result").fetchall()
                self.assertEqual(125, len(schema))
                self.assertEqual("VARCHAR", schema[0][1])
                self.assertEqual("DECIMAL(18,4)", schema[14][1])
            finally:
                conn.close()


class V014RunnerIntegrationTests(unittest.TestCase):
    @staticmethod
    def _create_empty(conn: duckdb.DuckDBPyConnection, name: str, ddl: str) -> None:
        conn.execute(f'CREATE TABLE "{name}" ({ddl})')

    def test_one_sku_seven_day_stage_runs_to_typed_output(self) -> None:
        days = [
            (date(2026, 7, 13) + timedelta(days=offset)).isoformat()
            for offset in range(8)
        ]
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "stage.duckdb"
            output = Path(tmp) / "shadow.duckdb"
            conn = duckdb.connect(str(source))
            try:
                source_manifest = pd.DataFrame([{
                    "source_name": "sales",
                    "source_table": EXTRACTION_CONTRACTS["sales"].full_name,
                    "requested_start": days[0], "requested_end": days[-1],
                    "source_partition": f"{days[0]}..{days[-1]}",
                    "row_count": 7, "checksum": "synthetic",
                    "authority": "observation", "source_system": "HIVE_MIRROR",
                    "hive_source_tables": (
                        "hive.dsl.dsl_transaction_non_daily_store_order_details_di"
                    ),
                }])
                conn.execute(
                    "CREATE TABLE v014_stage_source_manifest AS SELECT * FROM source_manifest"
                )
                completeness = pd.DataFrame([
                    {
                        "store_id": "A3XV", "business_date": day,
                        "source_name": source_name, "is_complete": True,
                    }
                    for day in days for source_name in REQUIRED_SOURCE_NAMES
                ])
                conn.execute("CREATE TABLE v014_stage_source_completeness AS SELECT * FROM completeness")
                groups = pd.DataFrame([
                    {
                        "inc_day": day, "area_name": None, "article_group_id": "G1",
                        "article_group_name": "测试商品集", "article_id": "S1",
                    }
                    for day in days
                ])
                conn.execute("CREATE TABLE v014_stage_product_group AS SELECT * FROM groups")
                pairs = pd.DataFrame([
                    {
                        "store_id": "A3XV", "business_date": day,
                        "source_article_id": "S1", "target_article_id": "S1",
                    }
                    for day in days
                ])
                conn.execute("CREATE TABLE v014_stage_relation_candidates AS SELECT * FROM pairs")
                self._create_empty(
                    conn, "v014_stage_bom",
                    "store_id VARCHAR, business_date VARCHAR, parent_article_id VARCHAR, "
                    "sub_article_id VARCHAR, effective_from VARCHAR, effective_to VARCHAR, "
                    "category_level1_description VARCHAR, dressing_rate DOUBLE, "
                    "cost_rate DOUBLE, approved BOOLEAN",
                )
                self._create_empty(
                    conn, "v014_stage_processing",
                    "store_id VARCHAR, business_date VARCHAR, relation_id VARCHAR, "
                    "raw_article_id VARCHAR, finished_article_id VARCHAR, raw_qty DOUBLE, "
                    "yield_qty DOUBLE, effective_from VARCHAR, effective_to VARCHAR, approved BOOLEAN, "
                    "relation_source VARCHAR, external_finished_receipt_qty DOUBLE, "
                    "external_finished_receipt_amt DOUBLE",
                )
                self._create_empty(
                    conn, "v014_stage_explicit_convert",
                    "store_id VARCHAR, business_date VARCHAR, source_article_id VARCHAR, "
                    "target_article_id VARCHAR, effective_from VARCHAR, effective_to VARCHAR, "
                    "actual_event BOOLEAN, fixed_rule BOOLEAN, convert_rate DOUBLE, "
                    "cost_rate DOUBLE, approved BOOLEAN",
                )
                activities = pd.DataFrame([
                    {
                        "store_id": "A3XV", "business_date": day, "article_id": "S1",
                        "day_clear": "1", "gross_sale_qty": 1.0, "sale_return_qty": 0.0,
                        "net_sale_qty": 1.0, "net_sale_amt": 20.0,
                        "known_lost_qty": 0.0, "actual_stock_qty": float("nan"),
                        "is_counted": False, "store_receive_qty": 10.0 if index == 0 else 0.0,
                        "store_receive_amt": 100.0 if index == 0 else 0.0,
                        "previous_stock_qty": 0.0, "count_group_id": "",
                        "code_role": "SALE",
                    }
                    for index, day in enumerate(days)
                ])
                conn.execute("CREATE TABLE v014_stage_activities AS SELECT * FROM activities")
                openings = pd.DataFrame([{
                    "store_id": "A3XV", "article_id": "S1", "opening_qty": 0.0,
                    "opening_amt": 0.0, "opening_source": "OBSERVED_ZERO",
                    "opening_source_day": days[0],
                }])
                conn.execute("CREATE TABLE v014_stage_openings AS SELECT * FROM openings")
                self._create_empty(
                    conn, "v014_stage_conversion_events",
                    "store_id VARCHAR, business_date VARCHAR, event_group_id VARCHAR, "
                    "source_article_id VARCHAR, target_article_id VARCHAR, "
                    "source_qty DOUBLE, target_qty DOUBLE, source_common_qty DOUBLE, "
                    "target_common_qty DOUBLE, amount_allocation_ratio DOUBLE, quantity_source VARCHAR",
                )
                self._create_empty(
                    conn, "v014_stage_finished_processing_daily",
                    "store_id VARCHAR, business_date VARCHAR, article_id VARCHAR, "
                    "net_sale_qty DOUBLE, known_lost_qty DOUBLE",
                )
                metric_rows = []
                for index, day in enumerate(days):
                    row = {column: 0.0 for column in ADDITIVE_INPUTS}
                    row.update({
                        "store_id": "A3XV", "business_date": day, "article_id": "S1",
                        "store_flag": "翠花店", "store_no": "A3XV", "store_name": "花家",
                        "sku_id": "S1", "sku_name": "测试商品",
                        "category_level1_description": "蔬菜类",
                        "category_level2_description": "叶菜", "category_level3_description": "菜心",
                        "spu_id": "SP1", "spu_name": "测试SPU", "sale_unit": "千克",
                        "day_clear": "1", "is_processing_raw": False, "is_reportable": True,
                        "has_observed_inventory": True,
                        "supply_chain_profit_amount": 0.0, "stock_sku_flag": 1.0,
                        "active_sku_flag": 1.0,
                        "original_price": 0.0, "dc_original_price": 0.0,
                        "last_sale_hour": 18.0,
                        "init_stock_qty": 0.0 if index == 0 else 10.0 - index,
                        "end_stock_qty": 9.0 - index,
                    })
                    metric_rows.append(row)
                reporting = pd.DataFrame(metric_rows)
                conn.execute("CREATE TABLE v014_stage_reporting_metrics AS SELECT * FROM reporting")
                customer_events = pd.DataFrame([
                    {
                        "store_id": "A3XV", "store_flag": "翠花店", "store_no": "A3XV",
                        "business_date": day, "store_name": "花家",
                        "sku_id": "S1", "sku_name": "测试商品",
                        "category_level1_description": "蔬菜类",
                        "category_level2_description": "叶菜",
                        "category_level3_description": "菜心",
                        "spu_id": "SP1", "spu_name": "测试SPU",
                        "day_clear": "1", "order_id": f"O-{day}",
                        "is_before_19": True, "is_online": False,
                        "is_offline": True, "is_jielong": False,
                        "is_jsd": False, "is_new": True, "is_old": False,
                    }
                    for day in days
                ])
                conn.execute(
                    "CREATE TABLE v014_stage_customer_events AS "
                    "SELECT * FROM customer_events"
                )
            finally:
                conn.close()

            result = run_v014_shadow_week(
                store_id="A3XV", end=days[-1], source_db=source, output_db=output,
            )
            self.assertEqual(days[1], result.window.start)
            self.assertGreater(result.row_count, 0)
            read = duckdb.connect(str(output), read_only=True)
            try:
                schema = read.execute("DESCRIBE t_v014_levels_result").fetchall()
                self.assertEqual(125, len(schema))
                self.assertEqual("DECIMAL(18,4)", schema[14][1])
                sku = read.execute(
                    "SELECT end_stock_qty, total_sale_amount, article_group_id, "
                    "store_profit_amount "
                    "FROM t_v014_levels_result WHERE level_description='sku' "
                    "AND day_clear='1' AND business_date=?",
                    [days[-1]],
                ).fetchone()
                self.assertEqual((2.0, 20.0, "G1"), tuple(map(float, sku[:2])) + (sku[2],))
                self.assertEqual(10.0, float(sku[3]))
                failures = read.execute(
                    "SELECT COUNT(*) FROM v014_validation_result "
                    "WHERE gate_type='HARD' AND NOT passed"
                ).fetchone()[0]
                self.assertEqual(0, failures)
                category_gate = read.execute(
                    "SELECT passed FROM v014_validation_result "
                    "WHERE check_name='CATEGORY_SNAPSHOT_EVIDENCE'"
                ).fetchone()
                self.assertEqual((False,), category_gate)
                manifest = read.execute(
                    "SELECT engine_version, publish_eligible, "
                    "category_evidence_status FROM v014_run_manifest"
                ).fetchone()
                self.assertEqual(("0.17", False, "LEGACY_STATIC_SNAPSHOT"), manifest)
                for table in (
                    "t_v014_levels_result",
                    "v014_sku_daily",
                    "v014_relation_registry",
                    "v014_relation_resolution",
                ):
                    published_days = read.execute(
                        f'SELECT MIN(business_date), MAX(business_date) FROM "{table}"'
                    ).fetchone()
                    self.assertEqual((days[1], days[-1]), published_days)
            finally:
                read.close()


if __name__ == "__main__":
    unittest.main()
