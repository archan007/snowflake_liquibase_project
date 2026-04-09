"""
Integration test: simulate drop-safety scenarios using a fake state reader.

Tests the three critical behaviours:
  1. Breaking drop (table) without confirmation -> FAIL
  2. Breaking drop (table) WITH confirmation -> succeeds
  3. Non-breaking drop (stream/task) without confirmation -> succeeds silently
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from engine.config_loader import load_all_bundles, load_platform_config
from engine.generate_ddl import compute_changesets, DropSafetyError
from engine.state_reader import ExistingObject, ExistingColumn


BUNDLES_ROOT = Path("bundles")
PLATFORM_ROOT = Path("platform")
ENV = "DEV"
DB = "DEV_FS_DB"


def _existing_state_with_extras():
    """Simulate Snowflake state that includes everything in bundles PLUS
    extra objects that would need to be dropped."""
    existing = {}

    # All objects currently in bundles (so they match desired state)
    for fqn in [
        f"{DB}.BRONZE.RAW_ORDERS",
        f"{DB}.BRONZE.RAW_CUSTOMERS",
        f"{DB}.SILVER.DIM_CUSTOMER",
        f"{DB}.GOLD.FCT_SALES",
    ]:
        db, sch, name = fqn.split(".")
        existing[fqn] = ExistingObject(
            object_type="table", database=db, schema=sch, name=name,
            columns=[],
        )

    # Add an EXTRA table that is not in any bundle - should trip the drop-safety check
    extra_table = f"{DB}.BRONZE.RAW_LEGACY_ORPHAN"
    existing[extra_table] = ExistingObject(
        object_type="table",
        database=DB, schema="BRONZE", name="RAW_LEGACY_ORPHAN",
    )

    # Add an EXTRA stream that is not in any bundle - should drop silently
    extra_stream = f"{DB}.BRONZE.STRM_LEGACY"
    existing[extra_stream] = ExistingObject(
        object_type="stream",
        database=DB, schema="BRONZE", name="STRM_LEGACY",
    )

    # Add an EXTRA task that is not in any bundle - should drop silently
    extra_task = f"{DB}.GOLD.TSK_LEGACY"
    existing[extra_task] = ExistingObject(
        object_type="task",
        database=DB, schema="GOLD", name="TSK_LEGACY",
    )

    return existing


def test_unconfirmed_table_drop_fails():
    print("\n[TEST 1] Unconfirmed breaking drop (table) should FAIL")
    platform_cfg = load_platform_config(PLATFORM_ROOT, ENV)
    bundles = load_all_bundles(BUNDLES_ROOT, ENV, platform_cfg)
    existing = _existing_state_with_extras()

    try:
        compute_changesets(bundles, existing)
        print("  FAIL: expected DropSafetyError but none raised")
        return False
    except DropSafetyError as e:
        msg = str(e)
        if "RAW_LEGACY_ORPHAN" in msg and "BREAKING CHANGE" in msg:
            print("  PASS: DropSafetyError raised with correct object listed")
            print("  --- error message ---")
            for line in msg.splitlines()[:12]:
                print("  " + line)
            return True
        print(f"  FAIL: error message missing expected content: {msg}")
        return False


def test_confirmed_table_drop_succeeds():
    print("\n[TEST 2] Confirmed breaking drop (table) should SUCCEED")
    bronze_yaml = BUNDLES_ROOT / "bronze" / "bundle.yaml"
    original = bronze_yaml.read_text()
    try:
        patched = original.replace(
            "confirmed_drops: []",
            f"confirmed_drops:\n  - {DB}.BRONZE.RAW_LEGACY_ORPHAN",
        )
        bronze_yaml.write_text(patched)

        platform_cfg = load_platform_config(PLATFORM_ROOT, ENV)
        bundles = load_all_bundles(BUNDLES_ROOT, ENV, platform_cfg)
        existing = _existing_state_with_extras()

        changesets = compute_changesets(bundles, existing)
        drop_table_cs = [
            c for c in changesets
            if c.operation == "drop" and c.object_type == "table"
        ]
        if len(drop_table_cs) == 1 and "RAW_LEGACY_ORPHAN" in drop_table_cs[0].sql:
            print("  PASS: confirmed table drop generated")
            return True
        print(f"  FAIL: expected 1 table drop, got {len(drop_table_cs)}")
        return False
    finally:
        bronze_yaml.write_text(original)


def test_non_breaking_drops_silent():
    print("\n[TEST 3] Non-breaking drops (stream, task) should SUCCEED silently")
    platform_cfg = load_platform_config(PLATFORM_ROOT, ENV)
    bundles = load_all_bundles(BUNDLES_ROOT, ENV, platform_cfg)

    existing = {}
    for fqn, t in [
        (f"{DB}.BRONZE.RAW_ORDERS", "table"),
        (f"{DB}.BRONZE.RAW_CUSTOMERS", "table"),
        (f"{DB}.SILVER.DIM_CUSTOMER", "table"),
        (f"{DB}.GOLD.FCT_SALES", "table"),
    ]:
        db, sch, name = fqn.split(".")
        existing[fqn] = ExistingObject(
            object_type=t, database=db, schema=sch, name=name
        )
    # Orphan stream + task (no confirmation required)
    existing[f"{DB}.BRONZE.STRM_LEGACY"] = ExistingObject(
        object_type="stream", database=DB, schema="BRONZE", name="STRM_LEGACY"
    )
    existing[f"{DB}.GOLD.TSK_LEGACY"] = ExistingObject(
        object_type="task", database=DB, schema="GOLD", name="TSK_LEGACY"
    )

    try:
        changesets = compute_changesets(bundles, existing)
    except DropSafetyError as e:
        print(f"  FAIL: unexpected DropSafetyError: {e}")
        return False

    drops = [c for c in changesets if c.operation == "drop"]
    stream_drops = [c for c in drops if c.object_type == "stream"]
    task_drops = [c for c in drops if c.object_type == "task"]

    if len(stream_drops) == 1 and "STRM_LEGACY" in stream_drops[0].sql:
        print("  PASS: stream dropped silently")
    else:
        print(f"  FAIL: expected 1 stream drop, got {len(stream_drops)}")
        return False

    if len(task_drops) == 1 and "TSK_LEGACY" in task_drops[0].sql:
        print("  PASS: task dropped silently")
        return True
    print(f"  FAIL: expected 1 task drop, got {len(task_drops)}")
    return False


if __name__ == "__main__":
    results = [
        test_unconfirmed_table_drop_fails(),
        test_confirmed_table_drop_succeeds(),
        test_non_breaking_drops_silent(),
    ]
    passed = sum(results)
    total = len(results)
    print(f"\n{'='*50}\n{passed}/{total} tests passed")
    sys.exit(0 if passed == total else 1)
