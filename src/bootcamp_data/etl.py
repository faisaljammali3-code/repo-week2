import json
import logging
from dataclasses import dataclass, asdict
from pathlib import Path
import pandas as pd

from bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet
from bootcamp_data.quality import require_columns, assert_non_empty, assert_unique_key, assert_in_range
from bootcamp_data.transforms import (
    enforce_schema,
    normalize_text,
    apply_mapping,
    add_missing_flags,
    parse_datetime,
    add_time_parts,
    winsorize,
    add_outlier_flag
)
from bootcamp_data.joins import safe_left_join

log = logging.getLogger(__name__)

@dataclass(frozen=True)
class ETLConfig:
    root: Path
    raw_orders: Path
    raw_users: Path
    out_orders_clean: Path
    out_users: Path
    out_analytics: Path
    run_meta: Path

def load_inputs(cfg: ETLConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    log.info("Loading raw inputs...")
    orders = read_orders_csv(cfg.raw_orders)
    users = read_users_csv(cfg.raw_users)
    return orders, users

def transform(orders_raw: pd.DataFrame, users: pd.DataFrame) -> pd.DataFrame:
    log.info("Starting transformation pipeline...")
    
    require_columns(orders_raw, ["order_id", "user_id", "amount", "quantity", "created_at", "status"])
    require_columns(users, ["user_id", "country", "signup_date"])
    assert_non_empty(orders_raw, "orders_raw")
    assert_non_empty(users, "users")
    assert_unique_key(users, "user_id")

    mapping = {"paid": "paid", "refund": "refund", "refunded": "refund"}
    
    orders = (
        orders_raw
        .pipe(enforce_schema)
        .assign(
            status_clean=lambda d: apply_mapping(normalize_text(d["status"]), mapping)
        )
        .pipe(add_missing_flags, cols=["amount", "quantity"])
        .pipe(parse_datetime, col="created_at", utc=True)
        .pipe(add_time_parts, ts_col="created_at")
    )

    log.info("Joining orders with users...")
    joined = safe_left_join(
        orders,
        users,
        on="user_id",
        validate="many_to_one",
        suffixes=("", "_user")
    )
    
    if len(joined) != len(orders):
        raise AssertionError(f"Join explosion detected! Input: {len(orders)}, Output: {len(joined)}")

    joined = joined.assign(amount_winsor=winsorize(joined["amount"]))
    joined = joined.pipe(add_outlier_flag, col="amount", k=1.5)
    
    assert_in_range(joined["amount"], lo=0, name="amount")

    return joined

def load_outputs(analytics: pd.DataFrame, users: pd.DataFrame, cfg: ETLConfig) -> None:
    log.info("Writing processed outputs (Idempotent)...")
    write_parquet(analytics, cfg.out_analytics)
    write_parquet(users, cfg.out_users)
    
    cols_to_drop = [c for c in analytics.columns if c in users.columns and c != "user_id"]
    orders_clean = analytics.drop(columns=cols_to_drop, errors="ignore")
    write_parquet(orders_clean, cfg.out_orders_clean)

def write_run_meta(cfg: ETLConfig, analytics: pd.DataFrame) -> None:
    meta = {
        "rows_out": len(analytics),
        "columns": analytics.columns.tolist(),
        "config": {k: str(v) for k, v in asdict(cfg).items()},
        "missing_created_at": int(analytics["created_at"].isna().sum()) if "created_at" in analytics.columns else 0,
        "country_match_rate": 1.0 - float(analytics["country"].isna().mean()) if "country" in analytics.columns else 0.0
    }
    
    cfg.run_meta.parent.mkdir(parents=True, exist_ok=True)
    cfg.run_meta.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    log.info(f"Run metadata saved to {cfg.run_meta}")

def run_etl(cfg: ETLConfig) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    
    orders_raw, users = load_inputs(cfg)
    analytics = transform(orders_raw, users)
    load_outputs(analytics, users, cfg)
    write_run_meta(cfg, analytics)
    log.info("ETL Pipeline finished successfully")