import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

#using pip install -e
from bootcamp_data.transforms import (apply_mapping,dedupe_keep_latest,add_missing_flags,enforce_schema,missingness_report,normalize_text)
from bootcamp_data.io import( read_orders_csv,read_parquet,read_users_csv,write_parquet)
from bootcamp_data.config import (dataclass,make_paths)
from bootcamp_data.quality import (assert_in_range,assert_non_empty,assert_unique_key,require_columns
)

def main()->None:

    paths=make_paths(ROOT)
    orders=read_orders_csv(paths.raw/"orders.csv")
    users=read_users_csv(paths.raw/"users.csv")

    orders_columns=['order_id','user_id','amount','quantity','created_at','status']
    users_columns=['user_id','country','signup_date']
    require_columns(orders,orders_columns)
    require_columns(users,users_columns)

    assert_non_empty(orders,"orders")
    assert_non_empty(users,"users")

    assert_unique_key(orders,"order_id")
    assert_unique_key(users,"user_id")

    orders_p=enforce_schema(orders)

    reports_dir = ROOT / "reports"
    reports_dir.mkdir(exist_ok=True)
    missingness_report(orders_p).to_csv(reports_dir / "missingness_orders.csv")
    missingness_report(orders).to_csv("reports/missingness_orders.csv")


    status_norm = normalize_text(orders_p["status"])

    mapping = {"paid": "paid", "refund": "refund", "refunded": "refund"}
    status_clean = apply_mapping(status_norm, mapping)
    
    
    orders_clean = (
                orders_p
                .assign(status_clean=status_clean)
                .pipe(add_missing_flags, cols=["amount", "quantity"])
                    )
    
    assert_in_range(orders_clean["amount"], lo=0,
    name="amount")
    assert_in_range(orders_clean["quantity"], lo=0,
    name="quantity")


    write_parquet(orders_clean, paths.processed / "orders_clean.parquet")
    write_parquet(users, paths.processed / "users.parquet")



if __name__ == "__main__":
    main()