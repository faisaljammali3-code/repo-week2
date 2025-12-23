import pandas as pd
import csv
from  pathlib import Path
import sys



ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT/"src"))

from bootcamp_data.config import make_paths
from bootcamp_data.io import read_orders_csv ,read_users_csv,write_parquet
from bootcamp_data.transforms import enforce_schema

def main():
    paths=make_paths(ROOT)
    orders=read_orders_csv(paths.raw/"orders.csv")
    users=read_users_csv(paths.raw/"users.csv")

    orders_trans=enforce_schema(orders)
    
    print(orders)

    print("Writing parquet files...")
    write_parquet(orders_trans, paths.processed / "orders.parquet")
    write_parquet(users, paths.processed / "users.parquet")
#يختلف عن السلايدات
if __name__=="__main__":
    main()