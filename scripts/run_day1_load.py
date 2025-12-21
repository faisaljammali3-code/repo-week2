import pandas as pd
import csv
from  pathlib import Path
import sys



ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT/"src"))

from bootcamp_data.config import make_paths
from bootcamp_data.io import read_orders_csv ,read_users_csv,write_parquet
from bootcamp_data.transforms import enforce_schema

paths=make_paths(ROOT)
orders=read_orders_csv(paths.raw/"orders.csv")
users=read_users_csv(paths.raw/"users.csv")
orders_trans=enforce_schema(orders)
users_trans=enforce_schema(users)
print(orders)

#يختلف عن السلايدات
