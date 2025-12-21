import pandas as pd
import pathlib as Path

NA=["","non" ,"NA","null","None"]
def read_orders_csv (path :Path)-> pd.DataFrame:
    return pd.read_csv(path,
                   dtype={"user_id": "string","order_id":"string"},
                   na_values=NA,
                   keep_default_na=True,
                   )
    

def read_users_csv (path :Path)-> pd.DataFrame:
         return pd.read_csv(path,
                   dtype={"user_id":"string"},
                   na_values=NA,
                   keep_default_na=True,
                            )
def write_parquet(df:pd.DataFrame,path:Path)->None:
       path.parent.mkdir(parents=True)
       df.to_parquet(path,index=False)
def read_parquet(path:Path)-> pd.DataFrame:
       return pd.read_parquet(path)