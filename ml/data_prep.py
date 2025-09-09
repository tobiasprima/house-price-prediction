import pandas as pd
from sqlalchemy import create_engine

df = pd.read_csv("/data/raw/csv")

engine = create_engine("postgresql://admin:admin@localhost:5432/house_prices")

df.to_sql("raw_house_prices", engine, if_exists="replace", index=False)