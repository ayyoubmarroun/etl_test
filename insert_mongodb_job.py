import os
import sys
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), 'src')))
import pandas as pd
from etl.reader import Reader
from etl.connector import MongoConnector
from etl.transform import TransformRetail


current_dir = os.path.dirname(os.path.abspath(__file__))

if __name__ == "__main__":

    # Read csv data and parse types
    dtypes = Reader.read_json(os.path.join(current_dir, "data/dtypes.json"))
    data = pd.read_csv(os.path.join(current_dir,"data/online retail.csv"), dtype=dtypes, parse_dates=["InvoiceDate"])
    
    # Clean  and process data
    data = TransformRetail.process(data)

    # Connect and authenticate to MongoDB database, using auth json file
    auth = Reader.read_json(os.path.join(current_dir, "auth/mongo.json"))
    db = MongoConnector()
    db.connect(**auth)
    db.append_data("online_retail", data)

    # Part 2. 
    # calculates the aggregate monthly price for the year 2011 and country United
    # Kingdom
    
    # Load query from filea and run it
    query = Reader.read_json(os.path.join(current_dir, "query/monthly_price_uk_2011.json"))
    monthly_price_uk_2011 = db.query_agg("online_retail", query)
    # Print and store data to csv
    print(monthly_price_uk_2011)
    monthly_price_uk_2011.to_csv(os.path.join(current_dir,"data/monthly_price_uk_2011_mongo.csv"), index=False)

    