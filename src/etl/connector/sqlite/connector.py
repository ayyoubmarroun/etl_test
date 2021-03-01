from etl.connector.base import BaseConnector
import sqlite3
import pandas as pd


class SqliteConnector(BaseConnector):

    def connect(self, database: str, **kwargs):
        self._con = sqlite3.connect(database, **kwargs)

    def append_data(self, table: str, data: pd.DataFrame, **kwargs):
        data.to_sql(name=table, con=self._con, if_exists="append",  index=False, **kwargs)

    def create_table(self, table: str):
        cursor = self._con.cursor()
        cursor.execute(table)
        self._con.commit()
    
    def query(self, query: str) -> pd.DataFrame:
        return pd.read_sql(query,self._con)
