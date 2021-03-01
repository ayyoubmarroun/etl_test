from etl.connector.base import BaseConnector
import sqlalchemy
import pandas as pd


class MySQLConnector(BaseConnector):

    def connect(self, database:str, host: str, user:str, password:str, port=3306, **kwargs):
        self._con = sqlalchemy.create_engine(
            "mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}".format(
                user=user, password=password,host=host, database=database, port=port
            )   
        ).connect()

    def append_data(self, table: str, data: pd.DataFrame, **kwargs):
        data.to_sql(name=table, con=self._con, if_exists="append", index=False, **kwargs)

    def create_table(self, query: str):
        with self._con.connect() as con:
            con.execute(query)
    
    def query(self, query: str) -> pd.DataFrame:
        return pd.read_sql(query,self._con)
