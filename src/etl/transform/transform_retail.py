import pandas as pd


class TransformRetail():

    @classmethod
    def process(cls, data) -> pd.DataFrame:
        data = data[pd.notnull(data["CustomerID"])]
        data = data[data["Quantity"] > 0]
        data = data[data["UnitPrice"] > 0]
        data = data.groupby(by=["InvoiceNo", "StockCode"]).agg({ 
            "Description": "first",
            "Quantity": "sum",
            "InvoiceDate": "first",
            "UnitPrice": "first",
            "Country": "first"
        }).reset_index()
        return data