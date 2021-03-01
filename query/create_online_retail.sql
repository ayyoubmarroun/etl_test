CREATE TABLE IF NOT EXISTS online_retail (
    InvoiceNo VARCHAR(20) NOT NULL,
    StockCode VARCHAR(20) NOT NULL,
    Description VARCHAR(100),
    Quantity INT NOT NULL,
    InvoiceDate DATETIME NOT NULL,
    UnitPrice FLOAT NOT NULL,
    CustomerID INT,
    Country VARCHAR(40),
    PRIMARY KEY(InvoiceNo, StockCode)
);