select country, EXTRACT(YEAR FROM InvoiceDate) as "year", EXTRACT(MONTH FROM InvoiceDate) as "month"  , sum(UnitPrice*Quantity) as price
from online_retail
where EXTRACT(YEAR FROM InvoiceDate) = 2011 and Country = "United Kingdom"
group by 1,2,3
order by 3;