CREATE TABLE stock_price (ticker string, open float, close float, adj_close float, lowThe float, highThe float, volume bigint, data date) row format delimited fields terminated by '\t';
LOAD DATA LOCAL INPATH '/home/hadoop/Desktop/bigdata_new.csv' OVERWRITE INTO TABLE stock_price;

CREATE TABLE support3 AS (SELECT ticker, MIN(data) AS minData, MONTH(data) as month, MAX(data) AS maxData FROM stock_price WHERE YEAR(data) == '2017' GROUP BY ticker, MONTH(data));

CREATE TABLE minimum3 AS (SELECT stock_price.ticker AS ticker, minData, month, close FROM support3 JOIN stock_price ON stock_price.ticker = support3.ticker WHERE minData = data ORDER BY ticker, month); 
CREATE TABLE maximum3 AS (SELECT stock_price.ticker AS ticker, maxData, month, close FROM support3 JOIN stock_price ON stock_price.ticker = support3.ticker WHERE maxData = data ORDER BY ticker, month); 

DROP TABLE stock_price;
DROP TABLE support3;

CREATE TABLE res3 AS (SELECT minimum3.ticker AS ticker, minData, maxData, minimum3.month as month, minimum3.close as minClose, maximum3.close as maxClose, ((maximum3.close-minimum3.close)/minimum3.close)*100 AS varClose FROM minimum3 JOIN maximum3 ON minimum3.ticker = maximum3.ticker WHERE month(minData) = month(maxData));

DROP TABLE minimum3;
DROP TABLE maximum3;

CREATE TABLE result3 AS (SELECT a.ticker as ticker1, b.ticker as ticker2, a.month as month, a.varClose as varClose1, b.varClose as varClose2 FROM res3 as a join res3 as b on a.month = b.month where abs(a.varClose - b.varClose) <= 1.0 and a.ticker < b.ticker);

DROP TABLE res3;

CREATE TABLE result31 AS (SELECT ticker1, ticker2, count(month) as mesi FROM result3 group by ticker1, ticker2);

CREATE TABLE result32 AS (SELECT a.ticker1 as ticker1, a.ticker2 as ticker2, month, varClose1, varClose2 FROM result3 as a join result31 as b on a.ticker1 = b.ticker1 and a.ticker2 = b.ticker2 where mesi == 12 order by ticker1, ticker2, month);

DROP TABLE result3;
DROP TABLE result31;

SELECT * FROM result32 LIMIT 10;

DROP TABLE result32;
