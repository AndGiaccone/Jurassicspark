CREATE TABLE stock_price (ticker string, open float, close float, adj_close float, lowThe float, highThe float, volume bigint, data date) row format delimited fields terminated by '\t';

load data local inpath '/home/hadoop/Desktop/bigdata_new.csv' overwrite into table stock_price;

create table service1 as (select ticker, max(highThe) as high, min(lowThe) as low, min(data) as min, max(data) as max from stock_price group by ticker);

create table service3 as (select service1.ticker as ticker, max, high, low, data, close from service1 join stock_price on service1.ticker = stock_price.ticker where data = max order by max);

create table service4 as (select service1.ticker as ticker, min, high, low, data, close from service1 join stock_price on service1.ticker = stock_price.ticker where data = min);

drop table stock_price;

create table result11 as (select a.ticker, min, max, round((b.close - a.close) / a.close * 100) as avgclose, a.high, a.low from (select ticker, high, low, min, close from service4) as a join (select ticker, max, close from service3) as b on a.ticker = b.ticker);

drop table service1;
drop table service3;
drop table service4;

select * from result11 limit 10;

drop table result11;
