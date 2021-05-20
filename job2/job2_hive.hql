CREATE TABLE stock_price (ticker string, open float, close float, adj_close float, lowThe float, highThe float, volume bigint, data date) row format delimited fields terminated by '\t';
LOAD DATA LOCAL INPATH '/home/hadoop/Desktop/bigdata_new.csv' OVERWRITE INTO TABLE stock_price;

CREATE TABLE meta_stock (ticker string, exc string, name string, sector string, industry string) row format delimited fields terminated by '\t';
LOAD DATA LOCAL INPATH '/home/hadoop/Desktop/historical_stock_new.csv' OVERWRITE INTO TABLE meta_stock;


CREATE TABLE support AS (SELECT ticker, MIN(data) AS minData, YEAR(data) AS year, MAX(data) AS maxData, SUM(volume) AS volume FROM stock_price WHERE YEAR(data) >= '2009' AND YEAR(data) <= '2018' GROUP BY ticker, YEAR(data));

CREATE TABLE minimum AS (SELECT stock_price.ticker AS ticker, minData, year, close FROM support JOIN stock_price ON stock_price.ticker = support.ticker WHERE minData = data ORDER BY ticker); 
CREATE TABLE maximum AS (SELECT stock_price.ticker AS ticker, maxData, year, close, support.volume as volume FROM support JOIN stock_price ON stock_price.ticker = support.ticker WHERE maxData = data ORDER BY ticker); 

DROP TABLE stock_price;
DROP TABLE support;

CREATE TABLE res0 AS (SELECT minimum.ticker AS ticker, minData, maxData, minimum.year as year, minimum.close as minClose, maximum.close as maxClose, ROUND(((maximum.close-minimum.close)/minimum.close)*100) AS varClose, volume FROM minimum JOIN maximum ON minimum.ticker = maximum.ticker WHERE YEAR(minData) = YEAR(maxData));

DROP TABLE minimum;
DROP TABLE maximum;

CREATE TABLE res AS (SELECT res0.ticker as ticker, meta_stock.sector as sector, minData, maxData, year, minClose, maxClose, varClose, volume FROM res0 JOIN meta_stock ON res0.ticker = meta_stock.ticker);

DROP TABLE res0;

CREATE TABLE res_joined AS (SELECT res.sector as sector, res.year as year, SUM(minClose) as sumMinClose, SUM(maxClose) as sumMaxClose, MAX(varClose) AS bestVarClose, MAX(volume) as bestVolume FROM res JOIN meta_stock ON res.ticker = meta_stock.ticker GROUP BY res.sector, res.year ORDER BY sector);

DROP TABLE meta_stock;

CREATE TABLE result_best_close AS (SELECT res.ticker AS ticker, res.sector as sector, res.year as year, varClose FROM res JOIN res_joined ON res.sector = res_joined.sector WHERE res.year = res_joined.year and varClose = bestVarClose);

CREATE TABLE result_best_volume AS (SELECT res.ticker AS ticker, res.sector as sector, res.year as year, volume FROM res JOIN res_joined ON res.sector = res_joined.sector WHERE res.year = res_joined.year and volume = bestVolume);

DROP TABLE res;

CREATE TABLE result0 AS (SELECT res_joined.sector AS sector, res_joined.year AS year, ROUND(((sumMaxClose-sumMinClose)/sumMinClose)*100) AS sectorVarClose, result_best_close.ticker as bestVarCloseTicker, result_best_close.varClose AS bestVarClose FROM res_joined JOIN result_best_close ON res_joined.sector = result_best_close.sector where res_joined.year = result_best_close.year);

DROP TABLE res_joined;
DROP TABLE result_best_close;

CREATE TABLE result AS (SELECT result0.sector AS sector, result0.year AS year, sectorVarClose, bestVarCloseTicker, result0.bestVarClose AS bestVarClose, result_best_volume.ticker AS bestVolumeTicker, result_best_volume.volume AS bestVolume FROM result0 JOIN result_best_volume ON result0.sector = result_best_volume.sector where result0.year = result_best_volume.year);

DROP TABLE result_best_volume;
DROP TABLE result0;

SELECT * FROM result limit 10;

DROP TABLE result;
