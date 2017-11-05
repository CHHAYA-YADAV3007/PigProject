REGISTER 'piggybank-0.17.0.jar';
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage();
A = LOAD '/user/cloudera/chhaya/pig_first_project/crime.csv' USING CSVExcelStorage(',') AS (
id:int,
case_number:chararray,
dated:chararray,
block:chararray,
iucr:int,
primary_type:chararray,
description:chararray,
location_description:chararray,
arrest:boolean,
domestic:boolean,
beat:int,
district:int,
ward:int,
community_area:int,
fbicode:chararray,
x_oordinate:int,
y_coordinate:int,
year:int,
updated_on:chararray,
latitude:float,
longitude:float,
location:chararray
);

B = FOREACH A GENERATE dated as date ,primary_type as primary_type,arrest as arrest;

D = FILTER B BY primary_type == 'THEFT';

E = FILTER D BY arrest ;

F = FILTER E BY date IS NOT NULL;

G = FOREACH F GENERATE date;

H = FOREACH G GENERATE (
INDEXOF(date,'-',0)==2 ?
CONCAT(SUBSTRING(date,6,10),CONCAT(SUBSTRING(date,3,5),SUBSTRING(date,0,2))):
(INDEXOF(date,'/',0)==2 ? 
CONCAT(SUBSTRING(date,6,10),CONCAT(SUBSTRING(date,0,2),SUBSTRING(date,3,5))):
SUBSTRING(date,0,10))
)
AS YYYYMMDD;

I = FOREACH H GENERATE ToDate(YYYYMMDD,'YYYYMMDD') AS dt;

J = FILTER I BY dt > ToDate('2014-09-30') AND dt < ToDate('2015-11-01');

K = GROUP J ALL;

L = FOREACH K GENERATE COUNT(J.dt);

STORE L INTO '/user/cloudera/chhaya/pig_first_project/pigqueryoutput4.txt';

DUMP L;

