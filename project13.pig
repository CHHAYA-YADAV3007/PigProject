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
B = FOREACH A GENERATE district as district,primary_type as primary_type,arrest as arrest;

C = FILTER B BY district IS NOT NULL;

D = FILTER C BY primary_type == 'THEFT';

E = FILTER D BY arrest ;

F = FOREACH E GENERATE district as district,primary_type as primary_type ;

G = GROUP F BY district;

H = FOREACH G GENERATE group,COUNT(F);

STORE H INTO '/user/cloudera/chhaya/pig_first_project/pigqueryoutput3.txt';

DUMP H;

