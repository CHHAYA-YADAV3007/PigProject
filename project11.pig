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

B = FOREACH A GENERATE fbicode as fbicode, case_number AS case_number;

C = FILTER B BY fbicode IS NOT NULL AND case_number IS NOT NULL ;

D = GROUP C BY fbicode;

E = FOREACH D GENERATE group,COUNT(C.fbicode);

STORE E INTO '/user/cloudera/chhaya/pig_first_project/pigqueryoutput1.txt';

DUMP E;


