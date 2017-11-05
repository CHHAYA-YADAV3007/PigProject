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

B = FOREACH A GENERATE id as id,case_number AS cr, fbicode as fc;

C = FILTER B BY id IS NOT NULL AND cr IS NOT NULL AND fc IS NOT NULL;

D = GROUP C BY fc;

E = FOREACH D GENERATE group as fcode,COUNT(C.fc) as totalcount ;

F = FILTER E BY fcode  == '32';

STORE F INTO '/user/cloudera/chhaya/pig_first_project/pigqueryoutput2.txt';

DUMP F;

