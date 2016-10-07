REGISTER '/home/acadgild/pig-0.14.0/lib/piggybank.jar'

REGISTER '/home/acadgild/CheckBPL.jar'
DEFINE CHECK_BPL apache_pig_udf.CheckBPLForEightyPercent();


1. FIND OUT THE DISTRICTS WHO ACHIEVED 100 PERCENT OBJECTIVE IN BPL CARDS

xml = load'/flume_import' using org.apache.pig.piggybank.storage.XMLLoader('row')as (doc:chararray);

xml_row = foreach xml generate FLATTEN(REGEX_EXTRACT_ALL(doc,'\\s*<row>\\s*<State_Name>(.*)</State_Name>\\s*<District_Name>(.*)</District_Name>\\s*<Project_Objectives_IHHL_BPL>(.*)</Project_Objectives_IHHL_BPL>\\s*<Project_Objectives_IHHL_APL>(.*)</Project_Objectives_IHHL_APL>\\s*<Project_Objectives_IHHL_TOTAL>(.*)</Project_Objectives_IHHL_TOTAL>\\s*<Project_Objectives_SCW>(.*)</Project_Objectives_SCW>\\s*<Project_Objectives_School_Toilets>(.*)</Project_Objectives_School_Toilets>\\s*<Project_Objectives_Anganwadi_Toilets>(.*)</Project_Objectives_Anganwadi_Toilets>\\s*<Project_Objectives_RSM>(.*)</Project_Objectives_RSM>\\s*<Project_Objectives_PC>(.*)</Project_Objectives_PC>\\s*<Project_Performance-IHHL_BPL>(.*)</Project_Performance-IHHL_BPL>\\s*<Project_Performance-IHHL_APL>(.*)</Project_Performance-IHHL_APL>\\s*<Project_Performance-IHHL_TOTAL>(.*)</Project_Performance-IHHL_TOTAL>\\s*<Project_Performance-SCW>(.*)</Project_Performance-SCW>\\s*<Project_Performance-School_Toilets>(.*)</Project_Performance-School_Toilets>\\s*<Project_Performance-Anganwadi_Toilets>(.*)</Project_Performance-Anganwadi_Toilets>\\s*<Project_Performance-RSM>(.*)</Project_Performance-RSM>\\s*<Project_Performance-PC>(.*)</Project_Performance-PC>\\s*</row>')) as (state:chararray,district:chararray,objBPL:long,objAPL:long,objTotal:long,objScw:long,objschool:long,objaanganwadi:long,objRsm:long,objPc:long,perfBPL:long,perfTotal:long,perfScw:long,perfschool:long,perfaanganwadi:long,perfRsm:long,perfPc:long);

C = foreach xml_row generate district,objBPL,perfBPL;
D = filter C by objBPL==perfBPL;
Store D into '/districtOutput' using PigStorage(',');


exporting to mysql

sqoop-export --connect jdbc:mysql://localhost/test --username root --table district_with_100percent_BPLobjectives -m 1 --export-dir '/districtOutput/part-m-00000'




2. Write a Pig UDF to filter the districts who have reached 80% of objectives of BPL cards


xml = load'/flume_import' using org.apache.pig.piggybank.storage.XMLLoader('row')as (doc:chararray);

xml_row = foreach xml generate FLATTEN(REGEX_EXTRACT_ALL(doc,'\\s*<row>\\s*<State_Name>(.*)</State_Name>\\s*<District_Name>(.*)</District_Name>\\s*<Project_Objectives_IHHL_BPL>(.*)</Project_Objectives_IHHL_BPL>\\s*<Project_Objectives_IHHL_APL>(.*)</Project_Objectives_IHHL_APL>\\s*<Project_Objectives_IHHL_TOTAL>(.*)</Project_Objectives_IHHL_TOTAL>\\s*<Project_Objectives_SCW>(.*)</Project_Objectives_SCW>\\s*<Project_Objectives_School_Toilets>(.*)</Project_Objectives_School_Toilets>\\s*<Project_Objectives_Anganwadi_Toilets>(.*)</Project_Objectives_Anganwadi_Toilets>\\s*<Project_Objectives_RSM>(.*)</Project_Objectives_RSM>\\s*<Project_Objectives_PC>(.*)</Project_Objectives_PC>\\s*<Project_Performance-IHHL_BPL>(.*)</Project_Performance-IHHL_BPL>\\s*<Project_Performance-IHHL_APL>(.*)</Project_Performance-IHHL_APL>\\s*<Project_Performance-IHHL_TOTAL>(.*)</Project_Performance-IHHL_TOTAL>\\s*<Project_Performance-SCW>(.*)</Project_Performance-SCW>\\s*<Project_Performance-School_Toilets>(.*)</Project_Performance-School_Toilets>\\s*<Project_Performance-Anganwadi_Toilets>(.*)</Project_Performance-Anganwadi_Toilets>\\s*<Project_Performance-RSM>(.*)</Project_Performance-RSM>\\s*<Project_Performance-PC>(.*)</Project_Performance-PC>\\s*</row>')) as (state:chararray,district:chararray,objBPL:long,objAPL:long,objTotal:long,objScw:long,objschool:long,objaanganwadi:long,objRsm:long,objPc:long,perfBPL:long,perfTotal:long,perfScw:long,perfschool:long,perfaanganwadi:long,perfRsm:long,perfPc:long);

C = foreach xml_row generate district,objBPL,perfBPL;
D = foreach C generate district,objBPL,perfBPL,CHECK_BPL(objBPL,perfBPL) as check;
E = filter D by check=='Yes';
F = foreach E generate district,objBPL,perfBPL;
Store F into '/BPL' using PigStorage(',');

exporting to mysql

sqoop-export --connect jdbc:mysql://localhost/test --username root --table district_with_80percent_BPLobjectives_progress -m 1 --export-dir '/BPL/part-m-00000'







