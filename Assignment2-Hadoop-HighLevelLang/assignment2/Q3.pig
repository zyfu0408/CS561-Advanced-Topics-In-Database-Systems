/*
 countryCode, NumberOfCutomers, MinTRansTotal, MAxTransTotal
*/

cust = LOAD '/user/hadoop/project_1/customers.txt' USING PigStorage(',') AS (id:int, name:chararray, age:int, countryCode:int, salary:float);

trans = LOAD '/user/hadoop/project_1/transactions.txt' USING PigStorage(',') as (transId:int, custId:int, transTotal:float, transNumItems:int, transDesc:chararray);

cust_trans = JOIN cust BY id, trans BY custId;

cc_data = GROUP cust_trans BY countryCode;

result = foreach cc_data generate group, COUNT(cust_trans.custId), MIN(cust_trans.transTotal), MAX(cust_trans.transTotal);

STORE result INTO '/user/hadoop/project_1/Q3';
