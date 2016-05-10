/*
	CustomerId, Name, Salary, NumOfTransactions, TotalSum, MinItems
*/

cust = LOAD '/user/hadoop/project_1/customers.txt' USING PigStorage(',') AS (id:int, name:chararray, age:int, countrycode:int, salary:float);

trans = LOAD '/user/hadoop/project_1/transactions.txt' USING PigStorage(',') as (transId:int, custId:int, transTotal:float, transNumItems:int, transDesc:chararray);

cust_trans = COGROUP cust BY id, trans BY custId;

Result = FOREACH cust_trans GENERATE group as custId, flatten(cust.name), flatten(cust.salary), COUNT(trans.transId), SUM(trans.transTotal), MIN(trans.transNumItems);

STORE Result INTO '/user/hadoop/project_1/Q2';
