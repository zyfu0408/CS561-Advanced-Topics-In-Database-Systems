/*
 CustomerName, Number of Transactions
*/

cust = LOAD '/user/hadoop/project_1/customers.txt' USING PigStorage(',') AS (id:int, name:chararray, age:int, countryCode:int, salary:float);

trans = LOAD '/user/hadoop/project_1/transactions.txt' USING PigStorage(',') as (transId:int, custId:int, transTotal:float, transNumItems:int, transDesc:chararray);

--cust_names = FOREACH cust GENERATE id as custId, name; 

--trans_group = GROUP trans BY custId;

trans_count = FOREACH (GROUP trans BY custId) GENERATE group as custId, COUNT(trans.transId) as trans_count_a;

--trans_min_g = GROUP trans_count ALL;

min_trans = FOREACH (GROUP trans_count ALL) GENERATE MIN(trans_count.trans_count_a) as minimum;

trans_count_filtered = FILTER trans_count BY trans_count_a == min_trans.minimum;

result = foreach (JOIN cust BY id, trans_count_filtered BY custId) GENERATE custId, name, trans_count_a;

STORE result INTO '/user/hadoop/project_1/Q4';
