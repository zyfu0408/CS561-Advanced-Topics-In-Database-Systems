/*
	CustomerId, NumTrans, TotalTrans
*/

trans = LOAD '/user/hadoop/project_1/transactions.txt' USING PigStorage(',') as (transId:int, custId:int, transTotal:float, transNumItems:int, transDesc:chararray);

cust_trans = GROUP trans BY custId;

Result = FOREACH cust_trans GENERATE group , COUNT(trans.custId), SUM(trans.transTotal);

STORE Result INTO '/user/hadoop/project_1/Q2';
