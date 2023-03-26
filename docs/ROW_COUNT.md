### RowCount
RowCount function is intended for performance benchmarking.
If you are running query like
```sql
select count(*) from <mytable>
```
Vertica is cheating - it is using column with RLE encoding with least cardinality (you should have one in each table) and fetch only one column without decoding/decomrpessing.

It is forcing Vertica to read all rows with all columns in full, and count rows from resultset.
It is not using any optimization regular count founction would do, and by far less efficient as count.

Function RowCount() does not add overhead by itself. You can measure time it take for SQL to be executed, and see Vertica performance (hint - Vertica is outrageously fast!).

RowCount function report node where it has been executed, and number of rows passed to it.
```sql
select RowCount(s.seq) over() from
(select seq(1,100) over())s;

node_name       row_count
--------------  ---------
v_mes_node0031  100
```

Here we are fetching ALL rows and ALL columns from table (do not try it at home - we are what is called professionals!):
```sql
select RowCount(*) over() from
(select * from mytable) v;                              
```
Checking QUERY PLAN : query
```sql
select * from mytable;
```
is executed on each node for local data, then results are sent to single inititator node, where single instance of RowCount() is getting all data.

Try to check how long it takes query execution step - send data form each node to initiator node:
```sql
select RowCount(*) over(partition nodes) from
(select * from mytable) v;
```
Above SQL is not sending data from nodes to single initiator node.
Each node in cluster is running one instance of RowCount(), and it is getting all local data from mytable on node.
No network transfer from nodes to single initiator node. You will see significant speed bump, especially on clusters with large node count.

Vertica do live up to claims that performance is linearly scales with number of nodes!

You can make Vertica run 16 instances of RowCount() per each node in cluster.
Vertica will split local data on node in 16 streams with opeartion called LOCAL RESEGMENT (see QUERY PLAN) and each instance of RowCount() will get it's own share of data.
Typically performance goes up order of magnitude.
```sql
select RowCount(*) over(partition best) from
(select * from mytable) v;
```
I would suggest to run above RowCount() examples with WHERE condition, limiting data per node to around 100MB.
That will give you a good feel how RowCount() works.
