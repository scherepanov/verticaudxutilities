### UDx Parallelization with help of Args()
Let's generate sequence of 10 days:
```sql
select seq (date('2014-06-01'), date('2014-06-10')) over();

Seq
----------
2014-06-01
2014-06-02
2014-06-03
2014-06-04
2014-06-05
2014-06-06
2014-06-07
2014-06-08
2014-06-09
2014-06-10
```
Result represent a table that can be used in susquent joins with other tables, or in calls to other UDx functions.

Let's call UDx function for each line and see on what node it will be executed:
```sql
select args(v.seq using parameters info='y') over()
from
(select seq (date('2014-06-01'), date('2014-06-10')) over() ) v;

Node_name        Seq
---------------  ----------
v_desk_node0001  2014-06-01
v_desk_node0001  2014-06-02
v_desk_node0001  2014-06-03
v_desk_node0001  2014-06-04
v_desk_node0001  2014-06-05
v_desk_node0001  2014-06-06
v_desk_node0001  2014-06-07
v_desk_node0001  2014-06-08
v_desk_node0001  2014-06-09
v_desk_node0001  2014-06-10
```
All executions happens on node 1 of our 3-node cluster.

Let's try to parallelize execution of function by specifying value in over() clause. It will cause global resegmentation of input table according to hash of over() parameters:
```sql
select args(v.seq using parameters info='y') over(partition by v.seq)
from
(select seq (date('2014-06-01'), date('2014-06-10')) over() ) v;
Node_name        Seq
---------------  ----------
v_desk_node0001  2014-06-01
v_desk_node0001  2014-06-05
v_desk_node0002  2014-06-02
v_desk_node0002  2014-06-04
v_desk_node0002  2014-06-07
v_desk_node0002  2014-06-08
v_desk_node0003  2014-06-03
v_desk_node0003  2014-06-06
v_desk_node0003  2014-06-09
v_desk_node0003  2014-06-10
```
We achieved random distribution of date values across nodes in cluster. This resultset can be used as table.

Let's check how many dates will be executed on every node:
```sql
select v1.node_name, count(*) from
(select args(v.seq using parameters info='y') over(partition by v.seq)
from
(select seq (date('2014-06-01'), date('2014-06-10')) over() ) v ) v1
group by v1.node_name;

node_name        count
---------------  -----
v_desk_node0002  4
v_desk_node0003  4
v_desk_node0001  2
```
We got almost random distribution of values across nodes, hash function have some noise due to small number of values.

Now let's serialize results again. We need to call UDx function with empty over() clause on top what we have, and results will be serialized:
```sql
select args(v1.seq using parameters info='y') over() from
(
select args(v.seq using parameters info='y') over(partition by v.seq) from
(
select seq (date('2014-06-01'), current_date) over()
) v
) v1;

Node_name        Seq
---------------  ----------
v_desk_node0001  2014-06-01
v_desk_node0001  2014-06-05
v_desk_node0001  2014-06-02
v_desk_node0001  2014-06-04
v_desk_node0001  2014-06-07
v_desk_node0001  2014-06-08
v_desk_node0001  2014-06-03
v_desk_node0001  2014-06-06
v_desk_node0001  2014-06-09
v_desk_node0001  2014-06-10
```
Execution again serialized to node 01 (initiator node).

If you look at QUERY PLAN what this query is doing:
```sql
select args(v1.seq using parameters info='y') over() -- serialize data to initiator node and execute there
from
(
select args(v.seq using parameters info='y') over(partition by v.seq) from -- resegment data for hash(v.seq)
-- and execute on resulting node
(
select seq (date('2014-06-01'), current_date) over() -- generate sequential data on initiator node
) v
) v1;
```
Now let's execute UDx function but keep execution pinned to same node where input data were provided. We will use partition nodes.

Partition nodes provides single-threaded execution of UDx per node. If you are using partition best, additionally to executing on multiple nodes you got 16 threads execution per node.
```sql
select args(v1.col0 using parameters info='y') over(partition nodes) from
(
select args(v.seq) over(partition by v.seq) from
(
select seq (date('2014-06-01'), date('2014-06-10')) over()
) v
) v1;

Node_name        Seq
---------------  ----------
v_desk_node0001  2014-06-01
v_desk_node0001  2014-06-05
v_desk_node0002  2014-06-02
v_desk_node0002  2014-06-04
v_desk_node0002  2014-06-07
v_desk_node0002  2014-06-08
v_desk_node0003  2014-06-03
v_desk_node0003  2014-06-06
v_desk_node0003  2014-06-09
v_desk_node0003  2014-06-10
```
We achieved full control over UDx parallelization across cluster nodes and multithreaded execution on nodes.
We can parallelize execution when needed, and serialize when we do not.
We can spread data across all nodes in cluster, keep it on same node where data were residing, or serialize data into initiator node.
