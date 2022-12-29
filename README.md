# Vertica UDx Utilities

UDx utilities is a c++ library containing helper functions for working with Vertica.
It also contains few functions that are missed from Vertica for various reasons.

## Prerequisites

Library registration claims UDx SDK v 11.1.1, you can build and run starting with Vertica v 9.3.
Library verified to work up to UDx SDK v 12.0.2-2.
You will need to edit library registration and put your Vertica version in cpp/verticaudxutilities/UDxUtilities.cpp.

There is no indications that you will not be able to compile and use library with version 12 and above.

Library compiles with g++ v 10.3 and above, with c++17.
No need to use docker, most likely it will lower your compiler version.

Library linked with static libstdc++, allowing you to use recent version of build server/desktop, and does nto depend on OS version on Vertica cluster nodes.
Vertica cluster nodes typically very outdated on OS version, static libstdc++ nicely decouples Vertica ndoes OS version from OS version and compiler version on build box.

Library with statically linked libstdc++ works well in fenced and unfenced mode, it is recommended mode.

It is recommended to use recent OS version for build server/desktop, with most recent g++ compiler. 

It is recommended to raise your c++ to c++20 if you install corresponding g++ compiler version.

If you want, you can lower c++ standard down to c++11, but you will need to do some work on tweaking c++ code.

## Install

Initially you need to run ./build.sh in root dir of repo.

To deploy, you need to run cpp/verticaudxutilities/deploy.sh. It accepts single parameter - node name, one of ndoes in cluster.

You need to have passwordless ssh into dbadmin on one of nodes in Vertica cluster.

Alternatively, you will need to enter OS dbadmin password 3 times per deploy.

Deploy is relying on Vertica recommended configuration of passwordless login into database dbadmin account from any node in cluster, as described in Vertica install guide.

After deployment, it is recommended to create table vertica_dual by running script cpp/verticaudxutilities/vertica_dual.sql.
It is used with Vertica log viewer functions.

Deployment creates all functions in schema public, and give grants to execute to public.

While for most functions public grants are correct, you may consider limiting access to for example VerticaLog, as it gives users full read-only access to vertica and UDx logs.

## Functionality

### Missing functions

There are several function to supplement Vertica date/time/timestamp conversions. 
Those functions are effectively doing cast from one type to another, leaving Vertica internal data representation intact.
This is possible because Vertica types Int, Time, TimeTZ, Timestamp, TimestampTz, Date are all using same internal representation - 8-byte integer. 
You can consider those functions as type cast without underlying data change.

Due to many reasons they are not present in Vertica, or doing not what is needed. 
For example, you cannot easily extract from timestamp number of micros since epoch into int, or reverse.

Functions Unix* additionally subtract constant to convert to Vertica epoch from Unix epoch. 
Unix epoch starts from 1970-01-01 midnight, while Vertica inheres Postgress epoch that starts at midnight 2000-01-01.
```
To_Int                  - Takes Timestamp, TimestampTz, Date, Time, TimeTZ parameter, and return int as number of days micros since Unix epoch/midnight
To_Numeric              - Takes Timestamp, TimestampTz, Date, Time, TimeTZ parameter, and return Numeric as number of days micros since Unix epoch/midnight
ToDate                  - Takes int as number of days since Unix epoch and return Date
ToTime                  - Takes int as number of micros since midnight and return Time
ToTimestamp             - Takes int as number of micros since Unix epoch and return Timestamp
ToTimestampTZ           - Takes int as number of micros since Unix epoch and return TimestampTZ
UnixNanosToTimestamp    - Takes int as number of nanos since Unix epoch and return Timestamp 
UnixNanosToTimestampTz  - Takes int as number of nanos since Unix epoch and return TimestampTz
UnixMicrosToTimestamp   - Takes int as number of micros since Unix epoch and return Timestamp
UnixMicrosToTimestampTz - Takes int as number of micros since Unix epoch and return timestampTz in Vertica epoch
Midnight_Nanos          - Takes Timestamp, TimestampTz, Time, TimeTz return int as number of nanos since midnight/Unix epoch
MicrosSinceEpoch        - takes two arguments - Date and Time, return int as number of micros since Unix epoch.
NanosSinceEpoch         - takes three arguments - Date , Time and int (3 digit nanos), return number of nanos since Unix epoch.
```

Char conversions
```
CharToBin - Takes Char, Varchar, LongVarchar and return LongVarbinary. Equivalent if Vertica had a cast to ::varbinary for those types
BinToChar - Takes Binary, Varbinary, LongVarbinary and return Varchar
```
## CSV
Read file on node in Vertica cluster and present result as table with string columns. 
Function is presenting external data as table in Vertica, without doing data loads.

Functionality is similar to external table, with CSV function having advantage of not needed to define
external tables for each file. You can have million of CSV files, each with it's own columns, and one function will rule them all.
CSV function produce varchar output columns, you need to convert to your data type.
```
select csv(<filename> [ using parameters <parameters> ] ) over();
```

Filename must be visible on node in Vertica cluster. If file is on client, it is not visible to Vertica cluster!

Parameters (case sensitive):
```
column_delimiter - column delimiter, default comma
row_separator - record separator, default line feed
column_names - line with list of column names delimited by column delimiter
column_count - number of columns, default 32
max_row_length - maximum length of row, default 4096
skip_row - number of rows to skip in file, default 0.
file_with_header - you can specify column names in separate file. Or, it can be in same file as data.
filename_column - Add column with this name and populate with data file name (no path)
path_column - Add column with this name and populate with path to data file
empty_as_null - empty column value ",," is treated as NULL. Default - it will be non-null empty string.
debug - 'y' or 'trace'. Will cause logs dumped on vertica node. Do not use.
```

Content of file:
```shell
cat /home/dbadmin/testdata/csvfile.txt
one,two,three
1,234-5,sdf
4,kdfds,sdfds
,dt,
c1,,r
sd,f,
sd,f,g,
```

SQL execution:
```sql
select csv('/home/dbadmin/testdata/csvfile.txt' using parameters file_with_header='/home/dbadmin/testdata/csvfile.txt', skip_rows=1, filename_column='y', path_column='y') over();
one |  two  | three |  filename   |          path          
-----+-------+-------+-------------+-------------------------
1   | 234-5 | sdf   | csvfile.txt | /home/dbadmin/testdata/
4   | kdfds | sdfds | csvfile.txt | /home/dbadmin/testdata/
    | dt    |       | csvfile.txt | /home/dbadmin/testdata/
c1  |       | r     | csvfile.txt | /home/dbadmin/testdata/
sd  | f     |       | csvfile.txt | /home/dbadmin/testdata/
sd  | f     | g     | csvfile.txt | /home/dbadmin/testdata/
(6 rows)
```
## VerticaLS

Will list directory as table.

SQL execution:

```sql
select VerticaLs('/tmp') over();
filename                                       | filetype |       timestamp        |   size  
-----------------------------------------------+----------+------------------------+----------
/tmp/yum_save_tx.2018-03-27.06-52.61ANVT.yumtx | f        | 2018-03-27 06:52:03-05 |    47119
/tmp/ssh-PyYopA1mW9HE                          | d        | 2018-03-12 08:50:55-05 |        
/tmp/yum_save_tx.2018-04-04.06-52.yUVufU.yumtx | f        | 2018-04-04 06:52:01-05 |    47119
/tmp/yum_save_tx.2018-04-01.06-52.0RjEl0.yumtx | f        | 2018-04-01 06:52:03-05 |    47119
/tmp/root                                      | d        | 2018-01-02 09:47:36-06 |        
/tmp/create_functions.sql                      | f        | 2018-04-04 11:02:24-05 |     3117
/tmp/hsperfdata_root                           | d        | 2018-04-04 00:26:23-05 |        
/tmp/.X0-lock                                  | f        | 2018-03-12 08:50:35-05 |       11
/tmp/yum_save_tx.2018-03-26.06-52.jXDhn3.yumtx | f        | 2018-03-26 06:52:03-05 |    47119
/tmp/.xrdp                                     | d        | 2018-03-12 09:05:41-05 |        
/tmp/i7z_output                                | f        | 2018-04-04 00:26:32-05 |       93
/tmp/yum_save_tx.2018-03-29.06-52.ZYgJ8q.yumtx | f        | 2018-03-29 06:52:01-05 |    47119
/tmp/yum_save_tx.2018-03-31.06-52.DU70nq.yumtx | f        | 2018-03-31 06:52:04-05 |    47119
/tmp/yum_save_tx.2018-04-03.06-52.BpfuRE.yumtx | f        | 2018-04-03 06:52:03-05 |    47119
/tmp/yum_save_tx.2018-04-02.06-52.eN_j5b.yumtx | f        | 2018-04-02 06:52:03-05 |    47119
/tmp/yum_save_tx.2018-03-30.06-52.t1LBnt.yumtx | f        | 2018-03-30 06:52:01-05 |    47119
/tmp/test_queries.sql                          | f        | 2018-04-04 11:02:24-05 |     3370
/tmp/.ICE-unix                                 | d        | 2018-03-12 08:50:56-05 |        
/tmp/yum_save_tx.2018-03-28.06-52.iefMzU.yumtx | f        | 2018-03-28 06:52:03-05 |    47119
/tmp/hos_update.log                            | f        | 2018-04-04 00:26:47-05 |     1900
/tmp/.X11-unix                                 | d        | 2018-03-12 08:50:35-05 |        
/tmp/.esd-999                                  | d        | 2018-03-12 08:50:57-05 |        
/tmp/libverticaudxutilities.so                 | f        | 2018-04-04 11:02:24-05 |  8600504
(26 rows)
```

## SEQ, RowCount and ARGS functions
### SEQ
Seq ( begin, end {, increment } )

Generates sequence of dates, timestamps, timestamptz, times, integers, floats.

Generate descending sequence if end less than begin.

Data type of begin and end arguments should be same.

Increment for Timestamp, TimestampTz, Time, TimeTz  is in microseconds.

Default increment is 1 (day, second, integer, float).

Date increment is in days
```sql
select seq( 1,3) over();                                -- 1,2,3
select seq (1, -3) over();                              -- 1,0,-1,-2,-3
select seq(1,5,2) over();                               -- 1,3,5
select seq(1.1, 1.6, 0.1) over();                       -- 1.1,1.2,1.3,1.4,1.5,1.6
select (date('2014-06-01'), date('2014-06-15') over();  -- Dates from 2014-06-01 till 2014-06-15
select seq(current_time, current_time - 0.005) over(); -- Time with 2 minutes interval for 20 minutes from current time back
select seq(current_timestamp, current_timestamp + 0.005, 60000000) over(); -- Timestamps with 2 minutes interval for 20 minutes
```
Seq function can be used when you have missed values in timeseries. Seq will have all values in timeseries.
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
### Args

Args function is intended for investigation of Vertica parallelism. 
You can control Vertica parallelism with Args(), typically you do not have to do it.
```
Args (... [using parameters info='y'])
```
Mutable function that accepts any number and type of arguments, and passing all arguments to output without change.
Can add first argument that show node where function is being executed.
```sql
select args() over();                  

v_desk_node0002

select args(v.num using parameters info='y') over()
from (select 5 num) v;

v_desk_node0002  5
```
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

### SQL_DIGEST
SQL_DIGEST function is converting SQL into static form - all lowercase, remove whitespaces. Literal constants strings and numbers are converted into numbered bind variable.

That conversion makes SQL having same text that differ only by parameter to look same.

For example both SQL below will have same static form, despite having different capitalization, different date, different whitespaces.
```sql
select count(*) from public.my_tbl where dt = '2020-01-01';

SElect     count (*) from  PUBLIC.my_tbl
where   DT   = '2020-02-02';
SQL_DIGEST function can provide static text, or calculate 8-byte hash of static SQL.
```
Intended usage - to find all identical SQL in Vertica.

Note that SQL_DIGEST function exist because Vertica provide digest only for SQL QUERY statements, SQL COPY command does not have it. 
SQL_DIGEST is being used to calculate digest for everything that is not SQL QUERY.
```sql
select sql_digest('select count(*) from public.my_tbl where dt = ''2022-03-17''');

sql_digest
3 325 144 705 735 003 687
```
Parameters
```
only_static_str - if present, show SQL text with string literals converted to numbered bind variables
only_static_int - if present, show SQL additionally in lowercase, with int converted to numbered bind variables
debug - if present, will dump debug info into internal Vertica logs on servers
```
Vertica Log
Vertica_Log() is a viewer for vertica.log and UDx.log.

Vertica_Log() is parsing leading part of log lines into standard set of columns, and have rest of log line as one column.

#### Vertica_dual table
It is recommended to create table public.vertica_dual, using script vertica_dual.sql.

It is guaranteed that table public.vertica_dual will have one and exactly one row residing on each node in cluster.

Table public.vertica_dual allow to start exactly one instance of VerticaLog() on each node in cluster, looking at logs on this node.
You can run UDx on arbitrary node, or subset of nodes in cluster, by filtering on Vertica_Dual table. 
### Vertica_Log syntax
Basic syntax:
```sql
select vertica_log() over() limit 10;

date        time          node_name       call    module  process         txn     group   level   body                                                                                                                                                                         
----------  ------------  --------------  ------  ------  --------------  ------  ------  ------  ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------  
2022-11-13  03:05:01.520  v_XXX_node0014  (null)  INFO    (null)          (null)  (null)  (null)  New log                                                                                                                                                                      
2022-11-13  03:05:01.520  v_XXX_node0014  (null)  Main    0x7ffff7e34c80  (null)  Init    INFO     Log /XXX/XXX/v_XXX_node0014_catalog/vertica.log opened; #8                                                                                                              
2022-11-13  03:05:01.520  v_XXX_node0014  (null)  Main    0x7ffff7e34c80  (null)  Init    INFO     Processing command line: /opt/vertica/bin/vertica -D /XXX/XXX/v_XXX_node0014_catalog -C XXX -n v_XXX_node0014 -h XX.XX.XX.XX -p 5433 -P 4803@XX.XX.XX.XX -Y ipv4       
2022-11-13  03:05:01.520  v_XXX_node0014  (null)  Main    0x7ffff7e34c80  (null)  Init    INFO     Starting up Vertica Analytic Database v11.1.1-7                                                                                                                             
2022-11-13  03:05:01.520  v_XXX_node0014  (null)  Main    0x7ffff7e34c80  (null)  Init    INFO     Project Codename: Jackhammer                                                                                                                                                
2022-11-13  03:05:01.520  v_XXX_node0014  (null)  Main    0x7ffff7e34c80  (null)  Init    INFO     vertica(v11.1.1-7) built by @re-docker4 from releases/VER_11_1_RELEASE_BUILD_1_7_20220912@06d5c80a144db5a66ebbc60af61438da1e1854df on 'Mon Sep 12 15:41:29 2022' $BuildId$
2022-11-13  03:05:01.520  v_XXX_node0014  (null)  Main    0x7ffff7e34c80  (null)  Init    INFO     CPU architecture: x86_64
2022-11-13  03:05:01.520  v_XXX_node0014  (null)  Main    0x7ffff7e34c80  (null)  Init    INFO     64-bit Optimized Build
2022-11-13  03:05:01.520  v_XXX_node0014  (null)  Main    0x7ffff7e34c80  (null)  Init    INFO     Compiler Version: 7.3.1 20180303 (Red Hat 7.3.1-5)
2022-11-13  03:05:01.520  v_XXX_node0014  (null)  Main    0x7ffff7e34c80  (null)  Init    INFO     LD_LIBRARY_PATH=/opt/vertica/lib                                                                                                                                            
```
You can see first 10 lines from today's log that has been rotated at 3:05 AM, on node 14 - initiator node for session.

Now, let's find first line in logs on every node in cluster:
```sql
select vertica_log(using parameters contains='New log', time_end='03:06') over(partition nodes)
from public.vertica_dual
order by node_name;

date        time          node_name       call    module  process  txn     group   level   body     
----------  ------------  --------------  ------  ------  -------  ------  ------  ------  -------  
2022-11-13  03:05:01.516  v_XXX_node0001  (null)  INFO    (null)   (null)  (null)  (null)  New log  
2022-11-13  03:05:01.676  v_XXX_node0002  (null)  INFO    (null)   (null)  (null)  (null)  New log  
2022-11-13  03:05:01.871  v_XXX_node0003  (null)  INFO    (null)   (null)  (null)  (null)  New log  
2022-11-13  03:05:02.159  v_XXX_node0004  (null)  INFO    (null)   (null)  (null)  (null)  New log  
2022-11-13  03:05:01.645  v_XXX_node0005  (null)  INFO    (null)   (null)  (null)  (null)  New log  
2022-11-13  03:05:01.631  v_XXX_node0006  (null)  INFO    (null)   (null)  (null)  (null)  New log  ```
```
With help of vertica_audit and PARTITION NODES, we executed single instance of Vertica_Log() on each node in 6-node cluster.
We found log lines that had 'New log' in text and time before 03:06.

We can run Vertica_Log() on initiator node, on all nodes in cluster, on arbitrary node in cluster, or on subset of ndoes in cluster.
For example, let's run Vertica_Log() on single arbitrary node no 3 in cluster. Not we do not know what node will be initiator for session, it is random:
```sql
select vertica_log(using parameters contains='New log', time_end='03:06') over(partition nodes) 
from public.vertica_dual
where node_index = 3
order by node_name;

date        time          node_name       call    module  process  txn     group   level   body     
----------  ------------  --------------  ------  ------  -------  ------  ------  ------  -------  
2022-11-13  03:05:01.871  v_XXX_node0003  (null)  INFO    (null)   (null)  (null)  (null)  New log  
```
All parameters are optional:
```
node        - string - Alternative way to run VerticaLog on specified node. String is matched from back of node name, for example '12'
date        - string - YYYYMMDD. Look on past log files instead of present. Not implemented yet
catalog_dir - string - Parent dir of catalog dir. Defalt /vertica/<database>
udx         - string - if specified, will look into UDxLogs/UDxFencedProcesses.log instead of vertica.log
last_mb     - number - if specified will cause VerticaLog to loon only on last N MB on log file. It is much fastr on huge logs if you need only something very recent.
log_date    - string - date to filter in YYYY-MM-DD format. To be used with time filter, log always spawn more than one day.
time_start  - string - Time start filter, in format HH24:MI:SS.SSS. Any part of time from back can be omited.
time_end    - string - Time end filter, same format as Time start.
txn         - string - Transaction ID in hex with lowercase a, b, c, d, e, f.
contains    - string - filter on string
contains2   - string - second filter on string, AND codition with 'contains' parameter.
debug       - string - 'y' or 'trace', will cause internal debug infor dumped inside Vertica cls=uster, do not use.
dummy       - string - ignored, needed for codegen due to required comma in parameters list.  
```
Most of Vertica_Log() parameters are for filtering text.
You can filter text in WHERE clause, but built-in Vertica_Log() filters are more efficient.

#### Planned new features for Vertica_Log()
1 - Support data parameter, read gzip compressed old logs

2 - Fast binary search on time. In uncompressed log, you can jump to any offset, find beginning of log line and determine time.
If time is before parameter, jump back in file, if time is ahead, jump forward in file.

Goal is to find time in huge log file in fraction of second.

### Vertica_Log_Size
Vertica_Log_Size() show size of current log:
```sql
select Vertica_Log_Size() over(partition nodes)
from public.vertica_dual
order by node_name;

node_name       vertica_log                                      size       
--------------  -----------------------------------------------  ---------  
v_XXX_node0001  /XXX/XXX/v_XXX_node0001_catalog/vertica.log  7 836 978  
v_XXX_node0002  /XXX/XXX/v_XXX_node0002_catalog/vertica.log  5 357 545  
v_XXX_node0003  /XXX/XXX/v_XXX_node0003_catalog/vertica.log  5 235 599  
v_XXX_node0004  /XXX/XXX/v_XXX_node0004_catalog/vertica.log  5 305 713  
v_XXX_node0005  /XXX/XXX/v_XXX_node0005_catalog/vertica.log  5 257 870  
v_XXX_node0006  /XXX/XXX/v_XXX_node0006_catalog/vertica.log  5 261 566  
```
Prime goal of Vertica_Log_Size() is to detect failed log rotation.

## Dynamic Python
[Dynamic Python](DynPython.md)

## License
Library released under MIT license

