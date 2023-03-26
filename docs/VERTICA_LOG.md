# Vertica Log
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
