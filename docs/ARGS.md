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
