### Args

Args function is intended for investigation of Vertica parallelism.
You can control Vertica parallelism with Args(), typically you do not have to do it.
```
Args (... [using parameters info=true, debug=true, order_by_column_count=3])
```
Mutable function that accepts any number and type of arguments, and passing all arguments to output without change.
Parameter info=true will add several output columns that show node where function is being executed and other info.
order_by_column_count=N will declare to optimiser first N columns in select list as part of ORDER BY.
Actual data order is being verified to be as claimed by Vertica, query will throw an error if order  violated.
```sql
select args() over();                  

v_desk_node0002

select args(v.num using parameters info='y') over()
from (select 5 num) v;

v_desk_node0002  5
```
