drop table if exists public.vertica_dual_tmp;
create table public.vertica_dual_tmp
(n int)
order by n
segmented by hash(n) all nodes ksafe 1;

drop table if exists public.vertica_dual;
create table public.vertica_dual
(n int,
node_name varchar(20),
node_index int
)
order by node_index
segmented by hash(n) all nodes ksafe 1;

insert into public.vertica_dual_tmp (select seq(1,1000) over());

insert into public.vertica_dual (
select min(n) as n, node_name, substr(node_name, length(node_name) - 3)::int as node_index from
(select args(* using parameters info='y') over(partition nodes)
from public.vertica_dual_tmp) v
group by node_name
order by node_name);

drop table public.vertica_dual_tmp;
grant select on public.vertica_dual to public;