### SQL_DIGEST
SQL_DIGEST function is converting SQL into static form - all lowercase, remove whitespaces. Literal constants strings and numbers are converted into numbered bind variable.

Note that SQL_DIGEST function exist because Vertica provide digest only for SQL QUERY statements, SQL COPY command does not have it.
SQL_DIGEST is being used to calculate digest for everything that is not SQL QUERY.

SQL_DIGEST conversion makes SQL look same, when SQL text is same but differ only by literal parameters.

For example both SQL below will have same static form, despite having different capitalization, different date, different whitespaces.
```sql
select count(*) from public.my_tbl where dt = '2020-01-01';

SElect     count (*) from  PUBLIC.my_tbl
where   DT   = '2020-02-02';
SQL_DIGEST function can provide static text, or calculate 8-byte hash of static SQL.
```

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
Other usage - strip literal parameter values from SQL, to prevent data leaking. You can publish SQL text with removed sensitive information.
