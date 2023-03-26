### Vertica_ls

List files in dir on database host as table

```sql
select vertica_ls('/tmp') over();
select vertica_ls('/home/dbadmin') over();
```