### Python VerticaStatTweaker

Python code for adressing comming problem - date columns have stale stats on next day, when new date added.

That makes optimiser to issue warning PREDICATE OUT OF RANGE and ignore table statistics.

Code read stats from database, and extend/interpolate stats one week ahead for date columns that has recent data.

That tricks optimiser to use table statistics.

Require passwordless ssh to dbadmin on one of cluster nodes.

### Parameters
db host and optional schema.table.column

