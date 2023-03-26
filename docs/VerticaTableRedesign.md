### Python VerticaTableRedesign

### Automated Vertica table optimisation

### General workflow steps:
Source -> Sample -> Order segment -> Optimised

Each step works on associated table, called source, sample, order_segment, optimised.

Table for each step should be different.

Steps can be omitted

Takes source table, possible on different cluster, and copy to sample table with same structure, with WHERE clause.

Sample table can be created if requested.

Order_segment table can be created, with new ORDER BY and SEGMENTED BY, and data copied from sample table.

Design projection encodings can be run on order segment or sample table

Optimised table can be created, with new encodings, and data copied from order segment table.

VerticaTableRedesign can be used to generate source table DDL in CREATE TABLE format, more convenient than Vertica EXPORT_CATALOG.

Boolean args recognised as True for 1, y, yes, t, true - case insensitive. Absence of value is True. Anything else is False

Code is safe - not deleting any data nor dropping or altering any table, table names are enforced different,
copy data only of table just been created

You have to drop tables and delete data manually.

### Parameters:

- db_host - required
- username - required
- password - required
- db_name - optional - default msg - need to be specified with source_db_host, and for Vertex query link
- source_db_host - optional, used when source table resides on different cluster
- source_username - optional
- source_password - optional
- source_table - schema.table - optional
- sample_table - [schema.]table - optional
- order_segment_table - [schema.]table - optional
- optimised_table - [schema.]table - optional
- generate_source_ddl - boolean, default False, generate source table DDL
- create_sample_table - boolean, default False, create sample table
- create_order_segment_table - boolean, default False, create order_segment table
- create_optimised_table - boolean, default False, create optimised table
- order_columns - csv list of order column to be placed in order_segment table ORDER BY
- segment_columns - csv list of segment columns to be placed in order_segment table SEGMENTED BY HASH
- design_encodings - boolean, default False, run design encodings for last specified table - source, sample, order_segment
- segment_columns_rle - boolean, default False, force columns from segment expression to encoding RLE on order_segment and optimised tables
- copy_source_where - string - default empty and no copy from source to sample - copy from source to sample with this where clause
- copy_sample_data - boolean, default False, copy data from sample table into order_segment and optimised tables
- log_level - critical, info, debug - default critical
