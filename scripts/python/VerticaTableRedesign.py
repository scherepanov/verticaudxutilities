#!/bin/python3

import logging, sys, os, subprocess
import pprint

logging.basicConfig(level=logging.INFO,
                    format='%(levelname).1s %(asctime)s.%(msecs)03d %(funcName)20s:%(lineno)4s %(message)s',
                    datefmt='%d %H:%M:%S')

HELP_STR = """
VerticaTableRedesign
Automated Vertica table optimisation
General workflow steps:
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
Parameters:
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
"""

SQL_RESERVED_WORDS = [
"ALL",
"AND",
"ANY",
"ARRAY",
"AS",
"ASC",
"AUTHORIZATION",
"BETWEEN",
"BIGINT",
"BINARY",
"BIT",
"BOOLEAN",
"BOTH",
"CASE",
"CAST",
"CHAR",
"CHAR_LENGTH",
"CHARACTER_LENGTH",
"CHECK",
"COLLATE",
"COLUMN",
"CONSTRAINT",
"CORRELATION",
"CREATE",
"CROSS",
"CURRENT_DATABASE",
"CURRENT_DATE",
"CURRENT_SCHEMA",
"CURRENT_TIME",
"CURRENT_TIMESTAMP",
"CURRENT_USER",
"DATEDIFF",
"DATETIME",
"DECIMAL",
"DECODE",
"DEFAULT",
"DEFERRABLE",
"DESC",
"DISTINCT",
"ELSE",
"ENCODED",
"END",
"EXCEPT",
"EXISTS",
"EXTRACT",
"FALSE",
"FLOAT",
"FOR",
"FOREIGN",
"FROM",
"FULL",
"GRANT",
"GROUP",
"HAVING",
"ILIKE",
"ILIKEB",
"IN",
"INITIALLY",
"INNER",
"INOUT",
"INT",
"INTEGER",
"INTERSECT",
"INTERVAL",
"INTERVALYM",
"INTO",
"IS",
"ISNULL",
"JOIN",
"KSAFE",
"LEADING",
"LEFT",
"LIKE",
"LIKEB",
"LIMIT",
"LOCALTIME",
"LOCALTIMESTAMP",
"MATCH",
"MINUS",
"MONEY",
"NATURAL",
"NCHAR",
"NEW",
"NONE",
"NOT",
"NOTNULL",
"NULL",
"NULLSEQUAL",
"NUMBER",
"NUMERIC",
"OFFSET",
"OLD",
"ON",
"ONLY",
"OR",
"ORDER",
"OUT",
"OUTER",
"OVER",
"OVERLAPS",
"OVERLAY",
"PINNED",
"POSITION",
"PRECISION",
"PRIMARY",
"REAL",
"REFERENCES",
"RIGHT",
"ROW",
"SCHEMA",
"SELECT",
"SESSION_USER",
"SIMILAR",
"SMALLDATETIME",
"SMALLINT",
"SOME",
"SUBSTRING",
"SYSDATE",
"TABLE",
"TEXT",
"THEN",
"TIME",
"TIMESERIES",
"TIMESTAMP",
"TIMESTAMPADD",
"TIMESTAMPDIFF",
"TIMESTAMPTZ",
"TIMETZ",
"TIMEZONE",
"TINYINT",
"TO",
"TRAILING",
"TREAT",
"TRIM",
"TRUE",
"UNBOUNDED",
"UNION",
"UNIQUE",
"USER",
"USING",
"UUID",
"VARBINARY",
"VARCHAR",
"VARCHAR2",
"WHEN",
"WHERE",
"WINDOW",
"WITH",
"WITHIN"
]

SQL_CHECK_CONNECTION = """
show autocommit;
"""

SQL_CREATE_LIKE = """
create table '{new_schema}.{new_table}' like '{schema}.{table}' including projections;
"""

SQL_COLUMNS = """
select column_name, data_type, is_nullable from columns
where lower(table_schema) = lower('{schema}') 
and lower(table_name) = lower('{table}') 
order by ordinal_position;
"""

SQL_TABLE = """
select partition_expression, partition_group_expression from tables 
where lower(table_schema) = lower('{schema}') and lower(table_name) = lower('{table}');
"""

SQL_PROJECTION = """
select lower(projection_name), segment_expression from projections
where lower(projection_schema) = lower('{schema}')
and lower(anchor_table_name) = lower('{table}')
order by projection_name
limit 1;
"""

SQL_PROJECTION_COLUMNS = """
select table_column_name, sort_position, encoding_type from projection_columns
where lower(table_schema) = lower('{schema}')
and lower(projection_name) = lower('{projection}')
order by column_position;
"""

SQL_INSERT = """
set session autocommit to on;
insert /*+ label('VerticaTableRedesign.py')*/ into {schema}.{table} (select * from {source_schema}.{source_table} {where}); 
"""

SQL_EXPORT_TO_VERTICA = """
set session resource pool longrun1hrs;
connect to vertica {db_name} user {username} password '{password}' on '{db_host}', 5433; 
export /*+ label('VerticaTableRedesign.py')*/ to vertica {db_name}.{schema}.{table}  as select * from {source_schema}.{source_table} {where}; 
"""

SQL_DESIGN = """
select /*+ label('VerticaTableRedesign.py')*/ designer_design_projection_encodings('{schema}.{table}', '', False, True);
"""


class DbConnect:
    def __init__(self, db_host, username, password, db_name):
        self.db_host = db_host
        self.username = username
        self.password = password
        self.db_name = db_name
        logging.debug("Check connection")
        self.runQuery(SQL_CHECK_CONNECTION)

    def runQuery(self, query):
        logging.debug(query)
        query_cmd = ["/opt/vertica/bin/vsql", "-h", self.db_host, "-U", self.username, "-w", self.password,
                     "-g", sys.argv[0], "-A", "-t", '-c', query]
        proc = subprocess.run(query_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if proc.returncode != 0:
            logging.error(query)
            raise ValueError("Error running query db_host " + self.db_host + " username " + self.username
                             + " " + proc.stderr.decode("UTF-8"))
        return [el for el in proc.stdout.decode("UTF-8").split('\n') if el]

    def runQueryRequired(self, query, error_text):
        rs = self.runQuery(query)
        if not rs:
            raise ValueError(error_text)
        return rs

    def runQueryRequiredOne(self, query, error_text):
        rs = self.runQuery(query)
        if len(rs) != 1:
            logging.critical(error_text)
            exit(1)
        return rs


class Table:
    def __init__(self, schema, table, db_connect):
        self.db_connect = db_connect
        self.schema = schema
        self.table = table
        self.table_columns = []
        self.projection_columns = []
        self.order_columns = []
        self.segment_columns = []
        self.column_encodings = {}
        self.partition_expression = ""
        self.group_partition_expression = ""
        self.projection = ""
        self.segment_expression = ""
        self.partition_group_month = 0
        self.partition_group_year = 0
        self.ddl = ""
        self.designed_column_encodings = {}
        self.init = False

    def copy(self, copy_from):
        self.table_columns = copy_from.table_columns.copy()
        self.projection_columns = copy_from.projection_columns.copy()
        self.order_columns = copy_from.order_columns.copy()
        self.segment_columns = copy_from.segment_columns.copy()
        self.column_encodings = copy_from.column_encodings.copy()
        self.partition_expression = copy_from.partition_expression
        self.group_partition_expression = copy_from.group_partition_expression
        self.projection = copy_from.projection
        self.segment_expression = copy_from.segment_expression
        self.partition_group_month = copy_from.partition_group_month
        self.partition_group_year = copy_from.partition_group_year
        self.ddl = ""
        self.designed_column_encodings = copy_from.designed_column_encodings.copy()
        self.init = copy_from.init

    def getTableInfo(self):
        if self.init:
            return
        logging.debug("Get table info " + self.db_connect.db_host + "." + self.schema + "." + self.table)
        self.getColumns()
        self.getTable()
        self.getProjection()
        self.getProjectionColumns()
        self.getColumnEncodings()
        self.getOrderColumns()
        self.getSegmentColumns()
        self.getPartitionGroup()
        self.init = True

    def getColumns(self):
        query = SQL_COLUMNS.replace('{schema}', self.schema).replace('{table}', self.table)
        result_lines = self.db_connect.runQueryRequired(query,
                                                        "Source table " + self.schema + "." + self.table + " not found")
        self.table_columns = []
        for rs in result_lines:
            rs_split = rs.split('|')
            if len(rs_split) != 3:
                raise ValueError("Bad result from query " + rs)
            self.table_columns.append(
                {"column_name": rs_split[0], "data_type": rs_split[1], "is_nullable": rs_split[2] == 't'})

    def getTable(self):
        query = SQL_TABLE.replace('{schema}', self.schema).replace('{table}', self.table)
        result_lines = self.db_connect.runQueryRequiredOne(query,
                                                           "Source table " + self.schema + "." + self.table + " not found")
        rs_split = result_lines[0].split('|')
        if len(rs_split) != 2:
            raise ValueError("Bad result from query " + result_lines[0])
        self.partition_expression = rs_split[0].lower().replace(self.table.lower() + ".", "")
        self.group_partition_expression = rs_split[1].lower().replace(self.table.lower() + ".", "")
        logging.debug("Partition expression " + self.partition_expression + " group " + self.group_partition_expression)

    def getProjection(self):
        query = SQL_PROJECTION.replace('{schema}', self.schema).replace('{table}', self.table)
        result_lines = self.db_connect.runQueryRequiredOne(query, "Source projection " + self.schema + "." + self.table
                                                           + " not found")
        rs_split = result_lines[0].split('|')
        if len(rs_split) != 2:
            raise ValueError("Bad result from query " + result_lines[0])
        self.projection = rs_split[0]
        self.segment_expression = rs_split[1]
        logging.debug("Segment expression " + self.segment_expression)

    def getProjectionColumns(self):
        query = SQL_PROJECTION_COLUMNS.replace('{schema}', self.schema).replace('{projection}', self.projection)
        result_lines = self.db_connect.runQueryRequired(query,
                                                        "Source projection columns " + self.schema + "." + self.projection + " not found")
        self.projection_columns = []
        for rs in result_lines:
            rs_split = rs.split('|')
            if len(rs_split) != 3:
                raise ValueError("Bad result from query " + rs)
            sp = None
            if rs_split[1]:
                sp = int(rs_split[1])
            self.projection_columns.append(
                {"column_name": rs_split[0], "sort_position": sp, "encoding_type": rs_split[2]})

    def getOrderColumns(self):
        nn = [e for e in self.projection_columns if e["sort_position"] is not None]
        self.order_columns = []
        for el in sorted(nn, key=lambda x: int(x.get("sort_position"))):
            self.order_columns.append(el["column_name"])
        logging.debug("Order columns " + ",".join(self.order_columns))

    def setOrderSegmentColumns(self, new_order_columns, new_segment_columns):
        logging.debug("Set new order segment columns " + ",".join(new_order_columns) + " len " + str(len(new_order_columns))
                      + ",".join(new_segment_columns) + " len " + str(len(new_segment_columns)))
        if new_order_columns is None:
            logging.debug("new order columns are None")
        if new_order_columns and len(new_order_columns) != 0:
            logging.debug("Replacing order columns to " + ",".join(new_order_columns))
            self.order_columns = new_order_columns.copy()
        if new_segment_columns and len(new_segment_columns) != 0:
            logging.debug("Replacing segment columns to " + ",".join(new_segment_columns))
            self.segment_columns = new_segment_columns.copy()

    def getSegmentColumns(self):
        self.segment_columns = []
        segment_columns = self.segment_expression.replace("hash(", "").replace('"', '')[0:-1]
        for sc in segment_columns.split(','):
            spl = sc.split('.')
            if len(spl) != 2 or not spl[0] or not spl[1]:
                raise ValueError("Error processing segment expression " + self.segment_expression + " " + sc)
            self.segment_columns.append(spl[1].strip())
        logging.debug("Segment columns " + ",".join(self.segment_columns))

    def getPartitionGroup(self):
        if self.group_partition_expression:
            find_expr = ")::date) >= "
            month_off = self.group_partition_expression.find(find_expr)
            year_off = self.group_partition_expression.find(find_expr, month_off + len(find_expr))
            month_str = self.group_partition_expression[
                        month_off + len(find_expr): month_off + len(find_expr) + 3].replace(
                ')', '')
            year_str = self.group_partition_expression[
                       year_off + len(find_expr): year_off + len(find_expr) + 3].replace(
                ')', '')
            self.partition_group_month = int(month_str)
            self.partition_group_year = int(year_str)
            logging.debug("Partition group month " + str(self.partition_group_month) + " year " + str(self.partition_group_year))

    def getColumnEncodings(self):
        self.column_encodings = {}
        for pc in self.projection_columns:
            self.column_encodings[pc["column_name"]] = pc["encoding_type"]

    def designProjectionEncodings(self):
        query = SQL_DESIGN.replace('{schema}', self.schema).replace('{table}', self.table)
        logging.debug("Running design " + self.schema + "." + self.table)
        result_lines = self.db_connect.runQueryRequired(query,
                                                        "Error running designer on " + self.schema + "." + self.table)
        logging.debug("\n".join(result_lines))
        self.designed_column_encodings = {}
        skip_first = True
        for r in result_lines:
            if skip_first:
                if r == "(":
                    skip_first = False
                continue
            if r == ")":
                break
            spl = r.split()
            if (len(spl) == 3):
                self.designed_column_encodings[spl[0].replace('"', '')] = spl[2].replace(',', '')
            else:
                self.designed_column_encodings[spl[0].replace('"', '').replace(',', '')] = "AUTO"

    def setNewEncodings(self, new_column_encodings):
        self.column_encodings = new_column_encodings.copy()

    def setSegmentRLE(self):
        for se in self.segment_columns:
            self.column_encodings[se] = "RLE"
        self.ddl = ""

    def generateDDL(self):
        if self.ddl:
            return
        self.ddl = "\ncreate table " + self.schema + "." + self.table + " ("
        first = True
        for c in self.table_columns:
            if first:
                first = False
            else:
                self.ddl += ","
            self.ddl += "\n  "
            if c["column_name"].upper() in SQL_RESERVED_WORDS:
                self.ddl += '"' + c["column_name"] + '"'
            else:
                self.ddl += c["column_name"]
            self.ddl += " " + c["data_type"]
            if not c["is_nullable"]:
                self.ddl += " not null"
            if self.column_encodings[c["column_name"]] and self.column_encodings[c["column_name"]] != "AUTO":
                self.ddl += " encoding " + self.column_encodings[c["column_name"]]
        self.ddl += "\n)\n"
        self.ddl += "order by " + ", ".join(self.order_columns) + "\n"
        self.ddl += "segmented by hash(" + ", ".join(self.segment_columns) + ") all nodes\n"
        if self.partition_expression:
            self.ddl += "partition by " + self.partition_expression
            if self.group_partition_expression:
                self.ddl += " group by calendar_hierarchy_day(" + self.partition_expression \
                            + ", " + str(self.partition_group_month) + ", " + str(self.partition_group_year) + ")"
        self.ddl += ";\n"

    def createTable(self):
        logging.info("Creating table " + self.schema + "." + self.table)
        self.generateDDL()
        self.db_connect.runQuery(self.ddl)

    def copyData(self, source_schema, source_table, source_where, source_db_connect):
        sql = SQL_INSERT
        if source_db_connect:
            sql = SQL_EXPORT_TO_VERTICA
        query = sql.replace('{schema}', self.schema).replace('{table}', self.table) \
            .replace('{source_schema}', source_schema).replace('{source_table}', source_table)
        full_where = ""
        if source_where:
            full_where = " where " + source_where
        query = query.replace('{where}', full_where)

        if source_db_connect:
            logging.info("Copy from from " + source_db_connect.username + "@" + source_db_connect.db_host + " "
                         + source_schema + "." + source_table + " where " + source_where + " into "
                         + self.db_connect.db_name + " " + self.schema + "." + self.table)
            query = query.replace('{username}', self.db_connect.username).replace('{password}', self.db_connect.password) \
                .replace('{db_host}', self.db_connect.db_host).replace('{db_name}', self.db_connect.db_name)
            rs = source_db_connect.runQuery(query)
            logging.info("Exported " + " ".join(rs) + " rows")
        else:
            logging.info("copy from " + source_schema + "." + source_table + full_where + " into " + self.schema + "."
                          + self.table)
            rs = self.db_connect.runQuery(query)
            logging.info("Copied " + " ".join(rs) + " rows")


class VerticaTableRedesign:
    def __init__(self, args):
        self.setLogLevel(args)
        logging.debug("VerticaTableRedesign")

        arg_password = args.get("password", "")
        if arg_password:
            args["password"] = "******"
        arg_source_password = args.get("source_password", "")
        if arg_source_password:
            args["source_password"] = "******"
        if "help" in args.keys() or "h" in args.keys():
            print(HELP_STR)
            exit(0)
        logging.debug(pprint.pformat(args))
        self.checkBadArgs(args)

        arg_db_host = args.get("db_host", "")
        arg_username = args.get("username", "")
        arg_db_name = args.get("db_name", "msg")
        arg_source_db_host = args.get("source_db_host", "")
        arg_source_username = args.get("source_username", "")

        arg_source_table = args.get("source_table", "").lower()
        arg_sample_table = args.get("sample_table", "").lower()
        arg_order_segment_table = args.get("order_segment_table", "").lower()
        arg_optimised_table = args.get("optimised_table", "").lower()
        arg_order_columns = args.get("order_columns", "")
        arg_segment_columns = args.get("segment_columns", "")
        self.generate_source_ddl = self.boolArg(args, "generate_source_ddl", False)
        self.create_sample_table = self.boolArg(args, "create_sample_table", False)
        self.create_order_segment_table = self.boolArg(args, "create_order_segment_table", False)
        self.create_optimised_table = self.boolArg(args, "create_optimised_table", False)
        self.design_encodings = self.boolArg(args, "design_encodings", False)
        self.segment_columns_rle = self.boolArg(args, "segment_columns_rle", False)
        self.copy_source_where = args.get("copy_source_where", "")
        self.copy_sample_data = self.boolArg(args, "copy_sample_data", False)

        if not arg_db_host or not arg_username or not arg_password:
            raise ValueError("Connection info args missing db_host, username, password")
        self.db_connect = DbConnect(arg_db_host, arg_username, arg_password, arg_db_name)
        self.source_db_connect = None
        if arg_source_db_host or arg_source_username or arg_source_password:
            if not arg_source_db_host or not arg_source_username or not arg_source_password:
                raise ValueError(
                    "Source connection info args missing one of source_db_host, source_username, source_password")
            if arg_db_host == arg_source_db_host:
                raise ValueError("Source_db_host and db_host are same. "
                                 "Do not use source_db_host, source_username, source_password when source table is in same database")
            self.source_db_connect = DbConnect(arg_source_db_host, arg_source_username, arg_source_password, "")

        self.order_columns = arg_order_columns.split(',')
        self.segment_columns = arg_segment_columns.split(',')

        table_list = []
        global_schema = ""

        source_schema_name, source_table_name, global_schema = \
            self.argSchemaTable(arg_source_table, global_schema, table_list, "source")
        sample_schema_name, sample_table_name, global_schema = \
            self.argSchemaTable(arg_sample_table, global_schema, table_list, "sample")
        order_segment_schema_name, order_segment_table_name, global_schema = \
            self.argSchemaTable(arg_order_segment_table, global_schema, table_list, "order_segment")
        optimised_schema_name, optimised_table_name, global_schema = \
            self.argSchemaTable(arg_optimised_table, global_schema, table_list, "sample")

        if not source_table_name and not sample_table_name:
            raise ValueError("Need source_table and/or sample_table")

        if self.source_db_connect and not source_table_name:
            raise ValueError("Need source_table with source_db_host")
        if source_table_name and not (self.generate_source_ddl or self.copy_source_where):
            raise ValueError("source_table need copy_source_where or generate_source_ddl")
        if not source_table_name and (self.generate_source_ddl or self.copy_source_where):
            raise ValueError("copy_source_where and generate_source_ddl need source_table")

        tmp_db_connect = self.db_connect
        if self.source_db_connect:
            tmp_db_connect = self.source_db_connect
        self.source_table = self.initTableInfo(source_schema_name, source_table_name, tmp_db_connect)
        self.sample_table = self.initTableInfo(sample_schema_name, sample_table_name, self.db_connect)
        self.order_segment_table = self.initTableInfo(order_segment_schema_name, order_segment_table_name,
                                                      self.db_connect)
        self.optimised_table = self.initTableInfo(optimised_schema_name, optimised_table_name, self.db_connect)

        if not self.generate_source_ddl and not self.create_sample_table and not self.create_order_segment_table \
                and not self.create_optimised_table and not self.design_encodings and not self.copy_source_where \
                and not self.copy_sample_data:
            raise ValueError("Need to have at least one of generate_source_ddl, create_sample_table, "
                             "create_order_segment_table, create_optimised_table, design_encodings, copy_source_where, "
                             "copy_sample_data")

    def initTableInfo(self, schema, table, db_connect):
        if table:
            return Table(schema, table, db_connect)
        else:
            return None

    def setLogLevel(self, args):
        log_level = args.get("log_level", "")
        if log_level is None or not log_level:
            return
        logging.getLogger().setLevel(logging.getLevelName(log_level.upper()))

    def checkBadArgs(self, args):
        arg_keys = HELP_STR.split('\n')
        local_args = args.copy()
        cnt = 0
        for arg_str in arg_keys:
            spl = arg_str.split()
            if len(spl) == 0 or spl[0] != '-':
                continue
            key = spl[1]
            if key in args.keys():
                del local_args[key]
        if local_args:
            raise ValueError("Unknown arguments " + pprint.pformat(local_args))

    def boolArg(self, args, arg, default):
        if arg in args.keys():
            value = args.get(arg)
            return value is None or value.lower() in ['1', 'y', 'yes', 't', 'true']
        else:
            return default

    def argSchemaTable(self, arg_table, global_schema, table_list, name):
        schema = ""
        table = ""
        if arg_table:
            spl = arg_table.split('.')
            if len(spl) > 2:
                raise ValueError("Bad format for " + name + "_table " + arg_table + " - expect [schema.]table")
            elif len(spl) == 2:
                schema = spl[0]
                table = spl[1]
            else:
                schema = global_schema
                table = spl[0]
            global_schema = schema
            if not schema or not table:
                raise ValueError("Bad format for " + name + "_table " + arg_table + " - expect [schema.]table")
            if table in table_list:
                raise ValueError(table + " copy over same table")
            table_list.append(table)
        return schema, table, global_schema

    def tableURL(self):
        if not self.copy_sample_data:
            return
        t1 = None
        t2 = None
        t3 = None
        if self.sample_table:
            t1 = self.sample_table
        if self.order_segment_table:
            if t1:
                t2 = self.order_segment_table
            else:
                t1 = self.optimised_table
        if self.optimised_table:
            if not t1:
                return
            if t2:
                t3 = self.optimised_table
            else:
                t2 = self.optimised_table
        if not t1 or not t2:
            return
        query_no = "2"
        if t3:
            query_no = "3"
        link = "https://webapps.virtu.com/vertex/vertica/query?db=" \
               + self.db_connect.db_name + "&query=size-columns-" + query_no + "-tables" \
               + "&schema=" + t1.schema + "&table=" + t1.table \
               + "&schema2=" + t2.schema + "&table2=" + t2.table
        if t3:
            link += "&schema3=" + t2.schema + "&table3=" + t3.table
        print(link)

    def run(self):
        if self.generate_source_ddl:
            if not self.source_table:
                raise ValueError("generate_source_ddl require source_table")
            logging.info("Create source DDL " + self.source_table.schema + '.' + self.source_table.table)
            self.source_table.getTableInfo()
            self.source_table.generateDDL()
            print(self.source_table.ddl)
            return

        if self.create_sample_table:
            if not self.source_table or not self.sample_table:
                raise ValueError("create_sample_table require source_table and sample_table")
            logging.info("Create sample table")
            self.source_table.getTableInfo()
            self.sample_table.copy(self.source_table)
            self.sample_table.createTable()

        if self.copy_source_where:
            if not self.source_table or not self.sample_table:
                raise ValueError("copy_source_where require source_table and sample_table")
            if not self.create_sample_table:
                raise ValueError("For safety, copy data only in just created table - copy_source_where and sample_table with missing create_source_table")
            logging.info("Copy source into sample")
            self.sample_table.copyData(self.source_table.schema, self.source_table.table,
                                       self.copy_source_where, self.source_db_connect)

        if self.create_order_segment_table:
            if not self.sample_table or not self.order_segment_table:
                raise ValueError("create_order_segment_table require sample_table and order_segment_table")
            if not self.order_columns and not self.segment_columns:
                raise ValueError("create_order_segment_table require order_columns and/or segment_columns")
            logging.info("Create order_segment table table")
            self.sample_table.getTableInfo()
            self.order_segment_table.copy(self.sample_table)
            if self.order_columns or self.segment_columns:
                logging.info("Set new order by and segmentation on order_segment table")
                self.order_segment_table.setOrderSegmentColumns(self.order_columns, self.segment_columns)
            if self.segment_columns_rle:
                logging.info("Set encoding RLE for segment columns in order_segment table")
                self.order_segment_table.setSegmentRLE()
            self.order_segment_table.createTable()
            if not self.optimised_table:
                print(self.order_segment_table.ddl)

        if self.copy_sample_data and self.order_segment_table:
            if not self.sample_table:
                raise ValueError("copy_sample_data require sample_table")
            if not self.create_order_segment_table:
                raise ValueError("For safety, copy data only in just created table - copy_sample_date and order_segment_table with missing create_order_segment_table")
            logging.info("Copy sample into order_segment")
            self.order_segment_table.copyData(self.sample_table.schema, self.sample_table.table, None,
                                              None)

        designed_column_encodings = {}
        if self.design_encodings:
            table_info = None
            if self.order_segment_table:
                logging.info("Design encodings for order_segment table")
                table_info = self.order_segment_table
            elif self.sample_table:
                logging.info("Design encodings for sample table")
                table_info = self.sample_table
            else:
                raise ValueError("design_encodings require order_segment and/or sample_table")
            table_info.designProjectionEncodings()
            designed_column_encodings = table_info.designed_column_encodings
            if (self.order_segment_table and not self.optimised_table) \
                    or (self.sample_table and not self.order_segment_table and not self.optimised_table) \
                    or (self.optimised_table and not self.create_optimised_table):
                table_info.getTableInfo()
                table_info.setNewEncodings(designed_column_encodings)
                if self.segment_columns_rle:
                    logging.info("Set encoding RLE for segment columns in order_segment table")
                    table_info.setSegmentRLE()
                if self.optimised_table and not self.create_optimised_table:
                    self.optimised_table.copy(table_info)
                    table_info = self.optimised_table
                table_info.generateDDL()
                print(table_info.ddl)
                exit(0)

        if self.create_optimised_table:
            if not designed_column_encodings:
                raise ValueError("create_optimised_table require design_encodings")
            table_info = None
            if self.order_segment_table:
                logging.info("Create optimised table from order_segment table")
                table_info = self.order_segment_table
            elif self.sample_table:
                logging.info("Create optimised table from sample table")
                table_info = self.sample_table
            else:
                raise ValueError("create_optimised_table require order_segment and/or sample_table")
            table_info.getTableInfo()
            self.optimised_table.copy(table_info)
            self.optimised_table.setNewEncodings(designed_column_encodings)
            if self.segment_columns_rle:
                logging.info("Set encoding RLE for segment columns in optimised table")
                self.optimised_table.setSegmentRLE()
            self.optimised_table.createTable()
            print(self.optimised_table.ddl)

        if self.copy_sample_data and self.optimised_table:
            table_info = None
            if self.order_segment_table:
                logging.info("copy order_segment table into optimised table")
                table_info = self.order_segment_table
            elif self.sample_table:
                logging.info("copy sample table into optimised table")
                table_info = self.sample_table
            else:
                raise ValueError(
                    "copy_sample_date with optimised_table required sample_table and/or order_segment table")
            if not self.create_optimised_table:
                raise ValueError("For safety, copy data only in just created table - copy_sample_date with missing create_optimised_table")
            self.optimised_table.copyData(table_info.schema, table_info.table, None, None)

        logging.info("Done")
        self.tableURL()


def argvToArgs():
    args = {}
    first_row = True
    for arg_orig in sys.argv:
        if first_row:
            first_row = False
            continue
        arg = arg_orig
        if arg[0] == "-":
            arg = arg[1:]
        if not arg:
            raise ValueError("Wrong format of argument, expecting [-[-]]key[=[value]], got " + arg + ", try --help")
        if arg[0] == "-":
            arg = arg[1:]

        eq_idx = arg.find("=")
        if eq_idx == 0:
            raise ValueError("Wrong format of argument, expecting [-[-]]key[=[value]], got " + arg + ", try --help")
        if eq_idx == -1:
            key = arg.lower()
            value = None
        elif eq_idx >= len(arg):
            key = arg[0: eq_idx].lower()
            value = None
        else:
            key = arg[0: eq_idx].lower()
            value = arg[eq_idx + 1:]
        if key in args.keys():
            raise ValueError("Duplicate argument " + key + " " + arg_orig)
        args[key] = value
    return args


def main():
    try:
        args = argvToArgs()
        vertica_table_redesign = VerticaTableRedesign(args)
        vertica_table_redesign.run()
    except ValueError as ve:
        print("ValueError: " + str(ve))
        raise ve


if __name__ == "__main__":
    main()