#!/bin/python3
import datetime, dateutil.relativedelta, time
import logging, sys, subprocess
import pprint

logging.basicConfig(level=logging.INFO,
                    format='%(levelname).1s %(asctime)s.%(msecs)03d %(funcName)20s:%(lineno)4s %(message)s',
                    datefmt='%d %H:%M:%S')

HELP_STR = """
VerticaExportTable
Automated Vertica export table 
Parameters:
- source_db_host - required
- source_username - required
- source_password - required
- dest_db_host - required
- dest_username - required
- dest_password - required
- dest_db_name - required 
- source_schema_table - schema.table - required
- dest_schema_table - schema.table - required
- part_column - partitioning column - required with date range
- date_min - date min, format YYYY-MM-DD
- date_max - date_max, format YYYY-MM-DD
- blackout_start - time to start blackout, format HH:MM
- blackout_end - time to end blackout, format HH:MM
- copy_size - d/daily, m/monthly, f/full, default monthly if dates specified, otherwise full
- drop_partitions - drop partitions if row count is different, default N, Y will drop
- truncate - truncate table if row count is diff and copy full, default false
- log_level - critical, info, debug - default critical
"""

SQL_CHECK_CONNECTION = """
show autocommit;
"""

SQL_INSERT = """
set session autocommit to on;
set session resource_pool = longrun1hrs;
insert /*+ label('VerticaExportTable.py')*/ into {dest_schema_table} (select * from {source_schema_table} {where}); 
"""

SQL_EXPORT_TO_VERTICA = """
set session resource_pool = longrun4hrs;
connect to vertica {dest_db_name} user {dest_username} password '{dest_password}' on '{dest_db_host}', 5433; 
export /*+ label('VerticaTableRedesign.py')*/ to vertica {dest_db_name}.{dest_schema_table} 
as select * from {source_schema_table} {where}; 
"""

SQL_ROW_COUNT = """
select /*+ label('VerticaExportTable.py')*/ count(*) from {schema_table} {where};
"""

SQL_DROP_PARTITIONS = """
select drop_partitions('{schema_table}','{date_min}', '{date_max}');
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
            raise ValueError("Error running query " + self.toString()
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

    def toString(self):
        s = self.username + "@" + self.db_host
        if self.db_name:
            s += ':' + self.db_name
        return s


class Table:
    def __init__(self, schema_table, db_connect, part_column):
        self.db_connect = db_connect
        self.schema_table = schema_table
        self.part_column = part_column
        self.date_min = ""
        self.date_max = ""

    def toString(self):
        return self.db_connect.toString() + " " + self.schema_table

    def clearDates(self):
        self.date_min = ""
        self.date_max = ""

    def setDates(self, date_min, date_max):
        if not self.part_column:
            raise ValueError("Part column not set for " + self.toString())
        self.date_min = date_min.isoformat()
        self.date_max = date_max.isoformat()

    def getWhere(self):
        if self.date_min:
            return " where " + self.part_column + " between '" + self.date_min + "' and '" + self.date_max + "'"
        else:
            return ""

    def copyDataTo(self, dest_table):
        if self.db_connect:
            sql = SQL_EXPORT_TO_VERTICA
        else:
            sql = SQL_INSERT
        query = sql.replace('{dest_schema_table}', dest_table.schema_table) \
            .replace('{source_schema_table}', self.schema_table)
        query = query.replace('{where}', self.getWhere())

        if self.db_connect:
            logging.debug("Copy from from " + self.toString() + self.getWhere() + " into "
                         + dest_table.toString())
            query = query.replace('{dest_username}', dest_table.db_connect.username) \
                .replace('{dest_password}', dest_table.db_connect.password) \
                .replace('{dest_db_host}', dest_table.db_connect.db_host) \
                .replace('{dest_db_name}', dest_table.db_connect.db_name) \
                .replace('{where}', self.getWhere())
            rs = self.db_connect.runQuery(query)
            logging.debug("Exported " + " ".join(rs) + " rows")
        else:
            logging.debug("copy from " + self.toString()
                         + " into " + dest_table.toString())
            rs = self.db_connect.runQuery(query)
            logging.debug("Copied " + " ".join(rs) + " rows")

    def getRowCount(self):
        logging.debug("Row count " + self.toString() + self.getWhere())
        query = SQL_ROW_COUNT
        query = query.replace('{schema_table}', self.schema_table).replace('{where}', self.getWhere())
        rs = self.db_connect.runQueryRequiredOne(query, "Error rowCount " + self.toString() + " " + self.getWhere())
        logging.debug("Row count " + self.toString() + self.getWhere() + " " + " ".join(rs))
        return int(" ".join(rs))

    def dropPartitions(self):
        logging.debug("Drop partitions " + self.toString())
        if not self.date_min:
            raise ValueError("Drop partitions require date_min/date_max/part_column")
        query = SQL_DROP_PARTITIONS
        query = query.replace('{schema_table}', self.schema_table) \
            .replace('{date_min}', self.date_min).replace('{date_max}', self.date_max)
        rs = self.db_connect.runQueryRequiredOne(query, "Error dropPartitions " + self.toString() + " " + self.getWhere())
        return " ".join(rs)

    def truncate(self):
        raise ValueError("Truncate not implemented - too dangerous - do truncate by hand " + self.toString())


class VerticaExportTable:
    def __init__(self, args):
        self.setLogLevel(args)
        logging.debug("VerticaExportTable")

        arg_source_password = args.get("source_password", "")
        if arg_source_password:
            args["source_password"] = "******"
        arg_dest_password = args.get("dest_password", "")
        if arg_dest_password:
            args["dest_password"] = "******"
        if "help" in args.keys() or "h" in args.keys():
            print(HELP_STR)
            exit(0)
        logging.debug(pprint.pformat(args))
        self.checkBadArgs(args)

        arg_source_db_host = args.get("source_db_host", "")
        arg_source_username = args.get("source_username", "")
        arg_dest_db_host = args.get("dest_db_host", "")
        arg_dest_username = args.get("dest_username", "")
        arg_dest_db_name = args.get("dest_db_name", "")

        arg_source_schema_table = args.get("source_schema_table", "")
        arg_dest_schema_table = args.get("dest_schema_table", "")
        if not arg_source_schema_table or not arg_dest_schema_table:
            raise ValueError("Source and dest schema_table are required")
        if not arg_source_db_host or not arg_source_username or not arg_source_password:
            raise ValueError("Arguments source_db_host, source_username and source_password are required")
        if not arg_dest_db_host or not arg_dest_username or not arg_dest_password:
            raise ValueError("Arguments dest_db_host, dest_username and dest_passwords are required")
        if arg_source_db_host != arg_dest_db_host and not arg_dest_db_name:
            raise ValueError("Argument dest_db_name required for export between clusters")
        if arg_source_db_host == arg_dest_db_host and arg_source_username == arg_dest_username \
            and arg_source_schema_table == arg_dest_schema_table:
            raise ValueError("Cannot export to same table on same clusters")
        self.part_column = args.get("part_column", "")
        source_db_connect = DbConnect(arg_source_db_host, arg_source_username, arg_source_password, None)
        self.source_table = Table(arg_source_schema_table, source_db_connect, self.part_column)
        dest_db_connect = DbConnect(arg_dest_db_host, arg_dest_username, arg_dest_password, arg_dest_db_name)
        self.dest_table = Table(arg_dest_schema_table, dest_db_connect, self.part_column)

        self.date_min = args.get("date_min", "")
        self.date_max = args.get("date_max", "")

        self.blackout_start = args.get("blackout_start", "")
        self.blackout_end = args.get("blackout_end", "")

        if self.blackout_start and not self.blackout_end or not self.blackout_start and self.blackout_end:
            raise ValueError("blackout_start and blackout_end should be set together")

        if self.blackout_start:
            try:
                time.strptime(self.blackout_start, '%H:%M')
            except:
                raise ValueError("Bad format for blackout_start, expected HH:MM, got " + self.blackout_start)
            try:
                time.strptime(self.blackout_end, '%H:%M')
            except:
                raise ValueError("Bad format for blackout_start, expected HH:MM, got " + self.blackout_end)
            if self.blackout_end <= self.blackout_start:
                raise ValueError("Blackout end should be past blackout start")

        if self.date_min or self.date_max or self.part_column:
            if not self.date_min or not self.date_max or not self.part_column:
                raise ValueError("part_column, date_min and date_max should be specified together")
            self.copy_size = args.get("copy_size", "m").lower()
            if self.copy_size == 'f':
                raise ValueError("Cannot have copy_size full when dates specified")
            if self.date_min >= self.date_max:
                raise ValueError("date_min " + self.date_min + " should be less or equal to date_max " + self.date_max)
        else:
            self.copy_size = args.get("copy_size", "f").lower()
            if self.copy_size != 'f':
                raise ValueError("Must have copy_size full when dates not specified")

        if self.copy_size not in ["d", "m", "f"]:
            raise ValueError("Argument copy_size must be one of d, m, f")
        self.drop_partitions = self.boolArg(args, "drop_partitions", False)
        self.truncate = self.boolArg(args, "truncate", False)
        self.total_copied = 0

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

    def copyData(self, date_start = None, date_end = None):
        if date_start:
            logging.info("Copy date range " + str(date_start) + " - " + str(date_end))
            self.source_table.setDates(date_start, date_end)
            self.dest_table.setDates(date_start, date_end)
        else:
            logging.info("Copy full")
        source_row_cnt = self.source_table.getRowCount()
        dest_row_cnt = self.dest_table.getRowCount()
        if source_row_cnt == dest_row_cnt:
            if source_row_cnt == 0:
                logging.info("No data in source and dest")
            else:
                logging.info("Row count is same " + str(source_row_cnt))
            return
        elif dest_row_cnt != 0:
            if source_row_cnt == 0:
                logging.info("Source row count is 0, while dest row count is " + str(dest_row_cnt)
                                 + " - need manual drop of dest data")
                raise ValueError("Source row count is 0, while dest row count is " + str(dest_row_cnt)
                                 + " - need manual drop of dest data")
            if self.source_table.date_min:
                if self.drop_partitions:
                    logging.info("drop partitions - src rows " + str(source_row_cnt) + " dst " + str(dest_row_cnt))
                    self.dest_table.dropPartitions()
                else:
                    logging.info("Different number of rows in src " + str(source_row_cnt) + " and dst "
                                 + str(dest_row_cnt) + ", but drop_partitions not set")
                    raise ValueError("Different number of rows in src " + str(source_row_cnt) + " and dst "
                                 + str(dest_row_cnt) + ", but drop_partitions not set")
            else:
                if self.truncate:
                    logging.info("truncate")
                    self.dest_table.truncate()
                else:
                    logging.info("Different number of rows in src " + str(source_row_cnt) + " and dst "
                                 + str(dest_row_cnt) + ", but truncate not set")
                    raise ValueError("Different number of rows in src " + str(source_row_cnt) + " and dst "
                                 + str(dest_row_cnt) + ", but truncate not set")
        self.source_table.copyDataTo(self.dest_table)
        dest_row_cnt = self.dest_table.getRowCount()
        if source_row_cnt != dest_row_cnt:
            raise ValueError("After full copy dest table " + self.dest_table.toString()
                             + " has different number of rows comparing to source table "
                             + self.source_table.toString() + " - " + str(source_row_cnt) + " vs " + str(dest_row_cnt))
        logging.info("Copied " + str(source_row_cnt))
        self.total_copied += source_row_cnt

    def run(self):
        self.total_copied = 0
        if self.copy_size == 'f':
            logging.info("Full copy from " + self.source_table.toString() + " into " + self.dest_table.toString())
            self.copyData()
            return
        elif self.copy_size == 'm':
            inc = 'month'
        else:
            inc = 'day'
        logging.info("Copy from " + self.source_table.toString() + " into " + self.dest_table.toString()
                      + " by " + inc + " from " + self.date_min + " to " + self.date_max)
        start_date = datetime.datetime.strptime(self.date_min, '%Y-%m-%d').date()
        end_date = datetime.datetime.strptime(self.date_max, '%Y-%m-%d').date()
        if start_date > end_date:
            raise ValueError("date_min " + self.date_min + " should be less than date_max " + self.date_max)

        work_date = start_date
        while work_date <= end_date:
            if self.blackout_start:
                cur_time = datetime.datetime.now().strftime("%H:%M")
                if cur_time >= self.blackout_start and cur_time < self.blackout_end:
                    logging.info("Sleeping after " + self.blackout_start + " till " + self.blackout_end)
                    while True:
                        cur_time = datetime.datetime.now().strftime("%H:%M")
                        if cur_time >= self.blackout_end:
                            logging.info("Resuming after sleep")
                            break
                        time.sleep(30)
            range_end_date = work_date
            if self.copy_size == 'm':
                range_end_date = range_end_date.replace(day=1)
                range_end_date += dateutil.relativedelta.relativedelta(months=1)
                range_end_date -= datetime.timedelta(days=1)
                if range_end_date > end_date:
                    range_end_date = end_date
            logging.info("Work date " + str(work_date))
            next_date = range_end_date + datetime.timedelta(days=1)
            self.copyData(work_date, range_end_date)
            work_date = next_date

        logging.info("Done, total copied " + str(self.total_copied) + " Copy from " + self.source_table.toString()
                     + " into " + self.dest_table.toString() + " by " + inc + " from " + self.date_min
                     + " to " + self.date_max)


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
        vertica_table_redesign = VerticaExportTable(args)
        vertica_table_redesign.run()
    except ValueError as ve:
        print("ValueError: " + str(ve))
        raise ve


if __name__ == "__main__":
    main()
