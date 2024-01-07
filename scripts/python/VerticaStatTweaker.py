#!/bin/python3

import logging, sys, os, datetime, subprocess
import xml.etree.ElementTree as ET

# How many days to interpolate starts into future, starting with current date.
# Usually it should overlap time to next planned full DB analyze stats.
# Assuming that full DB analyze has been run less than EXTEND_STAT_DAYS_AGO days ago
EXTEND_STAT_DAYS = 8

SQL_ALL_DATE_COLS = '''
select /*+ label('vertica stats tweak all_date_cols') */ 
  table_schema||'.'||table_name||'.'||column_name 
from columns c 
where c.data_type = 'date'
  and instr(column_name, '.') = 0
order by table_schema, table_name, column_name 
'''

SQL_TABLE_DATE_COLS = '''
select /*+ label('vertica stats tweak table_date_col') */ 
  table_schema||'.'||table_name||'.'||column_name 
from columns c  
where data_type = 'date' 
  and lower(table_schema||'.'||table_name) = lower('{schema_table}')
  and instr(column_name, '.') = 0
order by column_name   
'''

SQL_EXPORT_STAT = "select export_statistics('', '{schema_table_column}')"

SQL_IMPORT_STAT = "select import_statistics('{file}')"

logging.basicConfig(stream = sys.stdout, level = logging.INFO, format='%(levelname).1s %(asctime)s.%(msecs)03d %(funcName)20s:%(lineno)4s %(message)s', datefmt='%d %H:%M:%S')

class VerticaStatTweaker:
    def __init__(self, db_host, schema_table):
        logging.info(
            "VerticaStatTweaker db host " + db_host)
        if schema_table:
            logging.info("Tweaking stat for " + schema_table)
        else:
            logging.info("Tweaking stat for whole database")

        self.db_host = db_host
        self.schema_table = schema_table
        self.run_cnt = 0
        self.tweak_cnt = 0
        self.tmp_file = "/tmp/stat_" + str(datetime.datetime.now().microsecond) + ".xml"
        self.ssh_cmd_prefix = "ssh -o StrictHostKeyChecking=no -o ConnectTimeout=2 dbadmin@" + self.db_host + " "
        self.ssh_cmd_list_prefix = ["ssh", "-o StrictHostKeyChecking=no", "-o ConnectTimeout=2", "dbadmin@" + self.db_host]
        self.vertica_epoch = datetime.datetime.strptime("2000-01-01", '%Y-%m-%d')

    def getDateCols(self):
        logging.info("Getting tables with partition column where type is date")
        if self.schema_table:
            stc = self.schema_table.split('.')
            if len(stc) != 2 or not stc[0] or not stc[1]:
                logging.critical("Expected parameter in form schema.table, got " + self.schema_table)
                sys.exit(1)
            part_sql = (SQL_TABLE_DATE_COLS.format(schema_table=self.schema_table))
        else:
            part_sql = SQL_ALL_DATE_COLS
        part_cmd = self.ssh_cmd_list_prefix + [ "/opt/vertica/bin/vsql", "-A", "-t", '-c "' + part_sql + '"']
        logging.debug(" ".join(part_cmd))
        proc = subprocess.run(part_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        result_text = proc.stdout.decode("UTF-8")
        result_lines = result_text.split()
        if proc.returncode != 0:
            logging.critical("Error running select to get keys " + proc.stderr.decode("UTF-8"))
            sys.exit(1)
        if self.schema_table and not result_lines:
            logging.info("Table not found in database, or no columns with date datatype in table " + self.schema_table)
        return result_lines

    def tweakXML(self, source_xml_str):
        logging.debug(source_xml_str)
        stat = ET.fromstring(source_xml_str)
        current_day = (datetime.datetime.now() - datetime.datetime(2000, 1, 1)).days
        try:
            distinct = int(stat.find("tables/table/columns/column/intStats/distinct").attrib["value"])
            buckets = int(stat.find("tables/table/columns/column/intStats/buckets").attrib["value"])
            rows = int(stat.find("tables/table/columns/column/intStats/rows").attrib["value"])
            max_day = int(stat.find("tables/table/columns/column/intStats/maxValue").text)
        except:
            logging.info("Skipping - table has no stats")
            return ""

        if max_day > 20000 or max_day < -20000:
            max_day_str = ""
        else:
            max_day_str = (self.vertica_epoch + datetime.timedelta(days=max_day)).strftime('%Y-%m-%d')

        logging.info("DB stat: max day " + str(max_day) + " " + max_day_str +
                     " distinct " + str(distinct) + " buckets " + str(buckets) + " rows " + str(rows) +
                     " current day vertica epoch " + str(current_day) +
                     " " + (self.vertica_epoch + datetime.timedelta(days=current_day)).strftime('%Y-%m-%d'))

        if rows == 0:
            logging.info("Skipping - table has 0 rows")
            return ""
        elif max_day <= current_day - EXTEND_STAT_DAYS:
            logging.info("Skipping - max value way old")
            return ""
        elif max_day >= current_day + EXTEND_STAT_DAYS:
            logging.info("Skipping - max value too far in future")
            return ""

        new_bucket_max_day = current_day + EXTEND_STAT_DAYS
        new_bucket_rows = rows // buckets + 1
        new_bucket_distinct = EXTEND_STAT_DAYS
        distinct += new_bucket_distinct
        buckets += 1
        rows += new_bucket_rows

        cat_val = stat.findall('.//category')[0].find('count').attrib["value"]
        logging.debug('Sample first category count ' + cat_val)            

        if cat_val == '1310' or cat_val == '1311':
            logging.info('Stats for very large table detected - will use 1311 for new bucket rows count value');
            new_bucket_rows = 1311

        logging.info("Tweaked: max day " + str(new_bucket_max_day) +
                     " " + (self.vertica_epoch + datetime.timedelta(days=new_bucket_max_day)).strftime('%Y-%m-%d') +
                     " distinct " + str(distinct) + " buckets " + str(buckets) + " rows " + str(rows) +
                     " new bucket rows " + str(new_bucket_rows) + " distinct " + str(new_bucket_distinct))
        stat.find("tables/table/columns/column/intStats/distinct").attrib["value"] = str(distinct)
        stat.find("tables/table/columns/column/intStats/buckets").attrib["value"] = str(buckets)
        stat.find("tables/table/columns/column/intStats/rows").attrib["value"] = str(rows)
        stat.find("tables/table/columns/column/intStats/maxValue").text = str(new_bucket_max_day)
        new_category = ET.Element('category')
        new_bound = ET.SubElement(new_category, "bound")
        new_bound.text = str(new_bucket_max_day)
        new_rows = ET.SubElement(new_category, "count")
        new_rows.attrib["value"] = str(new_bucket_rows)
        new_distinct = ET.SubElement(new_category, "distinctCount")
        new_distinct.attrib["value"] = str(new_bucket_distinct)
        histogram = stat.find("tables/table/columns/column/intStats/histogram")
        histogram.append(new_category)
        tweaked_xml_str = ET.tostring(stat, encoding="UTF-8", method = "xml")
        logging.debug(tweaked_xml_str.decode('UTF-8'))
        return tweaked_xml_str

    def copyXmlToDbHost(self, tweaked_xml_str):
        file = open(self.tmp_file, 'w')
        file.write(tweaked_xml_str.decode("UTF-8"))
        file.close()
        scp_cmd = "scp -o ConnectTimeout=2 -o StrictHostKeyChecking=no " \
            + self.tmp_file + " dbadmin@" + self.db_host + ":" + self.tmp_file
        logging.debug("scp command: " + scp_cmd)
        if os.system(scp_cmd) != 0:
            logging.critical("Error copying scp")
            sys.exit(1)

    def putStatIntoDb(self):
        logging.info("Put tweaked stat into db")
        param = {"file" : self.tmp_file}
        put_sql = SQL_IMPORT_STAT.format(file = self.tmp_file)
        put_cmd = self.ssh_cmd_list_prefix + [ "/opt/vertica/bin/vsql", "-A", "-t", '-c "' + put_sql + '"']
        logging.debug(" ".join(put_cmd))
        proc = subprocess.run(put_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        result_text = proc.stdout.decode("UTF-8")
        if proc.returncode != 0:
            logging.critical("Error putting stat into db " + proc.stderr.decode("UTF-8"))
            sys.exit(1)
        logging.debug(result_text)

    def cleanUp(self):
        logging.info("Cleanup")
        try:
            os.remove(self.tmp_file)
        except:
            pass
        ssh_cmd = "ssh -o ConnectTimeout=2 -o StrictHostKeyChecking=no " \
            + "dbadmin@" + self.db_host + " rm -f " + self.tmp_file
        if os.system(ssh_cmd) != 0:
            logging.info("Error in final cleanup")

    def getStatXml(self, schema_table_column):
        logging.info("Tweaking stat for " + schema_table_column + ", run " + str(self.run_cnt))
        stat_sql = SQL_EXPORT_STAT.format(schema_table_column = schema_table_column)
        stat_cmd = self.ssh_cmd_list_prefix + [ "/opt/vertica/bin/vsql", "-A", "-t", '-c "' + stat_sql + '"']
        logging.debug(" ".join(stat_cmd))
        proc = subprocess.run(stat_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        result_text = proc.stdout.decode("UTF-8")
        if proc.returncode != 0:
            logging.critical("Error extracting stat on " + schema_table_column + " " + proc.stderr.decode("UTF-8"))
            sys.exit(1)
        logging.debug(result_text)
        return result_text

    def tweakStatistics(self, schema_table_column):
        self.run_cnt += 1
        source_xml_str = self.getStatXml(schema_table_column)
        tweaked_xml_str = self.tweakXML(source_xml_str)
        if tweaked_xml_str:
            self.tweak_cnt += 1
            self.copyXmlToDbHost(tweaked_xml_str)
            self.putStatIntoDb()

    def checkDbHostReachableSsh(self):
        logging.info("Check db host " + self.db_host + " is reachable by passwordless to dbadmin accont")
        cmd = self.ssh_cmd_prefix + "hostname"
        if os.system(cmd) != 0:
            logging.critical("Db host " + self.db_host + " is not reachable by passwordless ssh into dbadmin account")
            sys.exit(1)

    def run(self):
        self.checkDbHostReachableSsh()
        try:
            keys = self.getDateCols()
            logging.info("Get result " + str(len(keys)) + " date cols")
            for key in keys:
                self.tweakStatistics(key)
        except:
            logging.critical("Error", exc_info = True)
            sys.exit(1)
        finally:
            self.cleanUp()
        logging.info("Done runs " + str(self.run_cnt) + " tweaks " + str(self.tweak_cnt))

def main():
    if len(sys.argv) != 2 and len(sys.argv) != 3:
        logging.critical("Expecting arguments - database host and optional schema.table, got " + str(len(sys.argv) - 1))
        sys.exit(1)
    db_host = sys.argv[1]
    if len(sys.argv) == 2:
        vertica_stat_tweaker = VerticaStatTweaker(db_host, None)
    else:
        vertica_stat_tweaker = VerticaStatTweaker(db_host, sys.argv[2])
    vertica_stat_tweaker.run()


if __name__ == "__main__":
    main()
