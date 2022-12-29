select VerticaLs('/home/dbadmin/work') over();
select VerticaLs('/tmp') over();

select seq(1,5) over();
select seq(1,5,2) over();
select seq(1,-5) over();
select seq (1, -5 ,-2) over();
select seq(1,5,-1) over();
select seq(1,-5,1) over();
select seq(1,2,0) over();
select seq('2015-01-01'::date, '2015-02-05'::date, 3 ) over();

select seq('2015-01-01 01:01:01'::timestamp, '2015-01-01 05:01:03'::timestamp, '1 hour'::interval  ) over();

select seq(1.0::float, -3.0::float) over();

select args(seq) over(partition by seq) from
(select seq(1,10) over()) v;


select args(seq using parameters info='y', delay_ms=10) over(partition by seq) from
(select seq(1,10) over()) v;

select mpargs(seq using parameters info='y') over() from
(select seq(1,100) over()) v;

select mpargs(seq using parameters info='y') over(partition by seq) from
(select seq(1,100) over()) v;

select to_int(current_date), to_int(current_time), to_int(current_timestamp), to_int(current_timestamp - '2015-01-01'::timestamp),
 to_int (current_date - '2015-01-01'::date);

select to_int(null::date), to_int(null::time), to_int(null::timestamp), to_int(null::interval);


select to_numeric(current_date) a, to_numeric(current_time), to_numeric(current_timestamp), to_numeric(current_timestamp - '2015-01-01'::timestamp);
select to_numeric(null::date) a, to_numeric(null::time), to_numeric(null::timestamp), to_numeric(null::interval);

select midnight_nanos(current_time, 10), midnight_nanos(current_timestamp, 11);
select midnight_nanos(null::time, 10), midnight_nanos(current_time, null::int), midnight_nanos(null::time, null::int),
midnight_nanos(null::timestamp, 10), midnight_nanos(current_timestamp, null::int), midnight_nanos(null::timestamp, null::int);


select todate(1);
select toDate(null::int);
select totime(1);
select totime(null::int);

select totimestamp(10000001);
select totimestamp(null::int);

select totimestamptz(10000001);
select totimestamptz(null::int);

select CharToBin('Bin to Char is OK'::varchar(20));
select BinToChar(CharToBin('Char to Bin -> Bin to Char is OK'::varchar(30)));

select IntToBase36(null::int);
select IntToBase36(0);
select IntToBase36(35);
select IntToBase36(36);
select IntToBase36(36*36*36*36*36 + 21);
select Base36ToInt()IntToBase36(9223372036854775807));

select Base36ToInt(null::varchar);
select Base36ToInt('');
select IntToBase36(Base36ToInt('10'));
select Base36ToInt('3PZZZZZZZZZZ');

select Base36ToInt('1Y2P0IJ32E8E7');
select IntToBase36(Base36ToInt('1Y2P0IJ32E8E7'));



select UnixNanosToTimestamp(10000001);
select UnixNanosToTimestamp(null::int);
select UnixNanosToTimestampTz(10000001);
select UnixNanosToTimestampTz(null::int);
select UnixMicrosToTimestamp(10000001);
select UnixMicrosToTimestamp(null::int);
select UnixMicrosToTimestampTz(10000001);
select UnixMicrosToTimestampTz(null::int);
select UnixMicrosToDate(1501865730 * 1000000);
select UnixMicrosToDate(null::int);
select UnixDaysToDate(to_int(current_date) + 10975);
select UnixDaysToDate(null::int);

select MicrosSinceEpoch('2022-03-11'::date, '11:05:45.123456'::time);
select MicrosSinceEpoch(null::date, '11:05:45.123456'::time);
select MicrosSinceEpoch('2022-03-11'::date, null::time);
select MicrosSinceEpoch(null::date, null::time);
select NanosSinceEpoch('2022-03-11'::date, '11:05:45.123456'::time, 789);
select NanosSinceEpoch(null::date, '11:05:45.123456'::time, 789);
select NanosSinceEpoch('2022-03-11'::date, null::time, 789);
select NanosSinceEpoch('2022-03-11'::date, '11:05:45.123456'::time, null::int);
select NanosSinceEpoch(null::date, null::time, null::int);



select csv('') over();
select csv('/home/dbadmin/testdata/csvfile.txt' ) over();
select csv('/home/dbadmin/testdata/csvfile.txt' using parameters column_count=3) over();
select csv('/home/dbadmin/testdata/csvfile.txt' using parameters column_count=3, empty_as_null='y') over();
select csv('/home/dbadmin/testdata/csvfile.txt' using parameters column_names='one,two,three') over();
select csv('/home/dbadmin/testdata/csvfile.txt' using parameters file_with_header='/home/dbadmin/testdata/csvfile.txt') over();
select csv('/home/dbadmin/testdata/csvfile.txt' using parameters file_with_header='/home/dbadmin/testdata/notexist') over();
select csv('/home/dbadmin/testdata/csvfile.txt' using parameters file_with_header='/home/dbadmin/testdata/csvfile.txt', skip_rows=1) over();
select csv('/home/dbadmin/testdata/csvfile.txt' using parameters file_with_header='/home/dbadmin/testdata/csvfile.txt', skip_rows=1, filename_column='y') over();
select csv('/home/dbadmin/testdata/csvfile.txt' using parameters file_with_header='/home/dbadmin/testdata/csvfile.txt', skip_rows=1, filename_column='y', path_column='y') over();
select csv('/home/dbadmin/testdata/csvfile.txt' using parameters file_with_header='/home/dbadmin/testdata/csvfile.txt', skip_rows=1, column_delimiter=',', row_separator='
') over();
select csv('/home/dbadmin/testdata/csvfile.txt' using parameters skip_rows=1, filename_column='y', column_count=4) over();

\!rm /home/dbadmin/users/t*.csv*

select to_int(current_timestamp) vertica_epoch, to_int(current_timestamp) + to_int('2000-01-01'::timestamp - '1970-01-01'::timestamp) unix_epoch;

select RowCount(v.*) over() from (select 1)v;

--select random_s(using parameters random_length = 64) over();

select sql_digest('a b c d ''fdf'' ''sedrswer'' 1 2 45 654 g564 56g 56.4');

select sql_digest('asDfg    123 g45 54sd 54.6') union all
select sql_digest('asDfg
    123 g45  54sD  54.6 ' )
     union all
select sql_digest('asDfg
    123
     g45  54sD
      54.6
     ');

select vertica_log() over() limit 10;
select vertica_log(using parameters udx = 'y') over() limit 10;
select vertica_log_size() over() limit 10;
select vertica_log_size(using parameters udx = 'y') over() limit 10;
select vertica_log(using parameters time_start = '03:02', debug='y') over() limit 10;
select vertica_log(using parameters time_start = '03:02', time_end = '03:03', debug='y') over() limit 10;
select vertica_log(using parameters time_end = '02:06', debug='y') over() limit 10;
select vertica_log(using parameters udx='y', last_mb=1) over() limit 10;
select vertica_log(using parameters txn = 'a000000071e534', debug='y') over() limit 10;
select vertica_log(using parameters contains = 'RebalanceCluster', debug='y') over() limit 10;
select vertica_log(using parameters time_start = '03:02', udx = 'y', debug='y') over() limit 10;

