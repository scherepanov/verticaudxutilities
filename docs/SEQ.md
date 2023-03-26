### SEQ
Seq ( begin, end {, increment } )

Generates sequence of dates, timestamps, timestamptz, times, integers, floats.

Generate descending sequence if end less than begin.

Data type of begin and end arguments should be same.

Increment for Timestamp, TimestampTz, Time, TimeTz  is in microseconds.

Default increment is 1 (day, second, integer, float).

Date increment is in days
```sql
select seq( 1,3) over();                                -- 1,2,3
select seq (1, -3) over();                              -- 1,0,-1,-2,-3
select seq(1,5,2) over();                               -- 1,3,5
select seq(1.1, 1.6, 0.1) over();                       -- 1.1,1.2,1.3,1.4,1.5,1.6
select (date('2014-06-01'), date('2014-06-15') over();  -- Dates from 2014-06-01 till 2014-06-15
select seq(current_time, current_time - 0.005) over(); -- Time with 2 minutes interval for 20 minutes from current time back
select seq(current_timestamp, current_timestamp + 0.005, 60000000) over(); -- Timestamps with 2 minutes interval for 20 minutes
```
