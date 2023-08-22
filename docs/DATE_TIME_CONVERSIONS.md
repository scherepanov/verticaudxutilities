## Date Time Conversions
There are several function to supplement Vertica date/time/timestamp conversions.
Those functions are effectively doing cast from one type to another, leaving Vertica internal data representation intact.
This is possible because Vertica types Int, Time, TimeTZ, Timestamp, TimestampTz, Date, Interval, ItnervalYM are all using same internal representation - 8-byte integer.
You can consider those functions as type cast without underlying data change.

Due to many reasons they are not present in Vertica, or doing not what is needed.
For example, Vertica do not have direct conversion of timestamp to number of micros since epoch into int, or reverse.

Functions Unix* additionally subtract constant to convert to Vertica epoch from Unix epoch.
Unix epoch starts from 1970-01-01 midnight, while Vertica inheres Postgress epoch that starts at midnight 2000-01-01.
TimeTz conversions remove internal timezone and working in UTC.

### Functions relative to Unix epoch or midnight

| Unix->Vertica           | Vertica->Unix          | Desc                                                               |
|-------------------------|------------------------|--------------------------------------------------------------------|
| UnixDaysToDate          | DateToUnixDays         | Convert between number of days since epoch and Vertica Date        |
| UnixMicrosToTimestamp   | TimestampToUnixMicros  | Convert between number of micros since epoch and Vertica Timestamp |
| UnixNanosToTimestamp    | TimestampToUnixNanos   | Convert between number of nanos since epoch and Vertica Timestamp  |
| UnixMicrosToDate        |                        | Convert number of micros since epoch to Vertica Date               |
| MidnightMicrosToTime    | TimeToMidnightMicros   | Convert between number of micros since midnight and Vertica Time   |     
| MidnightMicrosToTimeTz  | TimeTzToMidnightMicros | Convert between number of micros since midnight and Vertica TimeTz | 
| UnixMicrosToTimestampTz |                        | Convert number of micros since epoch to Vertica TimestampTz        |
| UnixNanosToTimestampTz  |                        | Convert number of nanos since epoch to Vertica TimestampTz         |

### To Int conversions

| Function   | 	arg                                                              | 	result   | 	Desc                                                                                                |
|------------|-------------------------------------------------------------------|-----------|------------------------------------------------------------------------------------------------------|
| To_Int     | 	Timestamp, TimestampTz, Date, Time, TimeTZ, Interval, IntervalYM | 	int      | 	Takes Timestamp, TimestampTz, Date, Time, TimeTz, Interval, IntervalYM parameter, and return int    |
| To_Numeric | 	Timestamp, TimestampTz, Date, Time, TimeTZ, Interval, IntervalYM | 	Numeric	 | Takes Timestamp, TimestampTz, Date, Time, TimeTz, Interval, IntervalYM parameter, and return Numeric |

### From Int conversions

| Function      | 	arg | 	result	     | Desc                                                            |
|---------------|------|--------------|-----------------------------------------------------------------|
| ToDate        | 	int | 	Date        | 	Takes number of days since epoch and return date               |
| ToTime        | 	int | 	Time        | 	Takes number of micros since midnight and return Time          |
| ToTimeTz      | 	int | 	TimeTz      | 	Takes number of micros since midnight and return TimeTz in UTC |
| ToTimestamp   | 	int | 	Timestamp   | 	Takes number of micros since epoch and return Timestamp        |
| ToTimestampTZ | 	int | 	TimestampTZ | 	Takes number of micros since epoch and return TimestampTZ      |

### Char Binary conversions

| Function  | 	arg                              | 	result        | 	Desc                                                             |
|-----------|-----------------------------------|----------------|-------------------------------------------------------------------|
| CharToBin | 	Char, Varchar, LongVarchar       | 	LongVarbinary | 	Takes Char, Varchar, LongVarchar and converts into LongVarbinary |
| BinToChar | 	Binary, Varbinary, LongVarbinary | 	Varchar       | 	Takes Binary, Varbinary, LongVarbinary and converts into Varchar |

### Convenience functions

| Function         | 	arg1                                 | 	arg2 | 	arg3                             | 	result | 	desc                                                                                           |
|------------------|---------------------------------------|-------|-----------------------------------|---------|-------------------------------------------------------------------------------------------------|
| Midnight_Nanos   | 	Timestamp, TimestampTz, Time, TimeTX |       | 	                                 | int     | Takes Timestamp, TimestampTz, Time, TimeTX return number of nanos since midnight                |
| MicrosSinceEpoch | 	Date                                 | 	Time |                                   | int     | Takes Date, Time, return number of micros since epoch                                           |Takes two arguments - Date and Time, return number of micros since epoch|
| NanosSinceEpoch  | 	Date                                 | 	Time | Int Nanos (3 digits, can be null) | int     | Takes three arguments - Date , Time and int (3 digit nanos), return number of nanos since epoch |






