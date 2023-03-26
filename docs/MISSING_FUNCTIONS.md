## Missing Functions
There are several function to supplement Vertica date/time/timestamp conversions.
Those functions are effectively doing cast from one type to another, leaving Vertica internal data representation intact.
This is possible because Vertica types Int, Time, TimeTZ, Timestamp, TimestampTz, Date are all using same internal representation - 8-byte integer.
You can consider those functions as type cast without underlying data change.

Due to many reasons they are not present in Vertica, or doing not what is needed.
For example, you cannot easily extract from timestamp number of micros since epoch into int, or reverse.

Functions Unix* additionally subtract constant to convert to Vertica epoch from Unix epoch.
Unix epoch starts from 1970-01-01 midnight, while Vertica inheres Postgress epoch that starts at midnight 2000-01-01.
```
To_Int                  - Takes Timestamp, TimestampTz, Date, Time, TimeTZ parameter, and return int as number of days micros since Unix epoch/midnight
To_Numeric              - Takes Timestamp, TimestampTz, Date, Time, TimeTZ parameter, and return Numeric as number of days micros since Unix epoch/midnight
ToDate                  - Takes int as number of days since Unix epoch and return Date
ToTime                  - Takes int as number of micros since midnight and return Time
ToTimestamp             - Takes int as number of micros since Unix epoch and return Timestamp
ToTimestampTZ           - Takes int as number of micros since Unix epoch and return TimestampTZ
UnixNanosToTimestamp    - Takes int as number of nanos since Unix epoch and return Timestamp 
UnixNanosToTimestampTz  - Takes int as number of nanos since Unix epoch and return TimestampTz
UnixMicrosToTimestamp   - Takes int as number of micros since Unix epoch and return Timestamp
UnixMicrosToTimestampTz - Takes int as number of micros since Unix epoch and return timestampTz in Vertica epoch
Midnight_Nanos          - Takes Timestamp, TimestampTz, Time, TimeTz return int as number of nanos since midnight/Unix epoch
MicrosSinceEpoch        - takes two arguments - Date and Time, return int as number of micros since Unix epoch.
NanosSinceEpoch         - takes three arguments - Date , Time and int (3 digit nanos), return number of nanos since Unix epoch.
```

Char conversions
```
CharToBin - Takes Char, Varchar, LongVarchar and return LongVarbinary. Equivalent if Vertica had a cast to ::varbinary for those types
BinToChar - Takes Binary, Varbinary, LongVarbinary and return Varchar
```
